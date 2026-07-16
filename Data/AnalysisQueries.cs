using System.Globalization;
using System.Text;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SqlSchemaMcp.Abstractions;
using SqlSchemaMcp.Abstractions.Capabilities;
using SqlSchemaMcp.SqlServer.Configuration;
using SqlSchemaMcp.SqlServer.Data;

namespace SqlSchemaMcp.Data;

public sealed class AnalysisQueries(
    IOptions<SqlServerEngineOptions> options,
    ILogger<AnalysisQueries> logger,
    ICapabilityResolver resolver)
    : SqlQueryBase(options, logger)
{
    public async Task<string> AnalyzeNamingConventions(
        string database,
        CancellationToken cancellationToken = default)
    {
        if (!_databases.TryGetValue(database, out var connectionString))
            return UnknownDatabase(database);

        const string objectSql = """
            SELECT 'TABLE' AS ObjectType, TABLE_SCHEMA AS SchemaName, TABLE_NAME AS ObjectName
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE'
                AND TABLE_NAME NOT LIKE @stagingPattern
            UNION ALL
            SELECT 'VIEW', TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS
            UNION ALL
            SELECT 'PROCEDURE', ROUTINE_SCHEMA, ROUTINE_NAME
            FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_TYPE = 'PROCEDURE'
            ORDER BY ObjectType, ObjectName
            """;

        const string columnSql = """
            SELECT c.TABLE_SCHEMA, c.TABLE_NAME, c.COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS c
            JOIN INFORMATION_SCHEMA.TABLES t
                ON t.TABLE_NAME = c.TABLE_NAME AND t.TABLE_SCHEMA = c.TABLE_SCHEMA
            WHERE t.TABLE_TYPE = 'BASE TABLE'
                AND c.TABLE_NAME NOT LIKE @stagingPattern
            ORDER BY c.TABLE_SCHEMA, c.TABLE_NAME, c.ORDINAL_POSITION
            """;

        try
        {
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(cancellationToken);

            var objects = new List<(string Type, string Schema, string Name)>();
            await using (var cmd = new SqlCommand(objectSql, conn))
            {
                cmd.Parameters.AddWithValue("@stagingPattern", StagingExcludeLike);
                await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                    objects.Add((reader.GetString(0), reader.GetString(1), reader.GetString(2)));
            }

            var columns = new List<(string Schema, string Table, string Column)>();
            await using (var cmd = new SqlCommand(columnSql, conn))
            {
                cmd.Parameters.AddWithValue("@stagingPattern", StagingExcludeLike);
                await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                    columns.Add((reader.GetString(0), reader.GetString(1), reader.GetString(2)));
            }

            return BuildNamingReport(database, objects, columns);
        }
        catch (Exception ex)
        {
            return SafeError(ex);
        }
    }

    private static string BuildNamingReport(
        string database,
        List<(string Type, string Schema, string Name)> objects,
        List<(string Schema, string Table, string Column)> columns)
    {
        var hungarian = new List<string>();
        var versionSuffix = new List<string>();
        var allCaps = new List<string>();
        var snakeCase = new List<string>();

        string[] hungarianPrefixes = ["tbl_", "sp_", "vw_", "col_", "f_", "fn_", "usp_"];
        string[] versionSuffixes = ["_v2", "_v3", "_v4", "_v5", "_final", "_old", "_backup", "_copy", "_new", "_temp", "_bak"];

        foreach (var (type, schema, name) in objects)
        {
            string lower = name.ToLowerInvariant();
            string label = $"  [{schema}].[{name}] ({type})";

            if (hungarianPrefixes.Any(p => lower.StartsWith(p, StringComparison.OrdinalIgnoreCase)))
                hungarian.Add(label);

            if (versionSuffixes.Any(s => lower.EndsWith(s, StringComparison.OrdinalIgnoreCase)))
                versionSuffix.Add(label);

            if (string.Equals(name, name.ToUpperInvariant(), StringComparison.Ordinal) && name.Length > 1)
                allCaps.Add(label);

            if (name.Contains('_') && !hungarianPrefixes.Any(p => lower.StartsWith(p, StringComparison.OrdinalIgnoreCase)))
                snakeCase.Add(label);
        }

        var colHungarian = new List<string>();
        var colAllCaps = new List<string>();
        var colSnakeCase = new List<string>();

        foreach (var (schema, table, column) in columns)
        {
            string lower = column.ToLowerInvariant();
            string label = $"  [{schema}].[{table}].{column}";

            if (hungarianPrefixes.Any(p => lower.StartsWith(p, StringComparison.OrdinalIgnoreCase)))
                colHungarian.Add(label);

            if (string.Equals(column, column.ToUpperInvariant(), StringComparison.Ordinal) && column.Length > 1)
                colAllCaps.Add(label);

            if (column.Contains('_'))
                colSnakeCase.Add(label);
        }

        var sb = new StringBuilder();
        sb.AppendLine($"NAMING CONVENTION ANALYSIS: [{database}]");
        sb.AppendLine(new string('─', 70));

        AppendViolationSection(sb, "HUNGARIAN PREFIXES (objects)", hungarian);
        AppendViolationSection(sb, "HUNGARIAN PREFIXES (columns)", colHungarian);
        AppendViolationSection(sb, "VERSION SUFFIXES (_v2, _OLD, _FINAL, etc.)", versionSuffix);
        AppendViolationSection(sb, "ALL_CAPS OBJECTS", allCaps);
        AppendViolationSection(sb, "ALL_CAPS COLUMNS", colAllCaps);
        AppendViolationSection(sb, "snake_case OBJECTS", snakeCase);
        AppendViolationSection(sb, "snake_case COLUMNS", colSnakeCase);

        int total = hungarian.Count + colHungarian.Count + versionSuffix.Count
            + allCaps.Count + colAllCaps.Count + snakeCase.Count + colSnakeCase.Count;
        sb.AppendLine($"Total violations: {total}");

        return sb.ToString();
    }

    private static void AppendViolationSection(StringBuilder sb, string header, List<string> items)
    {
        sb.AppendLine();
        sb.AppendLine($"{header} ({items.Count})");
        sb.AppendLine(new string('-', 60));
        if (items.Count == 0)
            sb.AppendLine("  (none)");
        else
            foreach (var item in items)
                sb.AppendLine(item);
    }

    public async Task<string> AnalyzeMissingForeignKeys(
        string database,
        CancellationToken cancellationToken = default)
    {
        if (!_databases.TryGetValue(database, out var connectionString))
            return UnknownDatabase(database);

        // Columns that look like FK candidates
        const string candidateSql = """
            SELECT
                c.TABLE_SCHEMA,
                c.TABLE_NAME,
                c.COLUMN_NAME,
                c.DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS c
            JOIN INFORMATION_SCHEMA.TABLES t
                ON t.TABLE_NAME = c.TABLE_NAME AND t.TABLE_SCHEMA = c.TABLE_SCHEMA
            WHERE t.TABLE_TYPE = 'BASE TABLE'
                AND c.TABLE_NAME NOT LIKE @stagingPattern
                AND c.DATA_TYPE IN ('int', 'bigint', 'smallint', 'uniqueidentifier')
                AND (
                    c.COLUMN_NAME LIKE '%Id'
                    OR c.COLUMN_NAME LIKE '%ID'
                    OR c.COLUMN_NAME LIKE '%_id'
                )
            ORDER BY c.TABLE_SCHEMA, c.TABLE_NAME, c.COLUMN_NAME
            """;

        // Columns that already have FK constraints
        const string existingFkSql = """
            SELECT
                OBJECT_SCHEMA_NAME(fk.parent_object_id) AS TableSchema,
                OBJECT_NAME(fk.parent_object_id) AS TableName,
                COL_NAME(fkc.parent_object_id, fkc.parent_column_id) AS ColumnName
            FROM sys.foreign_keys fk
            JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id
            """;

        // All PKs (to cross-reference candidate targets)
        const string pkSql = """
            SELECT
                tc.TABLE_SCHEMA,
                tc.TABLE_NAME,
                ku.COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku
                ON ku.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
                AND ku.TABLE_SCHEMA = tc.TABLE_SCHEMA
                AND ku.TABLE_NAME = tc.TABLE_NAME
            WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
            """;

        try
        {
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(cancellationToken);

            var candidates = new List<(string Schema, string Table, string Column, string Type)>();
            await using (var cmd = new SqlCommand(candidateSql, conn))
            {
                cmd.Parameters.AddWithValue("@stagingPattern", StagingExcludeLike);
                await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                    candidates.Add((reader.GetString(0), reader.GetString(1), reader.GetString(2), reader.GetString(3)));
            }

            var existingFks = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            await using (var cmd = new SqlCommand(existingFkSql, conn))
            await using (var reader = await cmd.ExecuteReaderAsync(cancellationToken))
                while (await reader.ReadAsync(cancellationToken))
                    existingFks.Add($"{reader.GetString(0)}.{reader.GetString(1)}.{reader.GetString(2)}");

            var pkColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            await using (var cmd = new SqlCommand(pkSql, conn))
            await using (var reader = await cmd.ExecuteReaderAsync(cancellationToken))
                while (await reader.ReadAsync(cancellationToken))
                    pkColumns.Add($"{reader.GetString(0)}.{reader.GetString(1)}.{reader.GetString(2)}");

            var sb = new StringBuilder();
            sb.AppendLine($"MISSING FOREIGN KEY ANALYSIS: [{database}]");
            sb.AppendLine(new string('─', 80));
            sb.AppendLine("Columns matching FK name patterns with no FK constraint defined:");
            sb.AppendLine();

            int count = 0;
            foreach (var (schema, table, column, type) in candidates)
            {
                string key = $"{schema}.{table}.{column}";
                if (existingFks.Contains(key))
                    continue;

                // Skip self-referencing PK columns
                if (pkColumns.Contains(key))
                    continue;

                count++;
                sb.AppendLine($"  [{schema}].[{table}].{column} ({type})");
            }

            if (count == 0)
                sb.AppendLine("  (none found — all FK-pattern columns have constraints)");

            sb.AppendLine();
            sb.AppendLine($"  {count} potential missing FK(s)");

            return sb.ToString();
        }
        catch (Exception ex)
        {
            return SafeError(ex);
        }
    }

    public async Task<string> AnalyzeMissingIndexes(
        string database,
        CancellationToken cancellationToken = default)
    {
        if (!_databases.TryGetValue(database, out var connectionString))
            return UnknownDatabase(database);

        const string candidateSql = """
            SELECT
                c.TABLE_SCHEMA,
                c.TABLE_NAME,
                c.COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS c
            JOIN INFORMATION_SCHEMA.TABLES t
                ON t.TABLE_NAME = c.TABLE_NAME AND t.TABLE_SCHEMA = c.TABLE_SCHEMA
            WHERE t.TABLE_TYPE = 'BASE TABLE'
                AND c.TABLE_NAME NOT LIKE @stagingPattern
                AND (
                    c.COLUMN_NAME LIKE '%Id'
                    OR c.COLUMN_NAME LIKE '%ID'
                    OR c.COLUMN_NAME LIKE '%_id'
                    OR c.COLUMN_NAME IN (
                        'IsActive', 'IsDeleted', 'Status', 'CreatedAt', 'DeletedAt',
                        'TenantId', 'OrganisationId', 'OrganizationId', 'AccountId'
                    )
                )
            ORDER BY c.TABLE_SCHEMA, c.TABLE_NAME, c.COLUMN_NAME
            """;

        const string indexedColumnsSql = """
            SELECT
                OBJECT_SCHEMA_NAME(ic.object_id) AS TableSchema,
                OBJECT_NAME(ic.object_id) AS TableName,
                c.name AS ColumnName
            FROM sys.index_columns ic
            JOIN sys.columns c
                ON c.object_id = ic.object_id AND c.column_id = ic.column_id
            JOIN sys.indexes i
                ON i.object_id = ic.object_id AND i.index_id = ic.index_id
            WHERE ic.is_included_column = 0
            """;

        try
        {
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(cancellationToken);

            var candidates = new List<(string Schema, string Table, string Column)>();
            await using (var cmd = new SqlCommand(candidateSql, conn))
            {
                cmd.Parameters.AddWithValue("@stagingPattern", StagingExcludeLike);
                await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                    candidates.Add((reader.GetString(0), reader.GetString(1), reader.GetString(2)));
            }

            var indexedColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            await using (var cmd = new SqlCommand(indexedColumnsSql, conn))
            await using (var reader = await cmd.ExecuteReaderAsync(cancellationToken))
                while (await reader.ReadAsync(cancellationToken))
                    indexedColumns.Add($"{reader.GetString(0)}.{reader.GetString(1)}.{reader.GetString(2)}");

            var sb = new StringBuilder();
            sb.AppendLine($"MISSING INDEX ANALYSIS: [{database}]");
            sb.AppendLine(new string('─', 80));
            sb.AppendLine("FK-pattern and common filter columns with no index:");
            sb.AppendLine();

            int count = 0;
            foreach (var (schema, table, column) in candidates)
            {
                string key = $"{schema}.{table}.{column}";
                if (indexedColumns.Contains(key))
                    continue;
                count++;
                sb.AppendLine($"  [{schema}].[{table}].{column}");
            }

            if (count == 0)
                sb.AppendLine("  (none found — all candidate columns are indexed)");

            sb.AppendLine();
            sb.AppendLine($"  {count} potentially unindexed column(s)");

            return sb.ToString();
        }
        catch (Exception ex)
        {
            return SafeError(ex);
        }
    }
    public Task<string> AnalyzeDuplicateIndexes(
        string database,
        CancellationToken cancellationToken = default) =>
        ResolveSqlServerAnalysis(database, nameof(AnalyzeDuplicateIndexes), capability => capability.AnalyzeDuplicateIndexes(database, cancellationToken));

    public Task<string> FindUnusedTables(
        string database,
        CancellationToken cancellationToken = default) =>
        ResolveSqlServerAnalysis(database, nameof(FindUnusedTables), capability => capability.FindUnusedTables(database, cancellationToken));

    public Task<string> FindUnusedProcedures(
        string database,
        CancellationToken cancellationToken = default) =>
        ResolveSqlServerAnalysis(database, nameof(FindUnusedProcedures), capability => capability.FindUnusedProcedures(database, cancellationToken));

    public Task<string> AnalyzeProcComplexity(
        string database,
        string? nameFilter = null,
        CancellationToken cancellationToken = default) =>
        ResolveSqlServerAnalysis(database, nameof(AnalyzeProcComplexity), capability => capability.AnalyzeProcComplexity(database, nameFilter, cancellationToken));

    public Task<string> AnalyzeViewComplexity(
        string database,
        string? nameFilter = null,
        CancellationToken cancellationToken = default) =>
        ResolveSqlServerAnalysis(database, nameof(AnalyzeViewComplexity), capability => capability.AnalyzeViewComplexity(database, nameFilter, cancellationToken));

    public Task<string> AnalyzeIndexFragmentation(
        string database,
        string? nameFilter = null,
        CancellationToken cancellationToken = default) =>
        ResolveSqlServerAnalysis(database, nameof(AnalyzeIndexFragmentation), capability => capability.AnalyzeIndexFragmentation(database, nameFilter, cancellationToken));

    public Task<string> AnalyzeTriggers(
        string database,
        CancellationToken cancellationToken = default) =>
        ResolveSqlServerAnalysis(database, nameof(AnalyzeTriggers), capability => capability.AnalyzeTriggers(database, cancellationToken));

    public Task<string> AnalyzeIdentityColumns(
        string database,
        CancellationToken cancellationToken = default) =>
        ResolveSqlServerAnalysis(database, nameof(AnalyzeIdentityColumns), capability => capability.AnalyzeIdentityColumns(database, cancellationToken));

    public Task<string> AnalyzeTableSizes(
        string database,
        CancellationToken cancellationToken = default) =>
        ResolveSqlServerAnalysis(database, nameof(AnalyzeTableSizes), capability => capability.AnalyzeTableSizes(database, cancellationToken));

    public Task<string> AnalyzeMissingIndexSuggestions(
        string database,
        CancellationToken cancellationToken = default) =>
        ResolveSqlServerAnalysis(database, nameof(AnalyzeMissingIndexSuggestions), capability => capability.AnalyzeMissingIndexSuggestions(database, cancellationToken));

    public Task<string> GetRecentObjectChanges(
        string database,
        int days,
        CancellationToken cancellationToken = default) =>
        ResolveSqlServerAnalysis(database, nameof(GetRecentObjectChanges), capability => capability.GetRecentObjectChanges(database, days, cancellationToken));

    public Task<string> AnalyzeTableQueryStats(
        string database,
        CancellationToken cancellationToken = default) =>
        ResolveSqlServerAnalysis(database, nameof(AnalyzeTableQueryStats), capability => capability.AnalyzeTableQueryStats(database, cancellationToken));

    public Task<string> AnalyzeTableAccessStats(
        string database,
        CancellationToken cancellationToken = default) =>
        ResolveSqlServerAnalysis(database, nameof(AnalyzeTableAccessStats), capability => capability.AnalyzeTableAccessStats(database, cancellationToken));

    public Task<string> GenerateDatabaseSummary(
        string database,
        CancellationToken cancellationToken = default) =>
        ResolveSqlServerAnalysis(database, nameof(GenerateDatabaseSummary), capability => capability.GenerateDatabaseSummary(database, cancellationToken));

    private Task<string> ResolveSqlServerAnalysis(
        string database,
        string toolName,
        Func<ISqlServerAnalysisCapability, Task<string>> execute)
    {
        if (resolver.TryResolve<ISqlServerAnalysisCapability>(database, out _, out var capability) && capability is not null)
            return execute(capability);

        return Task.FromResult(
            resolver.TryGetEngine(database, out var engine)
                ? Sentinels.Unsupported(toolName, engine)
                : Sentinels.UnknownDatabase(resolver.DatabaseNames, database));
    }
}
