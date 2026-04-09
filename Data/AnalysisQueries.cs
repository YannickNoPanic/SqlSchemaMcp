using System.Globalization;
using System.Text;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;
using SqlSchemaMcp.Configuration;

namespace SqlSchemaMcp.Data;

public sealed class AnalysisQueries(IOptions<SqlServerOptions> options) : SqlQueryBase(options)
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
            return $"ERROR: {ex.Message}";
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
            return $"ERROR: {ex.Message}";
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
            return $"ERROR: {ex.Message}";
        }
    }

    public async Task<string> AnalyzeDuplicateIndexes(
        string database,
        CancellationToken cancellationToken = default)
    {
        if (!_databases.TryGetValue(database, out var connectionString))
            return UnknownDatabase(database);

        const string sql = """
            SELECT
                OBJECT_SCHEMA_NAME(i.object_id) AS SchemaName,
                OBJECT_NAME(i.object_id) AS TableName,
                i.name AS IndexName,
                i.is_unique,
                ic.key_ordinal,
                c.name AS ColumnName
            FROM sys.indexes i
            JOIN sys.index_columns ic
                ON ic.object_id = i.object_id AND ic.index_id = i.index_id
            JOIN sys.columns c
                ON c.object_id = ic.object_id AND c.column_id = ic.column_id
            JOIN sys.tables t
                ON t.object_id = i.object_id
            WHERE i.type > 0
                AND i.is_primary_key = 0
                AND i.is_disabled = 0
                AND ic.is_included_column = 0
                AND t.name NOT LIKE @stagingPattern
            ORDER BY OBJECT_NAME(i.object_id), i.name, ic.key_ordinal
            """;

        try
        {
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(cancellationToken);
            await using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@stagingPattern", StagingExcludeLike);
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

            // Build: tableKey -> list of (indexName, isUnique, orderedKeyColumns)
            var raw = new Dictionary<string, List<(string IndexName, bool IsUnique, List<string> Cols)>>(StringComparer.OrdinalIgnoreCase);

            string? curTable = null;
            string? curIndex = null;
            bool curUnique = false;
            List<string>? curCols = null;

            void FlushIndex()
            {
                if (curTable == null || curIndex == null || curCols == null) return;
                if (!raw.TryGetValue(curTable, out var list))
                    raw[curTable] = list = [];
                list.Add((curIndex, curUnique, curCols));
            }

            while (await reader.ReadAsync(cancellationToken))
            {
                string tableKey = $"{reader.GetString(0)}.{reader.GetString(1)}";
                string indexName = reader.GetString(2);
                bool isUnique = reader.GetBoolean(3);
                string col = reader.GetString(5);

                if (tableKey != curTable || indexName != curIndex)
                {
                    FlushIndex();
                    curTable = tableKey;
                    curIndex = indexName;
                    curUnique = isUnique;
                    curCols = [];
                }
                curCols!.Add(col);
            }
            FlushIndex();

            // Detect redundancies: index A is redundant if B has A's columns as a leading prefix
            var redundant = new List<(string Table, string Redundant, string CoveredBy, List<string> RedundantCols, List<string> CoveringCols)>();

            foreach (var (tableKey, indexes) in raw)
            {
                for (int i = 0; i < indexes.Count; i++)
                {
                    for (int j = 0; j < indexes.Count; j++)
                    {
                        if (i == j) continue;
                        var (candidateName, _, candidateCols) = indexes[i];
                        var (coveringName, _, coveringCols) = indexes[j];
                        if (candidateCols.Count > coveringCols.Count) continue;
                        if (candidateCols.Count == coveringCols.Count && string.Equals(candidateName, coveringName, StringComparison.OrdinalIgnoreCase)) continue;

                        bool isPrefix = candidateCols
                            .Zip(coveringCols.Take(candidateCols.Count))
                            .All(p => string.Equals(p.First, p.Second, StringComparison.OrdinalIgnoreCase));

                        if (isPrefix && candidateCols.Count < coveringCols.Count)
                            redundant.Add((tableKey, candidateName, coveringName, candidateCols, coveringCols));
                    }
                }
            }

            // Deduplicate (A redundant vs B and B redundant vs A should only appear once)
            var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var deduped = redundant.Where(r =>
            {
                string key = $"{r.Table}|{r.Redundant}|{r.CoveredBy}";
                return seen.Add(key);
            }).ToList();

            var sb = new StringBuilder();
            sb.AppendLine($"DUPLICATE INDEX ANALYSIS: [{database}]");
            sb.AppendLine(new string('─', 90));

            if (deduped.Count == 0)
            {
                sb.AppendLine("  (no redundant indexes detected)");
            }
            else
            {
                foreach (var (tbl, redundantName, coveringName, rCols, cCols) in deduped)
                {
                    sb.AppendLine($"  Table: [{tbl}]");
                    sb.AppendLine($"    REDUNDANT: [{redundantName}]  ({string.Join(", ", rCols)})");
                    sb.AppendLine($"    COVERED BY: [{coveringName}]  ({string.Join(", ", cCols)})");
                    sb.AppendLine();
                }
            }

            sb.AppendLine($"  {deduped.Count} redundant index/indexes detected");
            return sb.ToString();
        }
        catch (Exception ex)
        {
            return $"ERROR: {ex.Message}";
        }
    }

    public async Task<string> FindUnusedTables(
        string database,
        CancellationToken cancellationToken = default)
    {
        if (!_databases.TryGetValue(database, out var connectionString))
            return UnknownDatabase(database);

        const string sql = """
            SELECT t.TABLE_SCHEMA, t.TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES t
            WHERE t.TABLE_TYPE = 'BASE TABLE'
                AND t.TABLE_NAME NOT LIKE @stagingPattern
                AND OBJECT_ID(QUOTENAME(t.TABLE_SCHEMA) + '.' + QUOTENAME(t.TABLE_NAME)) NOT IN (
                    SELECT referenced_id
                    FROM sys.sql_expression_dependencies
                    WHERE referenced_class = 1 AND referenced_id IS NOT NULL
                )
            ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME
            """;

        try
        {
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(cancellationToken);
            await using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@stagingPattern", StagingExcludeLike);
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

            var sb = new StringBuilder();
            sb.AppendLine($"UNUSED TABLES: [{database}]");
            sb.AppendLine(new string('─', 70));
            sb.AppendLine("  Tables with no references in any stored procedure or view.");
            sb.AppendLine("  NOTE: application-level references are not visible here — investigate before dropping.");
            sb.AppendLine();

            int count = 0;
            while (await reader.ReadAsync(cancellationToken))
            {
                count++;
                sb.AppendLine($"  [{reader.GetString(0)}].[{reader.GetString(1)}]");
            }

            if (count == 0)
                sb.AppendLine("  (all tables are referenced by at least one proc or view)");

            sb.AppendLine();
            sb.AppendLine($"  {count} unreferenced table(s)");
            return sb.ToString();
        }
        catch (Exception ex)
        {
            return $"ERROR: {ex.Message}";
        }
    }

    public async Task<string> FindUnusedProcedures(
        string database,
        CancellationToken cancellationToken = default)
    {
        if (!_databases.TryGetValue(database, out var connectionString))
            return UnknownDatabase(database);

        const string sql = """
            SELECT p.ROUTINE_SCHEMA, p.ROUTINE_NAME
            FROM INFORMATION_SCHEMA.ROUTINES p
            WHERE p.ROUTINE_TYPE = 'PROCEDURE'
                AND OBJECT_ID(QUOTENAME(p.ROUTINE_SCHEMA) + '.' + QUOTENAME(p.ROUTINE_NAME)) NOT IN (
                    SELECT referenced_id
                    FROM sys.sql_expression_dependencies
                    WHERE referenced_class = 1 AND referenced_id IS NOT NULL
                )
            ORDER BY p.ROUTINE_SCHEMA, p.ROUTINE_NAME
            """;

        try
        {
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(cancellationToken);
            await using var cmd = new SqlCommand(sql, conn);
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

            var sb = new StringBuilder();
            sb.AppendLine($"UNUSED PROCEDURES: [{database}]");
            sb.AppendLine(new string('─', 70));
            sb.AppendLine("  Procedures not referenced by any other SQL object.");
            sb.AppendLine("  NOTE: application-level calls are not visible here — investigate before dropping.");
            sb.AppendLine();

            int count = 0;
            while (await reader.ReadAsync(cancellationToken))
            {
                count++;
                sb.AppendLine($"  [{reader.GetString(0)}].[{reader.GetString(1)}]");
            }

            if (count == 0)
                sb.AppendLine("  (all procedures are referenced by at least one other object)");

            sb.AppendLine();
            sb.AppendLine($"  {count} unreferenced procedure(s)");
            return sb.ToString();
        }
        catch (Exception ex)
        {
            return $"ERROR: {ex.Message}";
        }
    }

    public async Task<string> AnalyzeProcComplexity(
        string database,
        string? nameFilter,
        CancellationToken cancellationToken = default)
    {
        if (!_databases.TryGetValue(database, out var connectionString))
            return UnknownDatabase(database);

        const string sql = """
            SELECT
                SCHEMA_NAME(o.schema_id) AS SchemaName,
                o.name AS ProcName,
                m.definition
            FROM sys.sql_modules m
            JOIN sys.objects o ON o.object_id = m.object_id
            WHERE o.type = 'P'
                AND (@nameFilter IS NULL OR o.name LIKE '%' + @nameFilter + '%')
            ORDER BY o.name
            """;

        try
        {
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(cancellationToken);
            await using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@nameFilter", (object?)nameFilter ?? DBNull.Value);

            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

            var sb = new StringBuilder();
            sb.AppendLine($"PROCEDURE COMPLEXITY ANALYSIS: [{database}]");
            sb.AppendLine(new string('─', 100));
            sb.AppendLine($"{"Procedure",-50} {"Lines",6} {"Cursor",7} {"TmpTbl",7} {"DynSQL",7} {"NOLOCK",7}  Flag");
            sb.AppendLine(new string('─', 100));

            int total = 0;
            int flagged = 0;
            var flaggedItems = new List<string>();

            while (await reader.ReadAsync(cancellationToken))
            {
                total++;
                string schema = reader.GetString(0);
                string name = reader.GetString(1);
                string def = reader.IsDBNull(2) ? "" : reader.GetString(2);
                string defUpper = def.ToUpperInvariant();

                int lines = def.Split('\n').Length;
                bool hasCursor = defUpper.Contains("CURSOR");
                bool hasTempTable = defUpper.Contains(" #") || defUpper.Contains("INTO #");
                bool hasDynamicSql = defUpper.Contains("EXEC(") || defUpper.Contains("EXECUTE(")
                    || defUpper.Contains("SP_EXECUTESQL") || defUpper.Contains("EXEC SP_EXECUTESQL");
                bool hasNolock = defUpper.Contains("NOLOCK");

                bool isRefactorCandidate = lines > 200 || hasCursor || hasDynamicSql;
                string flag = isRefactorCandidate ? "REFACTOR" : "";

                if (isRefactorCandidate)
                {
                    flagged++;
                    flaggedItems.Add($"  [{schema}].[{name}]  lines={lines} cursor={hasCursor} dynSql={hasDynamicSql}");
                }

                sb.AppendLine($"{$"[{schema}].[{name}]",-50} {lines,6} {BoolFlag(hasCursor),7} {BoolFlag(hasTempTable),7} {BoolFlag(hasDynamicSql),7} {BoolFlag(hasNolock),7}  {flag}");
            }

            sb.AppendLine(new string('─', 100));
            sb.AppendLine($"  {total} procedure(s) analysed, {flagged} refactor candidate(s)");

            if (flaggedItems.Count > 0)
            {
                sb.AppendLine();
                sb.AppendLine("REFACTOR CANDIDATES:");
                foreach (var item in flaggedItems)
                    sb.AppendLine(item);
            }

            return sb.ToString();
        }
        catch (Exception ex)
        {
            return $"ERROR: {ex.Message}";
        }
    }

    public async Task<string> AnalyzeViewComplexity(
        string database,
        string? nameFilter,
        CancellationToken cancellationToken = default)
    {
        if (!_databases.TryGetValue(database, out var connectionString))
            return UnknownDatabase(database);

        const string sql = """
            SELECT
                SCHEMA_NAME(o.schema_id) AS SchemaName,
                o.name AS ViewName,
                m.definition
            FROM sys.sql_modules m
            JOIN sys.objects o ON o.object_id = m.object_id
            WHERE o.type = 'V'
                AND (@nameFilter IS NULL OR o.name LIKE '%' + @nameFilter + '%')
            ORDER BY o.name
            """;

        try
        {
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(cancellationToken);
            await using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@nameFilter", (object?)nameFilter ?? DBNull.Value);

            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

            var views = new List<(string Schema, string Name, string Definition, int Lines)>();
            while (await reader.ReadAsync(cancellationToken))
            {
                string schema = reader.GetString(0);
                string name = reader.GetString(1);
                string def = reader.IsDBNull(2) ? "" : reader.GetString(2);
                views.Add((schema, name, def, def.Split('\n').Length));
            }

            var viewNames = views.Select(v => v.Name).ToHashSet(StringComparer.OrdinalIgnoreCase);

            var sb = new StringBuilder();
            sb.AppendLine($"VIEW COMPLEXITY ANALYSIS: [{database}]");
            sb.AppendLine(new string('─', 80));
            sb.AppendLine($"{"View",-50} {"Lines",6}  Nested Views");
            sb.AppendLine(new string('─', 80));

            int total = 0;
            int flagged = 0;

            foreach (var (schema, name, def, lines) in views)
            {
                total++;
                var nestedViews = viewNames
                    .Where(v => !string.Equals(v, name, StringComparison.OrdinalIgnoreCase)
                        && def.Contains(v, StringComparison.OrdinalIgnoreCase))
                    .ToList();

                bool isComplex = nestedViews.Count > 0 || lines > 100;
                if (isComplex) flagged++;

                string nested = nestedViews.Count > 0 ? string.Join(", ", nestedViews) : "";
                sb.AppendLine($"{$"[{schema}].[{name}]",-50} {lines,6}  {nested}");
            }

            sb.AppendLine(new string('─', 80));
            sb.AppendLine($"  {total} view(s) analysed, {flagged} complex view(s)");

            return sb.ToString();
        }
        catch (Exception ex)
        {
            return $"ERROR: {ex.Message}";
        }
    }

    public async Task<string> AnalyzeIndexFragmentation(
        string database,
        string? nameFilter,
        CancellationToken cancellationToken = default)
    {
        if (!_databases.TryGetValue(database, out var connectionString))
            return UnknownDatabase(database);

        const string sql = """
            SELECT
                OBJECT_SCHEMA_NAME(i.object_id) AS SchemaName,
                OBJECT_NAME(i.object_id) AS TableName,
                i.name AS IndexName,
                CAST(s.avg_fragmentation_in_percent AS decimal(5,1)) AS FragPct,
                s.page_count AS Pages
            FROM sys.indexes i
            CROSS APPLY sys.dm_db_index_physical_stats(DB_ID(), i.object_id, i.index_id, NULL, 'LIMITED') s
            WHERE i.type > 0
                AND i.is_disabled = 0
                AND s.page_count > 100
                AND s.avg_fragmentation_in_percent > 10
                AND (@nameFilter IS NULL OR OBJECT_NAME(i.object_id) LIKE '%' + @nameFilter + '%')
            ORDER BY s.avg_fragmentation_in_percent DESC
            """;

        SqlCommandGuard.AssertReadOnly(sql);

        try
        {
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(cancellationToken);
            await using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@nameFilter", (object?)nameFilter ?? DBNull.Value);
            cmd.CommandTimeout = 300;

            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

            var sb = new StringBuilder();
            sb.AppendLine($"INDEX FRAGMENTATION: [{database}]  (>10% frag, >100 pages)");
            sb.AppendLine(new string('─', 90));
            sb.AppendLine($"{"Table",-40} {"Index",-35} {"Frag%",6}  {"Pages",8}  Action");
            sb.AppendLine(new string('─', 90));

            int count = 0;
            while (await reader.ReadAsync(cancellationToken))
            {
                count++;
                string schema = reader.GetString(0);
                string table = reader.GetString(1);
                string index = reader.GetString(2);
                decimal frag = reader.GetDecimal(3);
                long pages = reader.GetInt64(4);

                string action = frag >= 30 ? "REBUILD" : "REORGANIZE";
                sb.AppendLine($"{$"[{schema}].[{table}]",-40} {index,-35} {frag,6}  {pages,8}  {action}");
            }

            if (count == 0)
                sb.AppendLine("  (no fragmented indexes found above thresholds)");
            else
                sb.AppendLine(new string('─', 90));

            sb.AppendLine($"  {count} fragmented index/indexes  (REBUILD recommended at >=30%, REORGANIZE at 10-29%)");
            return sb.ToString();
        }
        catch (Exception ex)
        {
            return $"ERROR: {ex.Message}";
        }
    }

    public async Task<string> AnalyzeTriggers(
        string database,
        CancellationToken cancellationToken = default)
    {
        if (!_databases.TryGetValue(database, out var connectionString))
            return UnknownDatabase(database);

        const string sql = """
            SELECT
                SCHEMA_NAME(p.schema_id) AS SchemaName,
                p.name AS TableName,
                t.name AS TriggerName,
                t.is_disabled,
                t.is_instead_of_trigger,
                MAX(CASE WHEN te.type_desc = 'INSERT' THEN 1 ELSE 0 END) AS OnInsert,
                MAX(CASE WHEN te.type_desc = 'UPDATE' THEN 1 ELSE 0 END) AS OnUpdate,
                MAX(CASE WHEN te.type_desc = 'DELETE' THEN 1 ELSE 0 END) AS OnDelete,
                o.modify_date
            FROM sys.triggers t
            JOIN sys.objects o ON o.object_id = t.object_id
            JOIN sys.objects p ON p.object_id = t.parent_id
            JOIN sys.trigger_events te ON te.object_id = t.object_id
            WHERE t.parent_class = 1
            GROUP BY SCHEMA_NAME(p.schema_id), p.name, t.name,
                     t.is_disabled, t.is_instead_of_trigger, o.modify_date
            ORDER BY p.name, t.name
            """;

        try
        {
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(cancellationToken);
            await using var cmd = new SqlCommand(sql, conn);
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

            var triggers = new List<(string Schema, string Table, string Name, bool Disabled, bool InsteadOf, bool Ins, bool Upd, bool Del, DateTime Modified)>();
            while (await reader.ReadAsync(cancellationToken))
                triggers.Add((reader.GetString(0), reader.GetString(1), reader.GetString(2),
                    reader.GetBoolean(3), reader.GetBoolean(4),
                    reader.GetInt32(5) == 1, reader.GetInt32(6) == 1, reader.GetInt32(7) == 1,
                    reader.GetDateTime(8)));

            var sb = new StringBuilder();
            sb.AppendLine($"TRIGGER ANALYSIS: [{database}]");
            sb.AppendLine(new string('─', 80));
            sb.AppendLine($"  Total triggers: {triggers.Count}");
            sb.AppendLine();

            var disabled = triggers.Where(t => t.Disabled).ToList();
            var insteadOf = triggers.Where(t => t.InsteadOf).ToList();
            var multiEvent = triggers.Where(t => (t.Ins ? 1 : 0) + (t.Upd ? 1 : 0) + (t.Del ? 1 : 0) > 1).ToList();

            // Tables with multiple triggers
            var multiTriggerTables = triggers
                .GroupBy(t => $"[{t.Schema}].[{t.Table}]")
                .Where(g => g.Count() > 1)
                .ToList();

            AppendTriggerSection(sb, "DISABLED TRIGGERS", [.. disabled.Select(t => $"  [{t.Schema}].[{t.Table}].{t.Name}  (modified {t.Modified:yyyy-MM-dd})")]);
            AppendTriggerSection(sb, "INSTEAD OF TRIGGERS (intercept DML — review carefully)", [.. insteadOf.Select(t => $"  [{t.Schema}].[{t.Table}].{t.Name}")]);
            AppendTriggerSection(sb, "TRIGGERS FIRING ON MULTIPLE EVENTS", [.. multiEvent.Select(t =>
            {
                string events = string.Join(",", new[] { t.Ins ? "INS" : null, t.Upd ? "UPD" : null, t.Del ? "DEL" : null }.Where(e => e != null));
                return $"  [{t.Schema}].[{t.Table}].{t.Name}  ({events})";
            })]);

            sb.AppendLine();
            sb.AppendLine($"TABLES WITH MULTIPLE TRIGGERS ({multiTriggerTables.Count})");
            sb.AppendLine(new string('-', 60));
            if (multiTriggerTables.Count == 0)
                sb.AppendLine("  (none)");
            else
                foreach (var g in multiTriggerTables)
                    sb.AppendLine($"  {g.Key}: {string.Join(", ", g.Select(t => t.Name))}");

            return sb.ToString();
        }
        catch (Exception ex)
        {
            return $"ERROR: {ex.Message}";
        }
    }

    private static void AppendTriggerSection(StringBuilder sb, string header, List<string> items)
    {
        sb.AppendLine($"{header} ({items.Count})");
        sb.AppendLine(new string('-', 60));
        if (items.Count == 0)
            sb.AppendLine("  (none)");
        else
            foreach (var item in items)
                sb.AppendLine(item);
        sb.AppendLine();
    }

    public async Task<string> AnalyzeIdentityColumns(
        string database,
        CancellationToken cancellationToken = default)
    {
        if (!_databases.TryGetValue(database, out var connectionString))
            return UnknownDatabase(database);

        const string sql = """
            SELECT
                SCHEMA_NAME(t.schema_id) AS SchemaName,
                t.name AS TableName,
                c.name AS ColumnName,
                tp.name AS DataType,
                CAST(ic.seed_value AS bigint) AS SeedValue,
                CAST(ic.last_value AS bigint) AS LastValue,
                CASE tp.name
                    WHEN 'tinyint'  THEN 255
                    WHEN 'smallint' THEN 32767
                    WHEN 'int'      THEN 2147483647
                    WHEN 'bigint'   THEN 9223372036854775807
                    ELSE 0
                END AS MaxValue
            FROM sys.identity_columns ic
            JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
            JOIN sys.objects t ON t.object_id = ic.object_id
            JOIN sys.types tp ON tp.user_type_id = c.user_type_id
            JOIN sys.tables st ON st.object_id = t.object_id
            WHERE t.type = 'U'
                AND st.name NOT LIKE @stagingPattern
            ORDER BY SCHEMA_NAME(t.schema_id), t.name
            """;

        try
        {
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(cancellationToken);
            await using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@stagingPattern", StagingExcludeLike);
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

            var sb = new StringBuilder();
            sb.AppendLine($"IDENTITY COLUMN ANALYSIS: [{database}]");
            sb.AppendLine(new string('─', 100));
            sb.AppendLine($"{"Table",-45} {"Column",-25} {"Type",-10} {"Last Value",15} {"Max Value",20} {"Used%",7}  Flag");
            sb.AppendLine(new string('─', 100));

            int total = 0;
            int flagged = 0;
            while (await reader.ReadAsync(cancellationToken))
            {
                total++;
                string schema = reader.GetString(0);
                string table = reader.GetString(1);
                string col = reader.GetString(2);
                string type = reader.GetString(3);
                long seed = reader.IsDBNull(4) ? 1 : reader.GetInt64(4);
                long? lastVal = reader.IsDBNull(5) ? null : reader.GetInt64(5);
                long maxVal = reader.GetInt64(6);

                double pct = lastVal.HasValue && maxVal > 0
                    ? Math.Round((lastVal.Value - seed + 1) * 100.0 / (maxVal - seed + 1), 1)
                    : 0;

                string flag = pct >= 90 ? "CRITICAL" : pct >= 70 ? "WARNING" : "";
                if (flag.Length > 0) flagged++;

                sb.AppendLine($"{$"[{schema}].[{table}]",-45} {col,-25} {type,-10} {(lastVal.HasValue ? lastVal.Value.ToString(CultureInfo.InvariantCulture) : "none"),15} {maxVal,20} {pct,6:F1}%  {flag}");
            }

            sb.AppendLine(new string('─', 100));
            sb.AppendLine($"  {total} identity column(s), {flagged} flagged (>=70% capacity used)");
            return sb.ToString();
        }
        catch (Exception ex)
        {
            return $"ERROR: {ex.Message}";
        }
    }

    public async Task<string> AnalyzeTableSizes(
        string database,
        CancellationToken cancellationToken = default)
    {
        if (!_databases.TryGetValue(database, out var connectionString))
            return UnknownDatabase(database);

        const string sql = """
            SELECT
                SCHEMA_NAME(t.schema_id) AS SchemaName,
                t.name AS TableName,
                SUM(CASE WHEN i.index_id <= 1 THEN ps.row_count ELSE 0 END) AS ApproxRows,
                SUM(ps.reserved_page_count) * 8 AS TotalKB,
                SUM(ps.used_page_count) * 8 AS UsedKB,
                (SUM(ps.reserved_page_count) - SUM(ps.used_page_count)) * 8 AS FreeKB
            FROM sys.tables t
            JOIN sys.indexes i ON i.object_id = t.object_id
            JOIN sys.dm_db_partition_stats ps
                ON ps.object_id = t.object_id AND ps.index_id = i.index_id
            GROUP BY SCHEMA_NAME(t.schema_id), t.name
            ORDER BY SUM(ps.reserved_page_count) DESC
            """;

        SqlCommandGuard.AssertReadOnly(sql);

        try
        {
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(cancellationToken);
            await using var cmd = new SqlCommand(sql, conn);
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

            var sb = new StringBuilder();
            sb.AppendLine($"TABLE SIZES: [{database}]");
            sb.AppendLine(new string('─', 90));
            sb.AppendLine($"{"Table",-50} {"Rows",12} {"Total",10} {"Used",10} {"Free",10}");
            sb.AppendLine(new string('─', 90));

            long totalKbAll = 0;
            int count = 0;
            while (await reader.ReadAsync(cancellationToken))
            {
                count++;
                string schema = reader.GetString(0);
                string table = reader.GetString(1);
                long rows = reader.GetInt64(2);
                long totalKb = reader.GetInt64(3);
                long usedKb = reader.GetInt64(4);
                long freeKb = reader.GetInt64(5);
                totalKbAll += totalKb;

                sb.AppendLine($"{$"[{schema}].[{table}]",-50} {rows,12:N0} {FormatKb(totalKb),10} {FormatKb(usedKb),10} {FormatKb(freeKb),10}");
            }

            sb.AppendLine(new string('─', 90));
            sb.AppendLine($"  {count} table(s)   Total reserved: {FormatKb(totalKbAll)}");
            return sb.ToString();
        }
        catch (Exception ex)
        {
            return $"ERROR: {ex.Message}";
        }
    }

    public async Task<string> AnalyzeMissingIndexSuggestions(
        string database,
        CancellationToken cancellationToken = default)
    {
        if (!_databases.TryGetValue(database, out var connectionString))
            return UnknownDatabase(database);

        const string sql = """
            SELECT
                SCHEMA_NAME(t.schema_id) AS SchemaName,
                t.name AS TableName,
                mid.equality_columns,
                mid.inequality_columns,
                mid.included_columns,
                ROUND(migs.avg_total_user_cost * migs.avg_user_impact
                    * (migs.user_seeks + migs.user_scans), 0) AS ImpactScore,
                migs.user_seeks,
                migs.user_scans,
                ROUND(migs.avg_user_impact, 1) AS AvgImpactPct
            FROM sys.dm_db_missing_index_details mid
            JOIN sys.dm_db_missing_index_groups mig
                ON mig.index_handle = mid.index_handle
            JOIN sys.dm_db_missing_index_group_stats migs
                ON migs.group_handle = mig.index_group_handle
            JOIN sys.objects t ON t.object_id = mid.object_id
            WHERE mid.database_id = DB_ID()
            ORDER BY ImpactScore DESC
            """;

        SqlCommandGuard.AssertReadOnly(sql);

        try
        {
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(cancellationToken);
            await using var cmd = new SqlCommand(sql, conn);
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

            var sb = new StringBuilder();
            sb.AppendLine($"MISSING INDEX SUGGESTIONS: [{database}]  (SQL Server recommendations)");
            sb.AppendLine(new string('─', 100));

            int count = 0;
            while (await reader.ReadAsync(cancellationToken))
            {
                count++;
                string schema = reader.GetString(0);
                string table = reader.GetString(1);
                string? eqCols = reader.IsDBNull(2) ? null : reader.GetString(2);
                string? ineqCols = reader.IsDBNull(3) ? null : reader.GetString(3);
                string? inclCols = reader.IsDBNull(4) ? null : reader.GetString(4);
                long impact = (long)reader.GetDouble(5);
                long seeks = reader.GetInt64(6);
                long scans = reader.GetInt64(7);
                double avgImpact = reader.GetDouble(8);

                sb.AppendLine($"  Table:    [{schema}].[{table}]");
                sb.AppendLine($"  Impact:   {impact:N0}  (seeks: {seeks:N0}, scans: {scans:N0}, avg benefit: {avgImpact}%)");
                if (eqCols != null) sb.AppendLine($"  Equality: {eqCols}");
                if (ineqCols != null) sb.AppendLine($"  Inequality: {ineqCols}");
                if (inclCols != null) sb.AppendLine($"  Include:  {inclCols}");
                sb.AppendLine(new string('-', 70));
            }

            if (count == 0)
                sb.AppendLine("  (no missing index suggestions — data resets on server restart)");
            else
                sb.AppendLine($"  {count} suggestion(s)");

            return sb.ToString();
        }
        catch (Exception ex)
        {
            return $"ERROR: {ex.Message}";
        }
    }

    public async Task<string> GetRecentObjectChanges(
        string database,
        int days,
        CancellationToken cancellationToken = default)
    {
        if (!_databases.TryGetValue(database, out var connectionString))
            return UnknownDatabase(database);

        if (days < 1) days = 1;
        if (days > 365) days = 365;

        const string sql = """
            SELECT
                SCHEMA_NAME(o.schema_id) AS SchemaName,
                o.name AS ObjectName,
                o.type_desc AS ObjectType,
                o.create_date AS CreatedAt,
                o.modify_date AS ModifiedAt
            FROM sys.objects o
            WHERE o.type IN ('U', 'V', 'P', 'FN', 'IF', 'TF', 'TR')
                AND o.modify_date >= DATEADD(day, -@days, GETUTCDATE())
                AND o.is_ms_shipped = 0
            ORDER BY o.modify_date DESC
            """;

        try
        {
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(cancellationToken);
            await using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@days", days);
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

            var sb = new StringBuilder();
            sb.AppendLine($"RECENT OBJECT CHANGES: [{database}]  (last {days} day(s))");
            sb.AppendLine(new string('─', 90));
            sb.AppendLine($"{"Modified",-20} {"Type",-30} {"Object",-35} Created");
            sb.AppendLine(new string('─', 90));

            int count = 0;
            while (await reader.ReadAsync(cancellationToken))
            {
                count++;
                string schema = reader.GetString(0);
                string name = reader.GetString(1);
                string type = reader.GetString(2);
                DateTime created = reader.GetDateTime(3);
                DateTime modified = reader.GetDateTime(4);

                bool isNew = (modified - created).TotalMinutes < 2;
                string label = $"[{schema}].[{name}]";
                sb.AppendLine($"{modified,-20:yyyy-MM-dd HH:mm} {type,-30} {label,-35} {(isNew ? "NEW" : created.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture))}");
            }

            if (count == 0)
                sb.AppendLine($"  (no objects modified in the last {days} day(s))");
            else
                sb.AppendLine(new string('─', 90));
            sb.AppendLine($"  {count} change(s)");
            return sb.ToString();
        }
        catch (Exception ex)
        {
            return $"ERROR: {ex.Message}";
        }
    }

    public async Task<string> AnalyzeTableQueryStats(
        string database,
        CancellationToken cancellationToken = default)
    {
        if (!_databases.TryGetValue(database, out var connectionString))
            return UnknownDatabase(database);

        const string uptimeSql = "SELECT sqlserver_start_time FROM sys.dm_os_sys_info";

        const string statsSql = """
            SELECT
                SCHEMA_NAME(t.schema_id) AS SchemaName,
                t.name AS TableName,
                SUM(ISNULL(u.user_seeks, 0))   AS TotalSeeks,
                SUM(ISNULL(u.user_scans, 0))   AS TotalScans,
                SUM(ISNULL(u.user_lookups, 0)) AS TotalLookups,
                SUM(ISNULL(u.user_updates, 0)) AS TotalUpdates
            FROM sys.tables t
            JOIN sys.indexes i ON i.object_id = t.object_id
            LEFT JOIN sys.dm_db_index_usage_stats u
                ON u.object_id = i.object_id
                AND u.index_id = i.index_id
                AND u.database_id = DB_ID()
            WHERE t.name NOT LIKE @stagingPattern
            GROUP BY SCHEMA_NAME(t.schema_id), t.name
            HAVING SUM(ISNULL(u.user_seeks, 0))
                 + SUM(ISNULL(u.user_scans, 0))
                 + SUM(ISNULL(u.user_lookups, 0))
                 + SUM(ISNULL(u.user_updates, 0)) > 0
            ORDER BY SUM(ISNULL(u.user_seeks, 0))
                   + SUM(ISNULL(u.user_scans, 0))
                   + SUM(ISNULL(u.user_lookups, 0)) DESC
            """;

        SqlCommandGuard.AssertReadOnly(statsSql);

        try
        {
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(cancellationToken);

            DateTime serverStart;
            await using (var cmd = new SqlCommand(uptimeSql, conn))
                serverStart = (DateTime)(await cmd.ExecuteScalarAsync(cancellationToken))!;

            double uptimeDays = Math.Max((DateTime.UtcNow - serverStart).TotalDays, 1);

            await using var statsCmd = new SqlCommand(statsSql, conn);
            statsCmd.Parameters.AddWithValue("@stagingPattern", StagingExcludeLike);
            await using var reader = await statsCmd.ExecuteReaderAsync(cancellationToken);

            var rows = new List<(string Schema, string Table, long Reads, long Updates)>();
            while (await reader.ReadAsync(cancellationToken))
            {
                long seeks   = reader.GetInt64(2);
                long scans   = reader.GetInt64(3);
                long lookups = reader.GetInt64(4);
                long updates = reader.GetInt64(5);
                rows.Add((reader.GetString(0), reader.GetString(1), seeks + scans + lookups, updates));
            }

            var sb = new StringBuilder();
            sb.AppendLine($"TABLE QUERY STATS: [{database}]");
            sb.AppendLine($"  Server start: {serverStart:yyyy-MM-dd HH:mm}  ({uptimeDays:F1} days uptime)");
            sb.AppendLine($"  Source: sys.dm_db_index_usage_stats — cumulative since server start, divided by uptime for daily avg.");
            sb.AppendLine($"  Only tables with at least 1 recorded operation are shown. Staging snapshots excluded.");
            sb.AppendLine(new string('─', 100));
            sb.AppendLine($"{"Table",-50} {"Total Reads",14} {"Reads/day",10} {"Total Writes",14} {"Writes/day",10}");
            sb.AppendLine(new string('─', 100));

            foreach (var (schema, table, reads, updates) in rows)
            {
                double readsPerDay   = reads   / uptimeDays;
                double updatesPerDay = updates / uptimeDays;
                sb.AppendLine($"{$"[{schema}].[{table}]",-50} {reads,14:N0} {readsPerDay,10:N1} {updates,14:N0} {updatesPerDay,10:N1}");
            }

            sb.AppendLine(new string('─', 100));
            sb.AppendLine($"  {rows.Count} table(s) with recorded activity");
            return sb.ToString();
        }
        catch (Exception ex)
        {
            return $"ERROR: {ex.Message}";
        }
    }

    public async Task<string> AnalyzeTableAccessStats(
        string database,
        CancellationToken cancellationToken = default)
    {
        if (!_databases.TryGetValue(database, out var connectionString))
            return UnknownDatabase(database);

        const string sql = """
            SELECT
                SCHEMA_NAME(t.schema_id) AS SchemaName,
                t.name AS TableName,
                MAX(u.last_user_seek)   AS LastSeek,
                MAX(u.last_user_scan)   AS LastScan,
                MAX(u.last_user_lookup) AS LastLookup,
                MAX(u.last_user_update) AS LastUpdate
            FROM sys.tables t
            JOIN sys.indexes i ON i.object_id = t.object_id
            LEFT JOIN sys.dm_db_index_usage_stats u
                ON u.object_id = i.object_id
                AND u.index_id = i.index_id
                AND u.database_id = DB_ID()
            WHERE t.name NOT LIKE @stagingPattern
            GROUP BY SCHEMA_NAME(t.schema_id), t.name
            """;

        SqlCommandGuard.AssertReadOnly(sql);

        try
        {
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(cancellationToken);
            await using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@stagingPattern", StagingExcludeLike);
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

            var rows = new List<(string Schema, string Table, DateTime? LastRead, DateTime? LastWrite)>();
            while (await reader.ReadAsync(cancellationToken))
            {
                DateTime? seek   = reader.IsDBNull(2) ? null : reader.GetDateTime(2);
                DateTime? scan   = reader.IsDBNull(3) ? null : reader.GetDateTime(3);
                DateTime? lookup = reader.IsDBNull(4) ? null : reader.GetDateTime(4);
                DateTime? update = reader.IsDBNull(5) ? null : reader.GetDateTime(5);

                DateTime? lastRead = new[] { seek, scan, lookup }
                    .Where(d => d.HasValue)
                    .Select(d => d!.Value)
                    .DefaultIfEmpty()
                    .Max() is DateTime r && r != default ? r : null;

                rows.Add((reader.GetString(0), reader.GetString(1), lastRead, update));
            }

            // Sort: never accessed first, then oldest access first
            rows = [.. rows.OrderBy(r => r.LastRead.HasValue || r.LastWrite.HasValue ? 1 : 0)
                           .ThenBy(r =>
                           {
                               if (r.LastRead.HasValue && r.LastWrite.HasValue)
                                   return r.LastRead.Value > r.LastWrite.Value ? r.LastRead.Value : r.LastWrite.Value;
                               return r.LastRead ?? r.LastWrite ?? DateTime.MinValue;
                           })];

            var sb = new StringBuilder();
            sb.AppendLine($"TABLE ACCESS STATS: [{database}]");
            sb.AppendLine("  Source: sys.dm_db_index_usage_stats — resets on SQL Server restart.");
            sb.AppendLine("  NULL = no access recorded since last restart, not necessarily never used.");
            sb.AppendLine(new string('─', 90));
            sb.AppendLine($"{"Table",-50} {"Last Read",-22} {"Last Write",-22}");
            sb.AppendLine(new string('─', 90));

            int neverCount = 0;
            foreach (var (schema, table, lastRead, lastWrite) in rows)
            {
                bool neverAccessed = lastRead is null && lastWrite is null;
                if (neverAccessed) neverCount++;

                string readLabel  = lastRead.HasValue  ? lastRead.Value.ToString("yyyy-MM-dd HH:mm", CultureInfo.InvariantCulture)  : "(none)";
                string writeLabel = lastWrite.HasValue ? lastWrite.Value.ToString("yyyy-MM-dd HH:mm", CultureInfo.InvariantCulture) : "(none)";

                sb.AppendLine($"{$"[{schema}].[{table}]",-50} {readLabel,-22} {writeLabel,-22}");
            }

            sb.AppendLine(new string('─', 90));
            sb.AppendLine($"  {rows.Count} table(s)   {neverCount} with no recorded access since last restart");
            return sb.ToString();
        }
        catch (Exception ex)
        {
            return $"ERROR: {ex.Message}";
        }
    }

    public async Task<string> GenerateDatabaseSummary(
        string database,
        CancellationToken cancellationToken = default)
    {
        if (!_databases.TryGetValue(database, out var connectionString))
            return UnknownDatabase(database);

        const string countsSql = """
            SELECT
                SUM(CASE WHEN type = 'U'  THEN 1 ELSE 0 END) AS Tables,
                SUM(CASE WHEN type = 'V'  THEN 1 ELSE 0 END) AS Views,
                SUM(CASE WHEN type = 'P'  THEN 1 ELSE 0 END) AS Procedures,
                SUM(CASE WHEN type IN ('FN','IF','TF','FS','FT') THEN 1 ELSE 0 END) AS Functions,
                SUM(CASE WHEN type = 'TR' THEN 1 ELSE 0 END) AS Triggers,
                SUM(CASE WHEN type = 'SN' THEN 1 ELSE 0 END) AS Synonyms
            FROM sys.objects WHERE is_ms_shipped = 0
            """;

        const string topTablesSql = """
            SELECT TOP 10
                SCHEMA_NAME(t.schema_id) AS SchemaName,
                t.name AS TableName,
                SUM(CASE WHEN i.index_id <= 1 THEN ps.row_count ELSE 0 END) AS ApproxRows,
                SUM(ps.reserved_page_count) * 8 / 1024 AS TotalMB
            FROM sys.tables t
            JOIN sys.indexes i ON i.object_id = t.object_id
            JOIN sys.dm_db_partition_stats ps
                ON ps.object_id = t.object_id AND ps.index_id = i.index_id
            GROUP BY SCHEMA_NAME(t.schema_id), t.name
            ORDER BY SUM(ps.row_count) DESC
            """;

        const string healthSql = """
            SELECT
                (SELECT COUNT(*) FROM sys.tables t
                    WHERE NOT EXISTS (SELECT 1 FROM sys.indexes i
                        WHERE i.object_id = t.object_id AND i.is_primary_key = 1)) AS TablesNoPK,
                (SELECT COUNT(*) FROM sys.tables t
                    WHERE NOT EXISTS (SELECT 1 FROM sys.indexes i
                        WHERE i.object_id = t.object_id AND i.type = 1)) AS Heaps,
                (SELECT COUNT(*) FROM sys.triggers WHERE is_disabled = 1) AS DisabledTriggers,
                (SELECT COUNT(*) FROM sys.indexes WHERE is_disabled = 1) AS DisabledIndexes,
                (SELECT COUNT(*) FROM sys.check_constraints WHERE is_disabled = 1) AS DisabledCheckConstraints
            """;

        const string sizeSql = """
            SELECT
                SUM(reserved_page_count) * 8 / 1024 AS ReservedMB,
                SUM(used_page_count) * 8 / 1024 AS UsedMB
            FROM sys.dm_db_partition_stats
            """;

        SqlCommandGuard.AssertReadOnly(topTablesSql);
        SqlCommandGuard.AssertReadOnly(sizeSql);

        try
        {
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(cancellationToken);

            var sb = new StringBuilder();
            sb.AppendLine($"DATABASE SUMMARY: [{database}]");
            sb.AppendLine(new string('═', 70));

            // Object counts
            await using (var cmd = new SqlCommand(countsSql, conn))
            await using (var r = await cmd.ExecuteReaderAsync(cancellationToken))
            {
                if (await r.ReadAsync(cancellationToken))
                {
                    sb.AppendLine("OBJECT COUNTS");
                    sb.AppendLine(new string('─', 50));
                    sb.AppendLine($"  Tables:      {r.GetInt32(0),6:N0}");
                    sb.AppendLine($"  Views:       {r.GetInt32(1),6:N0}");
                    sb.AppendLine($"  Procedures:  {r.GetInt32(2),6:N0}");
                    sb.AppendLine($"  Functions:   {r.GetInt32(3),6:N0}");
                    sb.AppendLine($"  Triggers:    {r.GetInt32(4),6:N0}");
                    sb.AppendLine($"  Synonyms:    {r.GetInt32(5),6:N0}");
                }
            }

            // Size
            sb.AppendLine();
            await using (var cmd = new SqlCommand(sizeSql, conn))
            await using (var r = await cmd.ExecuteReaderAsync(cancellationToken))
            {
                if (await r.ReadAsync(cancellationToken))
                {
                    sb.AppendLine("DATABASE SIZE");
                    sb.AppendLine(new string('─', 50));
                    sb.AppendLine($"  Reserved: {r.GetInt64(0),8:N0} MB");
                    sb.AppendLine($"  Used:     {r.GetInt64(1),8:N0} MB");
                }
            }

            // Top tables
            sb.AppendLine();
            sb.AppendLine("TOP 10 TABLES BY ROW COUNT");
            sb.AppendLine(new string('─', 70));
            sb.AppendLine($"  {"Table",-45} {"Rows",12} {"Size (MB)",10}");
            sb.AppendLine(new string('─', 70));
            await using (var cmd = new SqlCommand(topTablesSql, conn))
            await using (var r = await cmd.ExecuteReaderAsync(cancellationToken))
            {
                while (await r.ReadAsync(cancellationToken))
                    sb.AppendLine($"  {$"[{r.GetString(0)}].[{r.GetString(1)}]",-45} {r.GetInt64(2),12:N0} {r.GetInt64(3),10:N0}");
            }

            // Health flags
            sb.AppendLine();
            sb.AppendLine("HEALTH FLAGS");
            sb.AppendLine(new string('─', 50));
            await using (var cmd = new SqlCommand(healthSql, conn))
            await using (var r = await cmd.ExecuteReaderAsync(cancellationToken))
            {
                if (await r.ReadAsync(cancellationToken))
                {
                    sb.AppendLine($"  Tables without PK:          {r.GetInt32(0),5}");
                    sb.AppendLine($"  Heap tables (no clustered): {r.GetInt32(1),5}");
                    sb.AppendLine($"  Disabled triggers:          {r.GetInt32(2),5}");
                    sb.AppendLine($"  Disabled indexes:           {r.GetInt32(3),5}");
                    sb.AppendLine($"  Disabled check constraints: {r.GetInt32(4),5}");
                }
            }

            return sb.ToString();
        }
        catch (Exception ex)
        {
            return $"ERROR: {ex.Message}";
        }
    }
}
