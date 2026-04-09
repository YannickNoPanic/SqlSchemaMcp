using System.Globalization;
using System.Text;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;
using SqlSchemaMcp.Configuration;

namespace SqlSchemaMcp.Data;

public sealed class CompareQueries(IOptions<SqlServerOptions> options) : SqlQueryBase(options)
{
    public async Task<HashSet<string>> GetTableNames(
        string database,
        CancellationToken cancellationToken = default)
    {
        if (!_databases.TryGetValue(database, out var connectionString))
            return [];

        const string sql = """
            SELECT TABLE_SCHEMA + '.' + TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_SCHEMA, TABLE_NAME
            """;

        try
        {
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(cancellationToken);
            await using var cmd = new SqlCommand(sql, conn);
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

            var result = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            while (await reader.ReadAsync(cancellationToken))
                result.Add(reader.GetString(0));
            return result;
        }
        catch
        {
            return [];
        }
    }

    public async Task<HashSet<string>> GetProcNames(
        string database,
        CancellationToken cancellationToken = default)
    {
        if (!_databases.TryGetValue(database, out var connectionString))
            return [];

        const string sql = """
            SELECT ROUTINE_SCHEMA + '.' + ROUTINE_NAME
            FROM INFORMATION_SCHEMA.ROUTINES
            WHERE ROUTINE_TYPE = 'PROCEDURE'
            ORDER BY ROUTINE_SCHEMA, ROUTINE_NAME
            """;

        try
        {
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(cancellationToken);
            await using var cmd = new SqlCommand(sql, conn);
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

            var result = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            while (await reader.ReadAsync(cancellationToken))
                result.Add(reader.GetString(0));
            return result;
        }
        catch
        {
            return [];
        }
    }

    public async Task<HashSet<string>> GetViewNames(
        string database,
        CancellationToken cancellationToken = default)
    {
        if (!_databases.TryGetValue(database, out var connectionString))
            return [];

        const string sql = """
            SELECT TABLE_SCHEMA + '.' + TABLE_NAME
            FROM INFORMATION_SCHEMA.VIEWS
            ORDER BY TABLE_SCHEMA, TABLE_NAME
            """;

        try
        {
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(cancellationToken);
            await using var cmd = new SqlCommand(sql, conn);
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

            var result = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            while (await reader.ReadAsync(cancellationToken))
                result.Add(reader.GetString(0));
            return result;
        }
        catch
        {
            return [];
        }
    }

    public async Task<List<ColumnInfo>> GetTableColumns(
        string database,
        string tableName,
        CancellationToken cancellationToken = default)
    {
        if (!_databases.TryGetValue(database, out var connectionString))
            return [];

        var (schema, table) = ParseSchemaTable(tableName);

        const string sql = """
            SELECT
                c.COLUMN_NAME,
                c.DATA_TYPE,
                c.CHARACTER_MAXIMUM_LENGTH,
                c.NUMERIC_PRECISION,
                c.NUMERIC_SCALE,
                c.IS_NULLABLE
            FROM INFORMATION_SCHEMA.COLUMNS c
            WHERE c.TABLE_SCHEMA = @schema AND c.TABLE_NAME = @table
            ORDER BY c.ORDINAL_POSITION
            """;

        try
        {
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(cancellationToken);
            await using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@schema", schema);
            cmd.Parameters.AddWithValue("@table", table);
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

            var result = new List<ColumnInfo>();
            while (await reader.ReadAsync(cancellationToken))
            {
                int? maxLen = reader.IsDBNull(2) ? null : reader.GetInt32(2);
                int? precision = reader.IsDBNull(3) ? null : (int)reader.GetByte(3);
                int? scale = reader.IsDBNull(4) ? null : (int)reader.GetByte(4);
                result.Add(new ColumnInfo(
                    reader.GetString(0),
                    FormatColumnType(reader.GetString(1), maxLen, precision, scale),
                    reader.GetString(5)));
            }
            return result;
        }
        catch
        {
            return [];
        }
    }

    public async Task<(int LineCount, List<string> TablesReferenced)> GetProcStats(
        string database,
        string procName,
        CancellationToken cancellationToken = default)
    {
        if (!_databases.TryGetValue(database, out var connectionString))
            return (0, []);

        const string defSql = """
            SELECT m.definition
            FROM sys.sql_modules m
            JOIN sys.objects o ON o.object_id = m.object_id
            WHERE o.type = 'P' AND o.name = @name
            """;

        const string refSql = """
            SELECT DISTINCT re.referenced_entity_name
            FROM sys.dm_sql_referenced_entities(@qualifiedName, 'OBJECT') re
            JOIN sys.objects o ON o.name = re.referenced_entity_name
                AND o.schema_id = COALESCE(SCHEMA_ID(re.referenced_schema_name), SCHEMA_ID('dbo'))
            WHERE o.type IN ('U', 'V')
            ORDER BY re.referenced_entity_name
            """;

        try
        {
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(cancellationToken);

            string def = "";
            await using (var cmd = new SqlCommand(defSql, conn))
            {
                cmd.Parameters.AddWithValue("@name", procName.Trim('[', ']'));
                var result = await cmd.ExecuteScalarAsync(cancellationToken);
                def = result as string ?? "";
            }

            if (string.IsNullOrEmpty(def))
                return (0, []);

            int lines = def.Split('\n').Length;
            var tables = new List<string>();

            string qualifiedName = procName.Contains('.') ? procName : $"dbo.{procName}";
            await using (var cmd = new SqlCommand(refSql, conn))
            {
                cmd.Parameters.AddWithValue("@qualifiedName", qualifiedName);
                try
                {
                    await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
                    while (await reader.ReadAsync(cancellationToken))
                        tables.Add(reader.GetString(0));
                }
                catch
                {
                    // dm_sql_referenced_entities can fail for some procs; treat as empty
                }
            }

            return (lines, tables);
        }
        catch
        {
            return (0, []);
        }
    }

    public async Task<(int LineCount, List<string> TablesReferenced)> GetViewStats(
        string database,
        string viewName,
        CancellationToken cancellationToken = default)
    {
        if (!_databases.TryGetValue(database, out var connectionString))
            return (0, []);

        const string defSql = """
            SELECT m.definition
            FROM sys.sql_modules m
            JOIN sys.objects o ON o.object_id = m.object_id
            WHERE o.type = 'V' AND o.name = @name
            """;

        const string refSql = """
            SELECT DISTINCT re.referenced_entity_name
            FROM sys.dm_sql_referenced_entities(@qualifiedName, 'OBJECT') re
            JOIN sys.objects o ON o.name = re.referenced_entity_name
                AND o.schema_id = COALESCE(SCHEMA_ID(re.referenced_schema_name), SCHEMA_ID('dbo'))
            WHERE o.type IN ('U', 'V')
            ORDER BY re.referenced_entity_name
            """;

        try
        {
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(cancellationToken);

            string def = "";
            await using (var cmd = new SqlCommand(defSql, conn))
            {
                cmd.Parameters.AddWithValue("@name", viewName.Trim('[', ']'));
                var result = await cmd.ExecuteScalarAsync(cancellationToken);
                def = result as string ?? "";
            }

            if (string.IsNullOrEmpty(def))
                return (0, []);

            int lines = def.Split('\n').Length;
            var tables = new List<string>();

            string qualifiedName = viewName.Contains('.') ? viewName : $"dbo.{viewName}";
            await using (var cmd = new SqlCommand(refSql, conn))
            {
                cmd.Parameters.AddWithValue("@qualifiedName", qualifiedName);
                try
                {
                    await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
                    while (await reader.ReadAsync(cancellationToken))
                        tables.Add(reader.GetString(0));
                }
                catch
                {
                    // dm_sql_referenced_entities can fail for some views; treat as empty
                }
            }

            return (lines, tables);
        }
        catch
        {
            return (0, []);
        }
    }
}

public sealed record ColumnInfo(string Name, string Type, string Nullable);
