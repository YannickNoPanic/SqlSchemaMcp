using System.Text;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MySqlConnector;
using SqlSchemaMcp.Abstractions.Capabilities;
using SqlSchemaMcp.MariaDb.Configuration;

namespace SqlSchemaMcp.MariaDb.Data;

public sealed partial class MariaDbSchema(IOptions<MariaDbEngineOptions> options, ILogger<MariaDbSchema> logger)
    : MariaDbQueryBase(options, logger), ISchemaCapability
{
    public Task<string> ListTables(string database, string? schemaFilter, string? nameFilter, CancellationToken ct) =>
        ListObjects(database, "TABLES", "BASE TABLE", schemaFilter, nameFilter, ct);

    public Task<string> ListViews(string database, string? nameFilter, CancellationToken ct) =>
        ListObjects(database, "VIEWS", "VIEW", null, nameFilter, ct);

    public Task<string> ListProcedures(string database, string? nameFilter, CancellationToken ct) =>
        ListRoutines(database, "PROCEDURES", "PROCEDURE", nameFilter, ct);

    public Task<string> ListFunctions(string database, string? nameFilter, CancellationToken ct) =>
        ListRoutines(database, "FUNCTIONS", "FUNCTION", nameFilter, ct);

    public async Task<string> GetTableSchema(string database, string tableName, CancellationToken ct)
    {
        var (schema, table) = ParseSchemaObject(tableName);
        const string columnSql = """
            SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE,
                   IS_NULLABLE, COLUMN_DEFAULT
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = COALESCE(@schema, DATABASE()) AND TABLE_NAME = @table
            ORDER BY ORDINAL_POSITION
            """;
        const string fkSql = """
            SELECT CONSTRAINT_NAME, COLUMN_NAME, REFERENCED_TABLE_SCHEMA, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
            FROM information_schema.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = COALESCE(@schema, DATABASE()) AND TABLE_NAME = @table AND REFERENCED_TABLE_NAME IS NOT NULL
            ORDER BY CONSTRAINT_NAME
            """;
        const string indexSql = """
            SELECT INDEX_NAME, GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX SEPARATOR ', ') AS Columns
            FROM information_schema.STATISTICS
            WHERE TABLE_SCHEMA = COALESCE(@schema, DATABASE()) AND TABLE_NAME = @table
            GROUP BY INDEX_NAME
            ORDER BY INDEX_NAME
            """;

        var open = await OpenConnection(database, ct);
        if (open.Error is not null)
            return open.Error;
        await using var conn = open.Connection!;

        try
        {
            var sb = new StringBuilder();
            sb.AppendLine($"TABLE: [{schema ?? "(current)"}].[{table}]");
            sb.AppendLine(new string('-', 90));
            sb.AppendLine($"{"Column",-35} {"Type",-25} {"Null",-6} {"Default"}");
            sb.AppendLine(new string('-', 90));

            await using (var cmd = new MySqlCommand(columnSql, conn))
            {
                cmd.Parameters.AddWithValue("@schema", (object?)schema ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@table", table);
                await using var reader = await cmd.ExecuteReaderAsync(ct);
                bool any = false;
                while (await reader.ReadAsync(ct))
                {
                    any = true;
                    string type = MariaDbTypeMapper.FormatColumnType(
                        reader.GetString(1),
                        reader.IsDBNull(2) ? null : reader.GetInt64(2),
                        reader.IsDBNull(3) ? null : reader.GetInt64(3),
                        reader.IsDBNull(4) ? null : reader.GetInt64(4));
                    string nullable = reader.GetString(5) == "YES" ? "YES" : "NO";
                    string defaultValue = reader.IsDBNull(6) ? "" : reader.GetString(6);
                    sb.AppendLine($"{reader.GetString(0),-35} {type,-25} {nullable,-6} {defaultValue}");
                }

                if (!any)
                    return $"ERROR: Table '{tableName}' not found in [{database}].";
            }

            await AppendForeignKeys(conn, sb, fkSql, schema, table, ct);
            await AppendIndexes(conn, sb, indexSql, schema, table, ct);
            return sb.ToString();
        }
        catch (Exception ex)
        {
            return SafeError(ex, nameof(GetTableSchema));
        }
    }

    public Task<string> GetViewDefinition(string database, string viewName, CancellationToken ct) =>
        GetDefinition(database, "VIEW DEFINITION", "VIEW", viewName, ct);

    public Task<string> GetProcedureDefinition(string database, string procName, CancellationToken ct) =>
        GetDefinition(database, "PROCEDURE DEFINITION", "PROCEDURE", procName, ct);

    public Task<string> GetFunctionDefinition(string database, string functionName, CancellationToken ct) =>
        GetDefinition(database, "FUNCTION DEFINITION", "FUNCTION", functionName, ct);

    public async Task<string> FindReferences(string database, string objectName, CancellationToken ct)
    {
        const string sql = """
            SELECT ROUTINE_SCHEMA, ROUTINE_NAME, ROUTINE_TYPE
            FROM information_schema.ROUTINES
            WHERE ROUTINE_SCHEMA = DATABASE() AND ROUTINE_DEFINITION LIKE CONCAT('%', @objectName, '%')
            ORDER BY ROUTINE_SCHEMA, ROUTINE_NAME
            """;
        var open = await OpenConnection(database, ct);
        if (open.Error is not null)
            return open.Error;
        await using var conn = open.Connection!;

        try
        {
            await using var cmd = new MySqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@objectName", objectName);
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            var sb = new StringBuilder();
            sb.AppendLine($"REFERENCES TO [{objectName}] in [{database}]");
            sb.AppendLine(new string('-', 60));
            int count = 0;
            while (await reader.ReadAsync(ct))
            {
                count++;
                sb.AppendLine($"  [{reader.GetString(0)}].[{reader.GetString(1)}]  ({reader.GetString(2)})");
            }

            if (count == 0)
                sb.AppendLine("  (no references found)");
            sb.AppendLine();
            sb.AppendLine($"  {count} reference(s)");
            return sb.ToString();
        }
        catch (Exception ex)
        {
            return SafeError(ex, nameof(FindReferences));
        }
    }

    public async Task<string> SearchDefinitions(string database, string keyword, CancellationToken ct)
    {
        const string sql = """
            SELECT ROUTINE_SCHEMA, ROUTINE_NAME, ROUTINE_TYPE
            FROM information_schema.ROUTINES
            WHERE ROUTINE_SCHEMA = DATABASE() AND ROUTINE_DEFINITION LIKE CONCAT('%', @keyword, '%')
            ORDER BY ROUTINE_TYPE, ROUTINE_SCHEMA, ROUTINE_NAME
            """;
        var open = await OpenConnection(database, ct);
        if (open.Error is not null)
            return open.Error;
        await using var conn = open.Connection!;

        try
        {
            await using var cmd = new MySqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@keyword", keyword);
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            var sb = new StringBuilder();
            sb.AppendLine($"SEARCH RESULTS for '{keyword}' in [{database}]");
            sb.AppendLine(new string('-', 60));
            int count = 0;
            while (await reader.ReadAsync(ct))
            {
                count++;
                sb.AppendLine($"  {reader.GetString(2),-20} [{reader.GetString(0)}].[{reader.GetString(1)}]");
            }

            if (count == 0)
                sb.AppendLine("  (no matches)");
            sb.AppendLine();
            sb.AppendLine($"  {count} match(es)");
            return sb.ToString();
        }
        catch (Exception ex)
        {
            return SafeError(ex, nameof(SearchDefinitions));
        }
    }

}
