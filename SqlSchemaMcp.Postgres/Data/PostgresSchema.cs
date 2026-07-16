using System.Text;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;
using SqlSchemaMcp.Abstractions.Capabilities;
using SqlSchemaMcp.Postgres.Configuration;

namespace SqlSchemaMcp.Postgres.Data;

public sealed partial class PostgresSchema(IOptions<PostgresEngineOptions> options, ILogger<PostgresSchema> logger)
    : PostgresQueryBase(options, logger), ISchemaCapability
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
            SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_scale,
                   is_nullable, column_default
            FROM information_schema.columns
            WHERE table_schema = @schema AND table_name = @table
            ORDER BY ordinal_position
            """;
        const string fkSql = """
            SELECT tc.constraint_name, kcu.column_name, ccu.table_schema, ccu.table_name, ccu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON kcu.constraint_name = tc.constraint_name AND kcu.constraint_schema = tc.constraint_schema
            JOIN information_schema.constraint_column_usage ccu
                ON ccu.constraint_name = tc.constraint_name AND ccu.constraint_schema = tc.constraint_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
                AND tc.table_schema = @schema AND tc.table_name = @table
            ORDER BY tc.constraint_name
            """;
        const string indexSql = """
            SELECT indexname, indexdef
            FROM pg_indexes
            WHERE schemaname = @schema AND tablename = @table
            ORDER BY indexname
            """;

        var open = await OpenConnection(database, ct);
        if (open.Error is not null)
            return open.Error;
        await using var conn = open.Connection!;

        try
        {
            var sb = new StringBuilder();
            sb.AppendLine($"TABLE: [{schema}].[{table}]");
            sb.AppendLine(new string('-', 90));
            sb.AppendLine($"{"Column",-35} {"Type",-25} {"Null",-6} {"Default"}");
            sb.AppendLine(new string('-', 90));

            await using (var cmd = new NpgsqlCommand(columnSql, conn))
            {
                cmd.Parameters.AddWithValue("schema", schema);
                cmd.Parameters.AddWithValue("table", table);
                await using var reader = await cmd.ExecuteReaderAsync(ct);
                bool any = false;
                while (await reader.ReadAsync(ct))
                {
                    any = true;
                    string type = PostgresTypeMapper.FormatColumnType(
                        reader.GetString(1),
                        reader.IsDBNull(2) ? null : reader.GetInt32(2),
                        reader.IsDBNull(3) ? null : reader.GetInt32(3),
                        reader.IsDBNull(4) ? null : reader.GetInt32(4));
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
        GetRoutineOrViewDefinition(database, "VIEW DEFINITION", "VIEW", viewName, "pg_get_viewdef(c.oid, true)", "c.relkind = 'v'", ct);

    public Task<string> GetProcedureDefinition(string database, string procName, CancellationToken ct) =>
        GetRoutineOrViewDefinition(database, "PROCEDURE DEFINITION", "PROCEDURE", procName, "pg_get_functiondef(p.oid)", "p.prokind = 'p'", ct);

    public Task<string> GetFunctionDefinition(string database, string functionName, CancellationToken ct) =>
        GetRoutineOrViewDefinition(database, "FUNCTION DEFINITION", "FUNCTION", functionName, "pg_get_functiondef(p.oid)", "p.prokind = 'f'", ct);

    public async Task<string> FindReferences(string database, string objectName, CancellationToken ct)
    {
        const string sql = """
            SELECT n.nspname, c.relname, c.relkind::text
            FROM pg_rewrite r
            JOIN pg_class c ON c.oid = r.ev_class
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE pg_get_viewdef(c.oid, true) ILIKE @pattern
            ORDER BY n.nspname, c.relname
            """;

        var open = await OpenConnection(database, ct);
        if (open.Error is not null)
            return open.Error;
        await using var conn = open.Connection!;

        try
        {
            await using var cmd = new NpgsqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("pattern", $"%{objectName}%");
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
            SELECT routine_schema, routine_name, routine_type
            FROM information_schema.routines
            WHERE routine_schema NOT IN ('pg_catalog', 'information_schema')
                AND routine_definition ILIKE @pattern
            ORDER BY routine_type, routine_schema, routine_name
            """;

        var open = await OpenConnection(database, ct);
        if (open.Error is not null)
            return open.Error;
        await using var conn = open.Connection!;

        try
        {
            await using var cmd = new NpgsqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("pattern", $"%{keyword}%");
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
