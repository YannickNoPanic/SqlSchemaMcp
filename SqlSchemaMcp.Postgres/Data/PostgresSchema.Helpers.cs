using System.Text;
using Npgsql;

namespace SqlSchemaMcp.Postgres.Data;

public sealed partial class PostgresSchema
{
    private async Task<string> ListObjects(string database, string title, string tableType, string? schemaFilter, string? nameFilter, CancellationToken ct)
    {
        const string sql = """
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_type = @tableType
                AND table_schema NOT IN ('pg_catalog', 'information_schema')
                AND (@schemaFilter IS NULL OR table_schema = @schemaFilter)
                AND (@nameFilter IS NULL OR table_name ILIKE '%' || @nameFilter || '%')
            ORDER BY table_schema, table_name
            """;

        var open = await OpenConnection(database, ct);
        if (open.Error is not null)
            return open.Error;
        await using var conn = open.Connection!;

        try
        {
            await using var cmd = new NpgsqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("tableType", tableType);
            cmd.Parameters.AddWithValue("schemaFilter", (object?)schemaFilter ?? DBNull.Value);
            cmd.Parameters.AddWithValue("nameFilter", (object?)nameFilter ?? DBNull.Value);
            return await ReadNameList(database, title, cmd, ct);
        }
        catch (Exception ex)
        {
            return SafeError(ex, title);
        }
    }

    private async Task<string> ListRoutines(string database, string title, string routineType, string? nameFilter, CancellationToken ct)
    {
        const string sql = """
            SELECT routine_schema, routine_name
            FROM information_schema.routines
            WHERE routine_type = @routineType
                AND routine_schema NOT IN ('pg_catalog', 'information_schema')
                AND (@nameFilter IS NULL OR routine_name ILIKE '%' || @nameFilter || '%')
            ORDER BY routine_schema, routine_name
            """;

        var open = await OpenConnection(database, ct);
        if (open.Error is not null)
            return open.Error;
        await using var conn = open.Connection!;

        try
        {
            await using var cmd = new NpgsqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("routineType", routineType);
            cmd.Parameters.AddWithValue("nameFilter", (object?)nameFilter ?? DBNull.Value);
            return await ReadNameList(database, title, cmd, ct);
        }
        catch (Exception ex)
        {
            return SafeError(ex, title);
        }
    }

    private static async Task<string> ReadNameList(string database, string title, NpgsqlCommand cmd, CancellationToken ct)
    {
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        var sb = new StringBuilder();
        sb.AppendLine($"{title} in [{database}]");
        sb.AppendLine(new string('-', 60));
        int count = 0;
        while (await reader.ReadAsync(ct))
        {
            count++;
            sb.AppendLine($"  [{reader.GetString(0)}].[{reader.GetString(1)}]");
        }

        if (count == 0)
            sb.AppendLine("  (none found)");
        sb.AppendLine();
        sb.AppendLine($"  {count} item(s)");
        return sb.ToString();
    }

    private async Task<string> GetRoutineOrViewDefinition(string database, string title, string objectType, string objectName, string definitionExpression, string predicate, CancellationToken ct)
    {
        var (schema, name) = ParseSchemaObject(objectName);
        string sql = objectType == "VIEW"
            ? $"""
                SELECT {definitionExpression}
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE {predicate} AND n.nspname = @schema AND c.relname = @name
                """
            : $"""
                SELECT {definitionExpression}
                FROM pg_proc p
                JOIN pg_namespace n ON n.oid = p.pronamespace
                WHERE {predicate} AND n.nspname = @schema AND p.proname = @name
                """;

        var open = await OpenConnection(database, ct);
        if (open.Error is not null)
            return open.Error;
        await using var conn = open.Connection!;

        try
        {
            await using var cmd = new NpgsqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("schema", schema);
            cmd.Parameters.AddWithValue("name", name);
            var result = await cmd.ExecuteScalarAsync(ct);
            return result is null or DBNull
                ? $"ERROR: {objectType} '{objectName}' not found in [{database}]."
                : $"{title}: [{schema}].[{name}] in [{database}]\n{new string('-', 60)}\n{result}";
        }
        catch (Exception ex)
        {
            return SafeError(ex, title);
        }
    }

    private static async Task AppendForeignKeys(NpgsqlConnection conn, StringBuilder sb, string sql, string schema, string table, CancellationToken ct)
    {
        sb.AppendLine();
        sb.AppendLine("FOREIGN KEYS");
        sb.AppendLine(new string('-', 90));
        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("schema", schema);
        cmd.Parameters.AddWithValue("table", table);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        bool any = false;
        while (await reader.ReadAsync(ct))
        {
            any = true;
            sb.AppendLine($"  [{reader.GetString(0)}]");
            sb.AppendLine($"    {reader.GetString(1)} -> [{reader.GetString(2)}].[{reader.GetString(3)}]({reader.GetString(4)})");
        }

        if (!any)
            sb.AppendLine("  (none)");
    }

    private static async Task AppendIndexes(NpgsqlConnection conn, StringBuilder sb, string sql, string schema, string table, CancellationToken ct)
    {
        sb.AppendLine();
        sb.AppendLine("INDEXES");
        sb.AppendLine(new string('-', 90));
        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("schema", schema);
        cmd.Parameters.AddWithValue("table", table);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        bool any = false;
        while (await reader.ReadAsync(ct))
        {
            any = true;
            sb.AppendLine($"  [{reader.GetString(0)}]  {reader.GetString(1)}");
        }

        if (!any)
            sb.AppendLine("  (none)");
    }

    private static (string Schema, string Name) ParseSchemaObject(string objectName)
    {
        var trimmed = objectName.Trim().Trim('"');
        if (!trimmed.Contains('.'))
            return ("public", trimmed);

        var parts = trimmed.Split('.', 2);
        return (parts[0].Trim('"'), parts[1].Trim('"'));
    }
}
