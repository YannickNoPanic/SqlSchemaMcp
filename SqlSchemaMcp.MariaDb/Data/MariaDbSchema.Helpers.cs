using System.Text;
using MySqlConnector;

namespace SqlSchemaMcp.MariaDb.Data;

public sealed partial class MariaDbSchema
{
    private async Task<string> ListObjects(string database, string title, string tableType, string? schemaFilter, string? nameFilter, CancellationToken ct)
    {
        const string sql = """
            SELECT TABLE_SCHEMA, TABLE_NAME
            FROM information_schema.TABLES
            WHERE TABLE_TYPE = @tableType
                AND TABLE_SCHEMA = COALESCE(@schemaFilter, DATABASE())
                AND (@nameFilter IS NULL OR TABLE_NAME LIKE CONCAT('%', @nameFilter, '%'))
            ORDER BY TABLE_SCHEMA, TABLE_NAME
            """;
        var open = await OpenConnection(database, ct);
        if (open.Error is not null)
            return open.Error;
        await using var conn = open.Connection!;

        try
        {
            await using var cmd = new MySqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@tableType", tableType);
            cmd.Parameters.Add("@schemaFilter", MySqlDbType.VarChar).Value = (object?)schemaFilter ?? DBNull.Value;
            cmd.Parameters.Add("@nameFilter", MySqlDbType.VarChar).Value = (object?)nameFilter ?? DBNull.Value;
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
            SELECT ROUTINE_SCHEMA, ROUTINE_NAME
            FROM information_schema.ROUTINES
            WHERE ROUTINE_TYPE = @routineType
                AND ROUTINE_SCHEMA = DATABASE()
                AND (@nameFilter IS NULL OR ROUTINE_NAME LIKE CONCAT('%', @nameFilter, '%'))
            ORDER BY ROUTINE_SCHEMA, ROUTINE_NAME
            """;
        var open = await OpenConnection(database, ct);
        if (open.Error is not null)
            return open.Error;
        await using var conn = open.Connection!;

        try
        {
            await using var cmd = new MySqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@routineType", routineType);
            cmd.Parameters.Add("@nameFilter", MySqlDbType.VarChar).Value = (object?)nameFilter ?? DBNull.Value;
            return await ReadNameList(database, title, cmd, ct);
        }
        catch (Exception ex)
        {
            return SafeError(ex, title);
        }
    }

    private static async Task<string> ReadNameList(string database, string title, MySqlCommand cmd, CancellationToken ct)
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

    private async Task<string> GetDefinition(string database, string title, string routineType, string objectName, CancellationToken ct)
    {
        var (schema, name) = ParseSchemaObject(objectName);
        string sql = routineType == "VIEW"
            ? """
                SELECT VIEW_DEFINITION
                FROM information_schema.VIEWS
                WHERE TABLE_SCHEMA = COALESCE(@schema, DATABASE()) AND TABLE_NAME = @name
                """
            : """
                SELECT ROUTINE_DEFINITION
                FROM information_schema.ROUTINES
                WHERE ROUTINE_SCHEMA = COALESCE(@schema, DATABASE()) AND ROUTINE_NAME = @name AND ROUTINE_TYPE = @routineType
                """;
        var open = await OpenConnection(database, ct);
        if (open.Error is not null)
            return open.Error;
        await using var conn = open.Connection!;

        try
        {
            await using var cmd = new MySqlCommand(sql, conn);
            cmd.Parameters.Add("@schema", MySqlDbType.VarChar).Value = (object?)schema ?? DBNull.Value;
            cmd.Parameters.AddWithValue("@name", name);
            cmd.Parameters.AddWithValue("@routineType", routineType);
            var result = await cmd.ExecuteScalarAsync(ct);
            return result is null or DBNull
                ? $"ERROR: {routineType} '{objectName}' not found in [{database}]."
                : $"{title}: [{schema ?? "(current)"}].[{name}] in [{database}]\n{new string('-', 60)}\n{result}";
        }
        catch (Exception ex)
        {
            return SafeError(ex, title);
        }
    }

    private static async Task AppendForeignKeys(MySqlConnection conn, StringBuilder sb, string sql, string? schema, string table, CancellationToken ct)
    {
        sb.AppendLine();
        sb.AppendLine("FOREIGN KEYS");
        sb.AppendLine(new string('-', 90));
        await using var cmd = new MySqlCommand(sql, conn);
        cmd.Parameters.Add("@schema", MySqlDbType.VarChar).Value = (object?)schema ?? DBNull.Value;
        cmd.Parameters.AddWithValue("@table", table);
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

    private static async Task AppendIndexes(MySqlConnection conn, StringBuilder sb, string sql, string? schema, string table, CancellationToken ct)
    {
        sb.AppendLine();
        sb.AppendLine("INDEXES");
        sb.AppendLine(new string('-', 90));
        await using var cmd = new MySqlCommand(sql, conn);
        cmd.Parameters.Add("@schema", MySqlDbType.VarChar).Value = (object?)schema ?? DBNull.Value;
        cmd.Parameters.AddWithValue("@table", table);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        bool any = false;
        while (await reader.ReadAsync(ct))
        {
            any = true;
            sb.AppendLine($"  [{reader.GetString(0)}]  ({reader.GetString(1)})");
        }

        if (!any)
            sb.AppendLine("  (none)");
    }

    private static (string? Schema, string Name) ParseSchemaObject(string objectName)
    {
        var trimmed = objectName.Trim().Trim('`');
        if (!trimmed.Contains('.'))
            return (null, trimmed);

        var parts = trimmed.Split('.', 2);
        return (parts[0].Trim('`'), parts[1].Trim('`'));
    }
}
