using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MySqlConnector;
using SqlSchemaMcp.Abstractions;
using SqlSchemaMcp.Abstractions.Capabilities;
using SqlSchemaMcp.MariaDb.Configuration;

namespace SqlSchemaMcp.MariaDb.Data;

public sealed class MariaDbSchemaSnapshot(IOptions<MariaDbEngineOptions> options, ILogger<MariaDbSchemaSnapshot> logger)
    : MariaDbQueryBase(options, logger), ISchemaSnapshotCapability
{
    public async Task<SchemaSnapshot> GetSchemaSnapshot(string database, CancellationToken ct)
    {
        var open = await OpenConnection(database, ct);
        if (open.Error is not null)
            return EmptySnapshot();
        await using var conn = open.Connection!;

        var objects = await LoadObjects(conn, ct);
        var columns = await LoadColumns(conn, ct);
        var fkKeys = await LoadKeys(conn, ForeignKeySql, ct);
        var pkKeys = await LoadKeys(conn, PrimaryKeySql, ct);
        var indexedKeys = await LoadKeys(conn, IndexedColumnSql, ct);
        return new SchemaSnapshot(objects, columns, fkKeys, pkKeys, indexedKeys);
    }

    private static SchemaSnapshot EmptySnapshot() =>
        new([], [], new HashSet<string>(StringComparer.OrdinalIgnoreCase), new HashSet<string>(StringComparer.OrdinalIgnoreCase), new HashSet<string>(StringComparer.OrdinalIgnoreCase));

    private static async Task<List<SchemaObject>> LoadObjects(MySqlConnection conn, CancellationToken ct)
    {
        const string sql = """
            SELECT 'TABLE', TABLE_SCHEMA, TABLE_NAME
            FROM information_schema.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = DATABASE()
            UNION ALL
            SELECT 'VIEW', TABLE_SCHEMA, TABLE_NAME
            FROM information_schema.VIEWS
            WHERE TABLE_SCHEMA = DATABASE()
            UNION ALL
            SELECT ROUTINE_TYPE, ROUTINE_SCHEMA, ROUTINE_NAME
            FROM information_schema.ROUTINES
            WHERE ROUTINE_SCHEMA = DATABASE()
            ORDER BY 1, 2, 3
            """;
        var result = new List<SchemaObject>();
        await using var cmd = new MySqlCommand(sql, conn);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            result.Add(new SchemaObject(reader.GetString(0), reader.GetString(1), reader.GetString(2)));
        return result;
    }

    private static async Task<List<SchemaColumn>> LoadColumns(MySqlConnection conn, CancellationToken ct)
    {
        const string sql = """
            SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH,
                   NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
            ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION
            """;
        var result = new List<SchemaColumn>();
        await using var cmd = new MySqlCommand(sql, conn);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            string dataType = reader.GetString(3);
            result.Add(new SchemaColumn(
                reader.GetString(0),
                reader.GetString(1),
                reader.GetString(2),
                MariaDbTypeMapper.ToCategory(dataType),
                MariaDbTypeMapper.FormatColumnType(
                    dataType,
                    reader.IsDBNull(4) ? null : reader.GetInt64(4),
                    reader.IsDBNull(5) ? null : reader.GetInt64(5),
                    reader.IsDBNull(6) ? null : reader.GetInt64(6)),
                reader.GetString(7)));
        }

        return result;
    }

    private static async Task<HashSet<string>> LoadKeys(MySqlConnection conn, string sql, CancellationToken ct)
    {
        var result = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        await using var cmd = new MySqlCommand(sql, conn);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            result.Add($"{reader.GetString(0)}.{reader.GetString(1)}.{reader.GetString(2)}");
        return result;
    }

    private const string ForeignKeySql = """
        SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME
        FROM information_schema.KEY_COLUMN_USAGE
        WHERE TABLE_SCHEMA = DATABASE() AND REFERENCED_TABLE_NAME IS NOT NULL
        """;

    private const string PrimaryKeySql = """
        SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME
        FROM information_schema.KEY_COLUMN_USAGE
        WHERE TABLE_SCHEMA = DATABASE() AND CONSTRAINT_NAME = 'PRIMARY'
        """;

    private const string IndexedColumnSql = """
        SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME
        FROM information_schema.STATISTICS
        WHERE TABLE_SCHEMA = DATABASE()
        """;
}
