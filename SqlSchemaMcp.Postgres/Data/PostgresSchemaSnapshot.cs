using Npgsql;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SqlSchemaMcp.Abstractions;
using SqlSchemaMcp.Abstractions.Capabilities;
using SqlSchemaMcp.Postgres.Configuration;

namespace SqlSchemaMcp.Postgres.Data;

public sealed class PostgresSchemaSnapshot(IOptions<PostgresEngineOptions> options, ILogger<PostgresSchemaSnapshot> logger)
    : PostgresQueryBase(options, logger), ISchemaSnapshotCapability
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

    private static async Task<List<SchemaObject>> LoadObjects(NpgsqlConnection conn, CancellationToken ct)
    {
        const string sql = """
            SELECT 'TABLE', table_schema, table_name
            FROM information_schema.tables
            WHERE table_type = 'BASE TABLE' AND table_schema NOT IN ('pg_catalog', 'information_schema')
            UNION ALL
            SELECT 'VIEW', table_schema, table_name
            FROM information_schema.views
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
            UNION ALL
            SELECT CASE WHEN routine_type = 'PROCEDURE' THEN 'PROCEDURE' ELSE 'FUNCTION' END, routine_schema, routine_name
            FROM information_schema.routines
            WHERE routine_schema NOT IN ('pg_catalog', 'information_schema')
            ORDER BY 1, 2, 3
            """;
        var result = new List<SchemaObject>();
        await using var cmd = new NpgsqlCommand(sql, conn);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            result.Add(new SchemaObject(reader.GetString(0), reader.GetString(1), reader.GetString(2)));
        return result;
    }

    private static async Task<List<SchemaColumn>> LoadColumns(NpgsqlConnection conn, CancellationToken ct)
    {
        const string sql = """
            SELECT table_schema, table_name, column_name, data_type, character_maximum_length,
                   numeric_precision, numeric_scale, is_nullable
            FROM information_schema.columns
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
            ORDER BY table_schema, table_name, ordinal_position
            """;
        var result = new List<SchemaColumn>();
        await using var cmd = new NpgsqlCommand(sql, conn);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            string dataType = reader.GetString(3);
            result.Add(new SchemaColumn(
                reader.GetString(0),
                reader.GetString(1),
                reader.GetString(2),
                PostgresTypeMapper.ToCategory(dataType),
                PostgresTypeMapper.FormatColumnType(
                    dataType,
                    reader.IsDBNull(4) ? null : reader.GetInt32(4),
                    reader.IsDBNull(5) ? null : reader.GetInt32(5),
                    reader.IsDBNull(6) ? null : reader.GetInt32(6)),
                reader.GetString(7)));
        }

        return result;
    }

    private static async Task<HashSet<string>> LoadKeys(NpgsqlConnection conn, string sql, CancellationToken ct)
    {
        var result = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        await using var cmd = new NpgsqlCommand(sql, conn);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            result.Add($"{reader.GetString(0)}.{reader.GetString(1)}.{reader.GetString(2)}");
        return result;
    }

    private const string ForeignKeySql = """
        SELECT tc.table_schema, tc.table_name, kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON kcu.constraint_name = tc.constraint_name AND kcu.constraint_schema = tc.constraint_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
        """;

    private const string PrimaryKeySql = """
        SELECT tc.table_schema, tc.table_name, kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON kcu.constraint_name = tc.constraint_name AND kcu.constraint_schema = tc.constraint_schema
        WHERE tc.constraint_type = 'PRIMARY KEY'
        """;

    private const string IndexedColumnSql = """
        SELECT ns.nspname, t.relname, a.attname
        FROM pg_index i
        JOIN pg_class t ON t.oid = i.indrelid
        JOIN pg_namespace ns ON ns.oid = t.relnamespace
        JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(i.indkey)
        WHERE ns.nspname NOT IN ('pg_catalog', 'information_schema')
        """;
}
