using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SqlSchemaMcp.Abstractions;
using SqlSchemaMcp.Abstractions.Capabilities;
using SqlSchemaMcp.SqlServer.Configuration;

namespace SqlSchemaMcp.SqlServer.Data;

public sealed class SqlServerSchemaSnapshot(IOptions<SqlServerEngineOptions> options, ILogger<SqlServerSchemaSnapshot> logger)
    : SqlQueryBase(options, logger), ISchemaSnapshotCapability
{
    public async Task<SchemaSnapshot> GetSchemaSnapshot(string database, CancellationToken ct)
    {
        if (!_databases.TryGetValue(database, out var connectionString))
            return EmptySnapshot();

        try
        {
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(ct);

            var objects = await LoadObjects(conn, ct);
            var columns = await LoadColumns(conn, ct);
            var fkKeys = await LoadKeys(conn, ForeignKeySql, ct);
            var pkKeys = await LoadKeys(conn, PrimaryKeySql, ct);
            var indexedKeys = await LoadKeys(conn, IndexedColumnSql, ct);

            return new SchemaSnapshot(objects, columns, fkKeys, pkKeys, indexedKeys);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Schema snapshot operation failed");
            return EmptySnapshot();
        }
    }

    private static SchemaSnapshot EmptySnapshot() =>
        new([], [], new HashSet<string>(StringComparer.OrdinalIgnoreCase), new HashSet<string>(StringComparer.OrdinalIgnoreCase), new HashSet<string>(StringComparer.OrdinalIgnoreCase));

    private static async Task<List<SchemaObject>> LoadObjects(SqlConnection conn, CancellationToken ct)
    {
        const string sql = """
            SELECT 'TABLE' AS ObjectType, TABLE_SCHEMA AS SchemaName, TABLE_NAME AS ObjectName
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE'
            UNION ALL
            SELECT 'VIEW', TABLE_SCHEMA, TABLE_NAME
            FROM INFORMATION_SCHEMA.VIEWS
            UNION ALL
            SELECT 'PROCEDURE', ROUTINE_SCHEMA, ROUTINE_NAME
            FROM INFORMATION_SCHEMA.ROUTINES
            WHERE ROUTINE_TYPE = 'PROCEDURE'
            ORDER BY ObjectType, SchemaName, ObjectName
            """;

        var result = new List<SchemaObject>();
        await using var cmd = new SqlCommand(sql, conn);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            result.Add(new SchemaObject(reader.GetString(0), reader.GetString(1), reader.GetString(2)));

        return result;
    }

    private static async Task<List<SchemaColumn>> LoadColumns(SqlConnection conn, CancellationToken ct)
    {
        const string sql = """
            SELECT
                c.TABLE_SCHEMA,
                c.TABLE_NAME,
                c.COLUMN_NAME,
                c.DATA_TYPE,
                c.CHARACTER_MAXIMUM_LENGTH,
                c.NUMERIC_PRECISION,
                c.NUMERIC_SCALE,
                c.IS_NULLABLE
            FROM INFORMATION_SCHEMA.COLUMNS c
            JOIN INFORMATION_SCHEMA.TABLES t
                ON t.TABLE_NAME = c.TABLE_NAME AND t.TABLE_SCHEMA = c.TABLE_SCHEMA
            WHERE t.TABLE_TYPE = 'BASE TABLE'
            ORDER BY c.TABLE_SCHEMA, c.TABLE_NAME, c.ORDINAL_POSITION
            """;

        var result = new List<SchemaColumn>();
        await using var cmd = new SqlCommand(sql, conn);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            string dataType = reader.GetString(3);
            int? maxLength = reader.IsDBNull(4) ? null : reader.GetInt32(4);
            int? precision = reader.IsDBNull(5) ? null : Convert.ToInt32(reader.GetValue(5));
            int? scale = reader.IsDBNull(6) ? null : Convert.ToInt32(reader.GetValue(6));

            result.Add(new SchemaColumn(
                reader.GetString(0),
                reader.GetString(1),
                reader.GetString(2),
                ToCategory(dataType),
                FormatColumnType(dataType, maxLength, precision, scale),
                reader.GetString(7)));
        }

        return result;
    }

    private static async Task<HashSet<string>> LoadKeys(SqlConnection conn, string sql, CancellationToken ct)
    {
        var result = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        await using var cmd = new SqlCommand(sql, conn);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            result.Add($"{reader.GetString(0)}.{reader.GetString(1)}.{reader.GetString(2)}");

        return result;
    }

    private static ColumnTypeCategory ToCategory(string dataType) =>
        dataType.ToLowerInvariant() switch
        {
            "int" or "bigint" or "smallint" or "tinyint" => ColumnTypeCategory.Integer,
            "uniqueidentifier" => ColumnTypeCategory.Guid,
            "nvarchar" or "varchar" or "nchar" or "char" or "text" or "ntext" => ColumnTypeCategory.Text,
            "bit" => ColumnTypeCategory.Boolean,
            "date" or "datetime" or "datetime2" or "datetimeoffset" or "smalldatetime" or "time" => ColumnTypeCategory.Temporal,
            "decimal" or "numeric" or "money" or "smallmoney" or "float" or "real" => ColumnTypeCategory.Decimal,
            _ => ColumnTypeCategory.Other
        };

    private const string ForeignKeySql = """
        SELECT
            OBJECT_SCHEMA_NAME(fk.parent_object_id) AS TableSchema,
            OBJECT_NAME(fk.parent_object_id) AS TableName,
            COL_NAME(fkc.parent_object_id, fkc.parent_column_id) AS ColumnName
        FROM sys.foreign_keys fk
        JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id
        """;

    private const string PrimaryKeySql = """
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

    private const string IndexedColumnSql = """
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
}
