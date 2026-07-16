using SqlSchemaMcp.Abstractions;
using SqlSchemaMcp.Abstractions.Capabilities;
using SqlSchemaMcp.SqlServer.Data;

namespace SqlSchemaMcp.SqlServer;

public sealed class SqlServerEngine(SqlServerQuery query) : IDatabaseEngine, IReadOnlyQueryCapability
{
    public DatabaseEngine Kind => DatabaseEngine.SqlServer;

    public Task<string> ExecuteQuery(string database, string sql, CancellationToken ct) =>
        query.ExecuteQuery(database, sql, ct);
}
