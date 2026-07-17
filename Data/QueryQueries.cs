using SqlSchemaMcp.Abstractions;
using SqlSchemaMcp.Abstractions.Capabilities;

namespace SqlSchemaMcp.Data;

public sealed class QueryQueries(ICapabilityResolver resolver)
{
    public Task<string> ExecuteQuery(
        string database,
        string sql,
        CancellationToken cancellationToken = default)
    {
        if (resolver.TryResolve<IReadOnlyQueryCapability>(database, out _, out var capability) && capability is not null)
            return capability.ExecuteQuery(database, sql, cancellationToken);

        return Task.FromResult(
            resolver.TryGetEngine(database, out var engine)
                ? Sentinels.Unsupported(nameof(ExecuteQuery), engine, nameof(IReadOnlyQueryCapability))
                : Sentinels.UnknownDatabase(resolver.DatabaseNames, database));
    }
}
