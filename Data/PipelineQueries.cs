using SqlSchemaMcp.Abstractions;
using SqlSchemaMcp.Abstractions.Capabilities;

namespace SqlSchemaMcp.Data;

public sealed class PipelineQueries(ICapabilityResolver resolver)
{
    public Task<string> ListDataFeeds(string database, CancellationToken cancellationToken = default) =>
        Resolve(database, nameof(ListDataFeeds), capability => capability.ListDataFeeds(database, cancellationToken));

    public Task<string> AnalyzeStagingHealth(string database, CancellationToken cancellationToken = default) =>
        Resolve(database, nameof(AnalyzeStagingHealth), capability => capability.AnalyzeStagingHealth(database, cancellationToken));

    public Task<string> CompareStagingToCurrentSchema(
        string database,
        string feedBaseName,
        string currentTableName,
        CancellationToken cancellationToken = default) =>
        Resolve(database, nameof(CompareStagingToCurrentSchema), capability => capability.CompareStagingToCurrentSchema(database, feedBaseName, currentTableName, cancellationToken));

    private Task<string> Resolve(string database, string toolName, Func<ISqlServerPipelineCapability, Task<string>> execute)
    {
        if (resolver.TryResolve<ISqlServerPipelineCapability>(database, out _, out var capability) && capability is not null)
            return execute(capability);

        return Task.FromResult(
            resolver.TryGetEngine(database, out var engine)
                ? Sentinels.Unsupported(toolName, engine)
                : Sentinels.UnknownDatabase(resolver.DatabaseNames, database));
    }
}
