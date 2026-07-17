using SqlSchemaMcp.Abstractions;
using SqlSchemaMcp.Abstractions.Capabilities;

namespace SqlSchemaMcp.Data;

public sealed class SecurityQueries(ICapabilityResolver resolver)
{
    public Task<string> ListDatabaseUsers(string database, CancellationToken cancellationToken = default) =>
        Resolve(database, nameof(ListDatabaseUsers), capability => capability.ListDatabaseUsers(database, cancellationToken));

    public Task<string> ListObjectPermissions(string database, CancellationToken cancellationToken = default) =>
        Resolve(database, nameof(ListObjectPermissions), capability => capability.ListObjectPermissions(database, cancellationToken));

    private Task<string> Resolve(string database, string toolName, Func<ISqlServerSecurityCapability, Task<string>> execute)
    {
        if (resolver.TryResolve<ISqlServerSecurityCapability>(database, out _, out var capability) && capability is not null)
            return execute(capability);

        return Task.FromResult(
            resolver.TryGetEngine(database, out var engine)
                ? Sentinels.Unsupported(toolName, engine, nameof(ISqlServerSecurityCapability))
                : Sentinels.UnknownDatabase(resolver.DatabaseNames, database));
    }
}
