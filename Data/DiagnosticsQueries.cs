using SqlSchemaMcp.Abstractions;
using SqlSchemaMcp.Abstractions.Capabilities;

namespace SqlSchemaMcp.Data;

public sealed class DiagnosticsQueries(ICapabilityResolver resolver)
{
    public Task<string> ListAgentJobs(string database, CancellationToken cancellationToken = default) =>
        Resolve(database, nameof(ListAgentJobs), capability => capability.ListAgentJobs(database, cancellationToken));

    public Task<string> GetFailingJobs(string database, CancellationToken cancellationToken = default) =>
        Resolve(database, nameof(GetFailingJobs), capability => capability.GetFailingJobs(database, cancellationToken));

    public Task<string> GetJobHistory(string database, string jobName, int lastN = 20, CancellationToken cancellationToken = default) =>
        Resolve(database, nameof(GetJobHistory), capability => capability.GetJobHistory(database, jobName, lastN, cancellationToken));

    public Task<string> ListLinkedServers(string database, CancellationToken cancellationToken = default) =>
        Resolve(database, nameof(ListLinkedServers), capability => capability.ListLinkedServers(database, cancellationToken));

    public Task<string> FindLinkedServerUsage(string database, string? linkedServerName = null, CancellationToken cancellationToken = default) =>
        Resolve(database, nameof(FindLinkedServerUsage), capability => capability.FindLinkedServerUsage(database, linkedServerName, cancellationToken));

    public Task<string> ListServiceBroker(string database, CancellationToken cancellationToken = default) =>
        Resolve(database, nameof(ListServiceBroker), capability => capability.ListServiceBroker(database, cancellationToken));

    public Task<string> ListClrAssemblies(string database, CancellationToken cancellationToken = default) =>
        Resolve(database, nameof(ListClrAssemblies), capability => capability.ListClrAssemblies(database, cancellationToken));

    public Task<string> AnalyzeTopExpensiveQueries(string database, int top, CancellationToken cancellationToken = default) =>
        Resolve(database, nameof(AnalyzeTopExpensiveQueries), capability => capability.AnalyzeTopExpensiveQueries(database, top, cancellationToken));

    public Task<string> AnalyzeWaitStats(string database, CancellationToken cancellationToken = default) =>
        Resolve(database, nameof(AnalyzeWaitStats), capability => capability.AnalyzeWaitStats(database, cancellationToken));

    private Task<string> Resolve(string database, string toolName, Func<ISqlServerDiagnosticsCapability, Task<string>> execute)
    {
        if (resolver.TryResolve<ISqlServerDiagnosticsCapability>(database, out _, out var capability) && capability is not null)
            return execute(capability);

        return Task.FromResult(
            resolver.TryGetEngine(database, out var engine)
                ? Sentinels.Unsupported(toolName, engine, nameof(ISqlServerDiagnosticsCapability))
                : Sentinels.UnknownDatabase(resolver.DatabaseNames, database));
    }
}
