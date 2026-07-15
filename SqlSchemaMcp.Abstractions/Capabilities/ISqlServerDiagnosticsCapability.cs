namespace SqlSchemaMcp.Abstractions.Capabilities;

public interface ISqlServerDiagnosticsCapability
{
    Task<string> ListAgentJobs(string database, CancellationToken ct);
    Task<string> GetFailingJobs(string database, CancellationToken ct);
    Task<string> GetJobHistory(string database, string jobName, int maxRuns, CancellationToken ct);
    Task<string> AnalyzeTopExpensiveQueries(string database, int top, CancellationToken ct);
    Task<string> AnalyzeWaitStats(string database, CancellationToken ct);
    Task<string> ListLinkedServers(string database, CancellationToken ct);
    Task<string> FindLinkedServerUsage(string database, string? linkedServerName, CancellationToken ct);
    Task<string> ListServiceBroker(string database, CancellationToken ct);
    Task<string> ListClrAssemblies(string database, CancellationToken ct);
}
