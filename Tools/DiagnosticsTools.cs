using System.ComponentModel;
using ModelContextProtocol.Server;
using SqlSchemaMcp.Data;

namespace SqlSchemaMcp.Tools;

[McpServerToolType]
public sealed class DiagnosticsTools(DiagnosticsQueries queries)
{
    [McpServerTool, Description("List all SQL Agent jobs on the server with their enabled status and last run outcome. Uses the server of the specified configured database to connect to msdb.")]
    public async Task<string> ListAgentJobs(
        [Description("Name of the configured database (used to resolve the server connection)")] string database,
        CancellationToken cancellationToken = default) =>
        await queries.ListAgentJobs(database, cancellationToken);

    [McpServerTool, Description("List all SQL Agent jobs that have failed in the last 7 days, with error messages. Uses the server of the specified configured database to connect to msdb.")]
    public async Task<string> GetFailingJobs(
        [Description("Name of the configured database (used to resolve the server connection)")] string database,
        CancellationToken cancellationToken = default) =>
        await queries.GetFailingJobs(database, cancellationToken);

    [McpServerTool, Description("Get step-by-step run history for a specific SQL Agent job, including error messages for failed steps. Uses the server of the specified configured database to connect to msdb.")]
    public async Task<string> GetJobHistory(
        [Description("Name of the configured database (used to resolve the server connection)")] string database,
        [Description("Exact name of the SQL Agent job")] string jobName,
        [Description("Number of most recent history entries to return (default 20)")] int lastN = 20,
        CancellationToken cancellationToken = default) =>
        await queries.GetJobHistory(database, jobName, lastN, cancellationToken);

    [McpServerTool, Description("Return the top N most expensive queries by total CPU time from sys.dm_exec_query_stats, filtered to this database. Requires VIEW SERVER STATE permission.")]
    public async Task<string> AnalyzeTopExpensiveQueries(
        [Description("Name of the configured database")] string database,
        [Description("Number of queries to return (default 20)")] int top = 20,
        CancellationToken cancellationToken = default) =>
        await queries.AnalyzeTopExpensiveQueries(database, top, cancellationToken);

    [McpServerTool, Description("Return the top wait types from sys.dm_os_wait_stats, excluding benign idle waits. Useful for server-level performance diagnosis. Requires VIEW SERVER STATE permission.")]
    public async Task<string> AnalyzeWaitStats(
        [Description("Name of the configured database (used to resolve the server connection)")] string database,
        CancellationToken cancellationToken = default) =>
        await queries.AnalyzeWaitStats(database, cancellationToken);

    [McpServerTool, Description("List all linked servers configured on the SQL Server instance, including provider, data source, and login settings.")]
    public async Task<string> ListLinkedServers(
        [Description("Name of the configured database (used to resolve the server connection)")] string database,
        CancellationToken cancellationToken = default) =>
        await queries.ListLinkedServers(database, cancellationToken);

    [McpServerTool, Description("Find stored procedures and views that contain linked server calls (four-part names). Optionally filter to a specific linked server.")]
    public async Task<string> FindLinkedServerUsage(
        [Description("Name of the configured database")] string database,
        [Description("Linked server name to search for (optional — omit to find all four-part name references)")] string? linkedServerName = null,
        CancellationToken cancellationToken = default) =>
        await queries.FindLinkedServerUsage(database, linkedServerName, cancellationToken);

    [McpServerTool, Description("List all user-defined Service Broker queues and services in the specified database.")]
    public async Task<string> ListServiceBroker(
        [Description("Name of the configured database")] string database,
        CancellationToken cancellationToken = default) =>
        await queries.ListServiceBroker(database, cancellationToken);

    [McpServerTool, Description("List all user-defined CLR assemblies registered in the specified database.")]
    public async Task<string> ListClrAssemblies(
        [Description("Name of the configured database")] string database,
        CancellationToken cancellationToken = default) =>
        await queries.ListClrAssemblies(database, cancellationToken);
}
