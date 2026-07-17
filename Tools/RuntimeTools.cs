using System.ComponentModel;
using ModelContextProtocol.Server;
using SqlSchemaMcp.Auditing;
using SqlSchemaMcp.Data;

namespace SqlSchemaMcp.Tools;

[McpServerToolType]
public sealed class RuntimeTools(RuntimeQueries queries, IAuditLog audit)
{
    [McpServerTool, Description("List configured databases with their engine and supported capability groups.")]
    public Task<string> ListConfiguredDatabases(CancellationToken cancellationToken = default) =>
        audit.Invoke(nameof(ListConfiguredDatabases), "", "", () => Task.FromResult(queries.ListConfiguredDatabases()));

    [McpServerTool, Description("Show which capability groups are supported by SQL Server, PostgreSQL, and MariaDB.")]
    public Task<string> ListEngineCapabilities(CancellationToken cancellationToken = default) =>
        audit.Invoke(nameof(ListEngineCapabilities), "", "", () => Task.FromResult(queries.ListEngineCapabilities()));

    [McpServerTool, Description("Check configuration shape and summarize runtime readiness without reading schema data.")]
    public Task<string> CheckConfiguration(CancellationToken cancellationToken = default) =>
        audit.Invoke(nameof(CheckConfiguration), "", "", () => Task.FromResult(queries.CheckConfiguration()));
}
