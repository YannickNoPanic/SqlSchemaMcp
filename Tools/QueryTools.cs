using System.ComponentModel;
using ModelContextProtocol.Server;
using SqlSchemaMcp.Auditing;
using SqlSchemaMcp.Data;

namespace SqlSchemaMcp.Tools;

[McpServerToolType]
public sealed class QueryTools(QueryQueries queries, IAuditLog audit)
{
    [McpServerTool, Description("Execute a read-only SELECT query against a configured database. Returns results as an ASCII table (max 500 rows, 30-second timeout). Write operations (INSERT, UPDATE, DELETE, DROP, TRUNCATE, ALTER, CREATE, EXEC, MERGE, GRANT, REVOKE, DENY) are blocked.")]
    public Task<string> ExecuteQuery(
        [Description("Name of the configured database")] string database,
        [Description("SELECT statement or CTE (WITH ...) to execute. Write keywords are not permitted.")] string sql,
        CancellationToken cancellationToken = default) =>
        audit.Invoke(nameof(ExecuteQuery), database, $"sql={AuditSummary.Truncate(sql)}",
            () => queries.ExecuteQuery(database, sql, cancellationToken));
}
