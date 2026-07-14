using System.ComponentModel;
using ModelContextProtocol.Server;
using SqlSchemaMcp.Auditing;
using SqlSchemaMcp.Data;

namespace SqlSchemaMcp.Tools;

[McpServerToolType]
public sealed class SecurityTools(SecurityQueries queries, IAuditLog audit)
{
    [McpServerTool, Description("List all database users and their assigned roles. Excludes built-in accounts (dbo, guest, sys, INFORMATION_SCHEMA).")]
    public Task<string> ListDatabaseUsers(
        [Description("Name of the configured database")] string database,
        CancellationToken cancellationToken = default) =>
        audit.Invoke(nameof(ListDatabaseUsers), database, "",
            () => queries.ListDatabaseUsers(database, cancellationToken));

    [McpServerTool, Description("List all explicit object-level permissions (GRANT/DENY) on tables, views, and procedures. Excludes the dbo and public principals.")]
    public Task<string> ListObjectPermissions(
        [Description("Name of the configured database")] string database,
        CancellationToken cancellationToken = default) =>
        audit.Invoke(nameof(ListObjectPermissions), database, "",
            () => queries.ListObjectPermissions(database, cancellationToken));
}
