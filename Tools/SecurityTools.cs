using System.ComponentModel;
using ModelContextProtocol.Server;
using SqlSchemaMcp.Data;

namespace SqlSchemaMcp.Tools;

[McpServerToolType]
public sealed class SecurityTools(SecurityQueries queries)
{
    [McpServerTool, Description("List all database users and their assigned roles. Excludes built-in accounts (dbo, guest, sys, INFORMATION_SCHEMA).")]
    public async Task<string> ListDatabaseUsers(
        [Description("Name of the configured database")] string database,
        CancellationToken cancellationToken = default) =>
        await queries.ListDatabaseUsers(database, cancellationToken);

    [McpServerTool, Description("List all explicit object-level permissions (GRANT/DENY) on tables, views, and procedures. Excludes the dbo and public principals.")]
    public async Task<string> ListObjectPermissions(
        [Description("Name of the configured database")] string database,
        CancellationToken cancellationToken = default) =>
        await queries.ListObjectPermissions(database, cancellationToken);
}
