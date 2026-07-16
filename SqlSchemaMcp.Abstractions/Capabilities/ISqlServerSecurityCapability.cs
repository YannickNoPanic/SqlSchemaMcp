namespace SqlSchemaMcp.Abstractions.Capabilities;

public interface ISqlServerSecurityCapability
{
    Task<string> ListDatabaseUsers(string database, CancellationToken ct);
    Task<string> ListObjectPermissions(string database, CancellationToken ct);
}
