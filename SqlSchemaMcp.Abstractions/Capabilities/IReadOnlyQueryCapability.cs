namespace SqlSchemaMcp.Abstractions.Capabilities;

public interface IReadOnlyQueryCapability
{
    Task<string> ExecuteQuery(string database, string sql, CancellationToken ct);
}
