namespace SqlSchemaMcp.Abstractions.Capabilities;

public interface ISqlServerPipelineCapability
{
    Task<string> ListDataFeeds(string database, CancellationToken ct);
    Task<string> AnalyzeStagingHealth(string database, CancellationToken ct);
    Task<string> CompareStagingToCurrentSchema(string database, string feedBaseName, string currentTableName, CancellationToken ct);
}
