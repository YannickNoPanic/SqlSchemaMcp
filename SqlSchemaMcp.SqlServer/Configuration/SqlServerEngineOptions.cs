namespace SqlSchemaMcp.SqlServer.Configuration;

public sealed class SqlServerEngineOptions
{
    public Dictionary<string, string> Databases { get; init; } = new(StringComparer.OrdinalIgnoreCase);
}
