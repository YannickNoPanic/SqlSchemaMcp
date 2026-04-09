namespace SqlSchemaMcp.Configuration;

public sealed class SqlServerOptions
{
    public Dictionary<string, string> Databases { get; init; } = new(StringComparer.OrdinalIgnoreCase);
}
