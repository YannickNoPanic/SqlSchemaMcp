namespace SqlSchemaMcp.MariaDb.Configuration;

public sealed class MariaDbEngineOptions
{
    public Dictionary<string, string> Databases { get; init; } = new(StringComparer.OrdinalIgnoreCase);
}
