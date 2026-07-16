namespace SqlSchemaMcp.Postgres.Configuration;

public sealed class PostgresEngineOptions
{
    public Dictionary<string, string> Databases { get; init; } = new(StringComparer.OrdinalIgnoreCase);
}
