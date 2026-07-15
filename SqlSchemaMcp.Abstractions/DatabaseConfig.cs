namespace SqlSchemaMcp.Abstractions;

public sealed record DatabaseConfig(
    string Name,
    DatabaseEngine Engine,
    string ConnectionString);
