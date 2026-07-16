using SqlSchemaMcp.Abstractions;

namespace SqlSchemaMcp.Configuration;

public sealed class ConfiguredDatabases
{
    public ConfiguredDatabases(IReadOnlyList<DatabaseConfig> databases)
    {
        All = databases;
        SqlServerConnectionStrings = databases
            .Where(database => database.Engine == DatabaseEngine.SqlServer)
            .ToDictionary(database => database.Name, database => database.ConnectionString, StringComparer.OrdinalIgnoreCase);
        PostgresConnectionStrings = databases
            .Where(database => database.Engine == DatabaseEngine.Postgres)
            .ToDictionary(database => database.Name, database => database.ConnectionString, StringComparer.OrdinalIgnoreCase);
        MariaDbConnectionStrings = databases
            .Where(database => database.Engine == DatabaseEngine.MariaDb)
            .ToDictionary(database => database.Name, database => database.ConnectionString, StringComparer.OrdinalIgnoreCase);
    }

    public IReadOnlyList<DatabaseConfig> All { get; }

    public IReadOnlyDictionary<string, string> SqlServerConnectionStrings { get; }

    public IReadOnlyDictionary<string, string> PostgresConnectionStrings { get; }

    public IReadOnlyDictionary<string, string> MariaDbConnectionStrings { get; }
}
