using Microsoft.Extensions.Configuration;
using SqlSchemaMcp.Abstractions;

namespace SqlSchemaMcp.Configuration;

public static class DatabaseConfigLoader
{
    public static IReadOnlyList<DatabaseConfig> Load(IConfiguration configuration)
    {
        var section = configuration.GetSection("SqlServer:Databases");
        var databases = new List<DatabaseConfig>();

        foreach (var child in section.GetChildren())
        {
            var bareValue = child.Value;
            if (!string.IsNullOrWhiteSpace(bareValue))
            {
                databases.Add(new DatabaseConfig(child.Key, DatabaseEngine.SqlServer, bareValue));
                continue;
            }

            var engineValue = child["Engine"];
            var connectionString = child["ConnectionString"];

            if (string.IsNullOrWhiteSpace(engineValue) && string.IsNullOrWhiteSpace(connectionString))
                continue;

            if (string.IsNullOrWhiteSpace(engineValue))
                throw new InvalidOperationException($"Database '{child.Key}' declares a connection string but no engine.");

            if (string.IsNullOrWhiteSpace(connectionString))
                throw new InvalidOperationException($"Database '{child.Key}' declares an engine but no connection string.");

            if (!Enum.TryParse<DatabaseEngine>(engineValue, ignoreCase: true, out var engine) || !Enum.IsDefined(engine))
                throw new InvalidOperationException($"Database '{child.Key}' declares unsupported engine '{engineValue}'.");

            databases.Add(new DatabaseConfig(child.Key, engine, connectionString));
        }

        return databases;
    }
}
