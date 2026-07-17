using System.Text;
using SqlSchemaMcp.Abstractions;

namespace SqlSchemaMcp.Runtime;

public sealed class RuntimeCapabilityReporter(IReadOnlyList<DatabaseConfig> databases)
{
    public string ListConfiguredDatabases()
    {
        var sb = new StringBuilder();
        sb.AppendLine("CONFIGURED DATABASES");
        sb.AppendLine("--------------------");

        if (databases.Count == 0)
        {
            sb.AppendLine("(none)");
            return sb.ToString();
        }

        foreach (var database in databases.OrderBy(database => database.Name, StringComparer.OrdinalIgnoreCase))
        {
            sb.AppendLine($"{database.Name} [{database.Engine}]");
            sb.AppendLine($"  Supported: {string.Join(", ", RuntimeCapabilityCatalog.Supported(database.Engine))}");

            var unsupported = RuntimeCapabilityCatalog.Unsupported(database.Engine);
            if (unsupported.Count > 0)
                sb.AppendLine($"  Unsupported: {string.Join(", ", unsupported)}");
        }

        return sb.ToString();
    }

    public string ListEngineCapabilities()
    {
        var sb = new StringBuilder();
        sb.AppendLine("ENGINE CAPABILITIES");
        sb.AppendLine("-------------------");

        foreach (DatabaseEngine engine in Enum.GetValues<DatabaseEngine>())
        {
            sb.AppendLine($"{engine}");
            sb.AppendLine($"  Supported: {string.Join(", ", RuntimeCapabilityCatalog.Supported(engine))}");

            var unsupported = RuntimeCapabilityCatalog.Unsupported(engine);
            if (unsupported.Count > 0)
                sb.AppendLine($"  Unsupported: {string.Join(", ", unsupported)}");
        }

        return sb.ToString();
    }

    public string CheckConfiguration()
    {
        var sb = new StringBuilder();
        sb.AppendLine("CONFIGURATION CHECK");
        sb.AppendLine("-------------------");

        if (databases.Count == 0)
        {
            sb.AppendLine("ERROR: No databases configured.");
            sb.AppendLine("Add at least one entry under SqlServer:Databases.");
            return sb.ToString();
        }

        sb.AppendLine($"OK: {databases.Count} database(s) configured.");
        foreach (var database in databases.OrderBy(database => database.Name, StringComparer.OrdinalIgnoreCase))
            sb.AppendLine($"OK: {database.Name} uses engine {database.Engine}.");

        sb.AppendLine("NOTE: SQL Server logins are checked by the startup read-only gate.");
        sb.AppendLine("NOTE: PostgreSQL and MariaDB credentials must be read-only at the database level.");

        return sb.ToString();
    }
}
