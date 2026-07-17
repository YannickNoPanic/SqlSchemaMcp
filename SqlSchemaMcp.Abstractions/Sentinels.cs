namespace SqlSchemaMcp.Abstractions;

public static class Sentinels
{
    public static string UnknownDatabase(IEnumerable<string> availableNames, string database) =>
        $"ERROR: Unknown database '{database}'. Available: {string.Join(", ", availableNames)}";

    public static string Unsupported(string toolName, DatabaseEngine engine, string capabilityName) =>
        $"UNSUPPORTED: Tool '{toolName}' is not available for engine '{engine}'. Ask the developer to add '{capabilityName}' support for this engine.";
}
