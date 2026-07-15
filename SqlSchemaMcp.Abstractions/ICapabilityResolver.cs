namespace SqlSchemaMcp.Abstractions;

public interface ICapabilityResolver
{
    IReadOnlyCollection<string> DatabaseNames { get; }
    IReadOnlyList<DatabaseConfig> Databases { get; }

    bool TryGetEngine(string database, out DatabaseEngine engine);

    bool TryResolve<TCapability>(
        string database,
        out DatabaseEngine engine,
        out TCapability? capability)
        where TCapability : class;
}
