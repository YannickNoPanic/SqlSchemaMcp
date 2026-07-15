using SqlSchemaMcp.Abstractions;

namespace SqlSchemaMcp.Engines;

public sealed class CapabilityResolver : ICapabilityResolver
{
    private readonly IReadOnlyDictionary<string, DatabaseConfig> _databasesByName;
    private readonly IReadOnlyDictionary<DatabaseEngine, object> _enginesByKind;

    public CapabilityResolver(
        IReadOnlyList<DatabaseConfig> databases,
        IReadOnlyDictionary<DatabaseEngine, object> enginesByKind)
    {
        Databases = databases;
        _databasesByName = databases.ToDictionary(d => d.Name, StringComparer.OrdinalIgnoreCase);
        _enginesByKind = enginesByKind;
        DatabaseNames = _databasesByName.Keys.ToArray();
    }

    public IReadOnlyCollection<string> DatabaseNames { get; }
    public IReadOnlyList<DatabaseConfig> Databases { get; }

    public bool TryGetEngine(string database, out DatabaseEngine engine)
    {
        if (_databasesByName.TryGetValue(database, out var config))
        {
            engine = config.Engine;
            return true;
        }

        engine = default;
        return false;
    }

    public bool TryResolve<TCapability>(
        string database,
        out DatabaseEngine engine,
        out TCapability? capability)
        where TCapability : class
    {
        capability = null;

        if (!TryGetEngine(database, out engine))
            return false;

        if (!_enginesByKind.TryGetValue(engine, out var implementation))
            return false;

        capability = implementation as TCapability;
        return capability is not null;
    }
}
