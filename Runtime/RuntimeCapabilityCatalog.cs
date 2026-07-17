using SqlSchemaMcp.Abstractions;

namespace SqlSchemaMcp.Runtime;

public static class RuntimeCapabilityCatalog
{
    private static readonly IReadOnlyList<string> SqlServerCapabilities =
    [
        "Query",
        "Schema",
        "SchemaExtras",
        "SchemaSnapshot",
        "SharedAnalysis",
        "SqlServerAnalysis",
        "DataSampling",
        "Diagnostics",
        "Pipeline",
        "Security"
    ];

    private static readonly IReadOnlyList<string> SnapshotCapabilities =
    [
        "Schema",
        "SchemaSnapshot",
        "SharedAnalysis"
    ];

    public static IReadOnlyList<string> Supported(DatabaseEngine engine) =>
        engine == DatabaseEngine.SqlServer ? SqlServerCapabilities : SnapshotCapabilities;

    public static IReadOnlyList<string> Unsupported(DatabaseEngine engine)
    {
        var supported = Supported(engine).ToHashSet(StringComparer.Ordinal);

        return SqlServerCapabilities
            .Where(capability => !supported.Contains(capability))
            .ToArray();
    }
}
