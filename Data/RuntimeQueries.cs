using SqlSchemaMcp.Abstractions;
using SqlSchemaMcp.Runtime;

namespace SqlSchemaMcp.Data;

public sealed class RuntimeQueries(ICapabilityResolver resolver)
{
    public string ListConfiguredDatabases() =>
        new RuntimeCapabilityReporter(resolver.Databases).ListConfiguredDatabases();

    public string ListEngineCapabilities() =>
        new RuntimeCapabilityReporter(resolver.Databases).ListEngineCapabilities();

    public string CheckConfiguration() =>
        new RuntimeCapabilityReporter(resolver.Databases).CheckConfiguration();
}
