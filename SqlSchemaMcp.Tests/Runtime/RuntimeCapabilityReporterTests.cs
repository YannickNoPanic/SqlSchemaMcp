using FluentAssertions;
using SqlSchemaMcp.Abstractions;
using SqlSchemaMcp.Runtime;
using Xunit;

namespace SqlSchemaMcp.Tests.Runtime;

public sealed class RuntimeCapabilityReporterTests
{
    [Fact]
    public void ListConfiguredDatabases_WithThreeEngines_ShowsEngineAndCapabilities()
    {
        var sut = new RuntimeCapabilityReporter([
            new DatabaseConfig("poc", DatabaseEngine.SqlServer, "Server=.;Database=poc;"),
            new DatabaseConfig("reporting", DatabaseEngine.Postgres, "Host=localhost;Database=reporting;"),
            new DatabaseConfig("legacy", DatabaseEngine.MariaDb, "Server=localhost;Database=legacy;")
        ]);

        var result = sut.ListConfiguredDatabases();

        result.Should().Contain("CONFIGURED DATABASES");
        result.Should().Contain("poc");
        result.Should().Contain("SqlServer");
        result.Should().Contain("Query, Schema, SchemaExtras, SchemaSnapshot, SharedAnalysis, SqlServerAnalysis, DataSampling, Diagnostics, Pipeline, Security");
        result.Should().Contain("reporting");
        result.Should().Contain("Postgres");
        result.Should().Contain("Schema, SchemaSnapshot, SharedAnalysis");
        result.Should().Contain("legacy");
        result.Should().Contain("MariaDb");
    }

    [Fact]
    public void ListEngineCapabilities_ShowsSupportedAndUnsupportedGroups()
    {
        var sut = new RuntimeCapabilityReporter([]);

        var result = sut.ListEngineCapabilities();

        result.Should().Contain("ENGINE CAPABILITIES");
        result.Should().Contain("SqlServer");
        result.Should().Contain("Supported: Query, Schema, SchemaExtras, SchemaSnapshot, SharedAnalysis, SqlServerAnalysis, DataSampling, Diagnostics, Pipeline, Security");
        result.Should().Contain("Postgres");
        result.Should().Contain("Supported: Schema, SchemaSnapshot, SharedAnalysis");
        result.Should().Contain("Unsupported: Query, SchemaExtras, SqlServerAnalysis, DataSampling, Diagnostics, Pipeline, Security");
        result.Should().Contain("MariaDb");
    }

    [Fact]
    public void CheckConfiguration_WithNoDatabases_ReturnsActionableError()
    {
        var sut = new RuntimeCapabilityReporter([]);

        var result = sut.CheckConfiguration();

        result.Should().Contain("CONFIGURATION CHECK");
        result.Should().Contain("ERROR: No databases configured.");
        result.Should().Contain("Add at least one entry under SqlServer:Databases.");
    }
}
