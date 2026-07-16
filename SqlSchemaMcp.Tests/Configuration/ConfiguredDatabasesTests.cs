using FluentAssertions;
using SqlSchemaMcp.Abstractions;
using SqlSchemaMcp.Configuration;
using Xunit;

namespace SqlSchemaMcp.Tests.Configuration;

public sealed class ConfiguredDatabasesTests
{
    [Fact]
    public void SqlServerConnectionStrings_ReturnsOnlySqlServerDatabases()
    {
        var sut = new ConfiguredDatabases([
            new DatabaseConfig("poc", DatabaseEngine.SqlServer, "Server=poc;"),
            new DatabaseConfig("reporting", DatabaseEngine.Postgres, "Host=reporting;"),
            new DatabaseConfig("legacy", DatabaseEngine.MariaDb, "Server=legacy;")
        ]);

        var result = sut.SqlServerConnectionStrings;

        result.Should().ContainSingle();
        result.Should().ContainKey("poc").WhoseValue.Should().Be("Server=poc;");
    }

    [Fact]
    public void All_ReturnsEveryConfiguredDatabase()
    {
        var configs = new[]
        {
            new DatabaseConfig("poc", DatabaseEngine.SqlServer, "Server=poc;"),
            new DatabaseConfig("reporting", DatabaseEngine.Postgres, "Host=reporting;")
        };
        var sut = new ConfiguredDatabases(configs);

        var result = sut.All;

        result.Should().BeEquivalentTo(configs);
    }
}
