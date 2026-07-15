using FluentAssertions;
using Microsoft.Extensions.Configuration;
using SqlSchemaMcp.Abstractions;
using SqlSchemaMcp.Configuration;
using Xunit;

namespace SqlSchemaMcp.Tests.Configuration;

public sealed class DatabaseConfigLoaderTests
{
    private static IConfiguration Build(Dictionary<string, string?> values) =>
        new ConfigurationBuilder().AddInMemoryCollection(values).Build();

    [Fact]
    public void Load_BareString_ImpliesSqlServer()
    {
        var configuration = Build(new()
        {
            ["SqlServer:Databases:poc"] = "Server=x;Database=y;"
        });

        var result = DatabaseConfigLoader.Load(configuration);

        result.Should().ContainSingle();
        result[0].Should().Be(new DatabaseConfig("poc", DatabaseEngine.SqlServer, "Server=x;Database=y;"));
    }

    [Fact]
    public void Load_ObjectForm_UsesDeclaredEngine()
    {
        var configuration = Build(new()
        {
            ["SqlServer:Databases:reporting:Engine"] = "Postgres",
            ["SqlServer:Databases:reporting:ConnectionString"] = "Host=h;Database=d;"
        });

        var result = DatabaseConfigLoader.Load(configuration);

        result.Should().ContainSingle();
        result[0].Should().Be(new DatabaseConfig("reporting", DatabaseEngine.Postgres, "Host=h;Database=d;"));
    }

    [Fact]
    public void Load_MixedForms_LoadsAllDatabases()
    {
        var configuration = Build(new()
        {
            ["SqlServer:Databases:poc"] = "Server=x;",
            ["SqlServer:Databases:legacy:Engine"] = "MariaDb",
            ["SqlServer:Databases:legacy:ConnectionString"] = "Server=m;"
        });

        var result = DatabaseConfigLoader.Load(configuration);

        result.Should().BeEquivalentTo([
            new DatabaseConfig("poc", DatabaseEngine.SqlServer, "Server=x;"),
            new DatabaseConfig("legacy", DatabaseEngine.MariaDb, "Server=m;")
        ]);
    }

    [Fact]
    public void Load_ObjectFormMissingConnectionString_ThrowsClearError()
    {
        var configuration = Build(new()
        {
            ["SqlServer:Databases:bad:Engine"] = "Postgres"
        });

        var act = () => DatabaseConfigLoader.Load(configuration);

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("Database 'bad' declares an engine but no connection string.");
    }

    [Fact]
    public void Load_ObjectFormWithUndefinedNumericEngine_ThrowsClearError()
    {
        var configuration = Build(new()
        {
            ["SqlServer:Databases:bad:Engine"] = "999",
            ["SqlServer:Databases:bad:ConnectionString"] = "Server=x;"
        });

        var act = () => DatabaseConfigLoader.Load(configuration);

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("Database 'bad' declares unsupported engine '999'.");
    }
}
