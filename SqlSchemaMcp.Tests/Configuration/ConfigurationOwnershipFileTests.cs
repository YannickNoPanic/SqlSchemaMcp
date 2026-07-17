using FluentAssertions;
using System.Text.Json.Nodes;
using Xunit;

namespace SqlSchemaMcp.Tests.Configuration;

public sealed class ConfigurationOwnershipFileTests
{
    private static string RootPath =>
        Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "../../../../"));

    [Fact]
    public void DockerCompose_DoesNotDeclareApplicationSettingsOrDatabaseKeys()
    {
        var text = File.ReadAllText(Path.Combine(RootPath, "docker-compose.yml"));

        text.Should().Contain("env_file:");
        text.Should().NotContain("SQLMCP_SqlServer__Databases__");
        text.Should().NotContain("SQLMCP_Security__");
        text.Should().NotContain("SQLMCP_Audit__");
        text.Should().NotContain("environment:");
        text.Should().NotContain("poc");
        text.Should().NotContain("azure");
    }

    [Fact]
    public void AppsettingsExample_ContainsDefaultsButNoDatabaseEntries()
    {
        var json = JsonNode.Parse(File.ReadAllText(Path.Combine(RootPath, "appsettings.example.json")))!;
        var databases = json["SqlServer"]?["Databases"]?.AsObject();

        json["Mcp"]?["Port"]?.GetValue<int>().Should().Be(5101);
        json["Security"]?["VerifyLoginsAtStartup"]?.GetValue<bool>().Should().BeTrue();
        databases.Should().NotBeNull();
        databases!.Count.Should().Be(0);
    }

    [Fact]
    public void EnvExample_OwnsDatabaseEntriesForAllSupportedEngines()
    {
        var text = File.ReadAllText(Path.Combine(RootPath, ".env.example"));

        text.Should().Contain("SQLMCP_SqlServer__Databases__poc=");
        text.Should().Contain("SQLMCP_SqlServer__Databases__postgres_reporting__Engine=Postgres");
        text.Should().Contain("SQLMCP_SqlServer__Databases__postgres_reporting__ConnectionString=");
        text.Should().Contain("SQLMCP_SqlServer__Databases__mariadb_legacy__Engine=MariaDb");
        text.Should().Contain("SQLMCP_SqlServer__Databases__mariadb_legacy__ConnectionString=");
    }
}
