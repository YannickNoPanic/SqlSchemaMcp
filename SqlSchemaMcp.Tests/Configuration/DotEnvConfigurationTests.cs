using FluentAssertions;
using SqlSchemaMcp.Configuration;
using Xunit;

namespace SqlSchemaMcp.Tests.Configuration;

public sealed class DotEnvConfigurationTests : IDisposable
{
    private readonly string _directory = Path.Combine(Path.GetTempPath(), $"sqlmcp-env-{Guid.NewGuid():N}");

    public DotEnvConfigurationTests()
    {
        Directory.CreateDirectory(_directory);
    }

    [Fact]
    public void Load_WithSqlMcpVariables_StripsPrefixAndMapsDoubleUnderscoreToConfigurationPath()
    {
        var path = Path.Combine(_directory, ".env");
        File.WriteAllLines(path, [
            "SQLMCP_Mcp__Port=6100",
            "SQLMCP_SqlServer__Databases__postgres_reporting__Engine=Postgres",
            "SQLMCP_SqlServer__Databases__postgres_reporting__ConnectionString=Host=localhost;Database=Reporting;Username=readonly;Password=secret"
        ]);

        var result = DotEnvConfiguration.Load(path);

        result.Should().ContainKey("Mcp:Port").WhoseValue.Should().Be("6100");
        result.Should().ContainKey("SqlServer:Databases:postgres_reporting:Engine").WhoseValue.Should().Be("Postgres");
        result.Should().ContainKey("SqlServer:Databases:postgres_reporting:ConnectionString")
            .WhoseValue.Should().Be("Host=localhost;Database=Reporting;Username=readonly;Password=secret");
    }

    [Fact]
    public void Load_WithCommentsBlankLinesAndQuotes_IgnoresCommentsAndUnquotesValues()
    {
        var path = Path.Combine(_directory, ".env");
        File.WriteAllLines(path, [
            "# comment",
            "",
            "SQLMCP_Audit__Path=\"/data/audit-log.jsonl\"",
            "UNRELATED=value"
        ]);

        var result = DotEnvConfiguration.Load(path);

        result.Should().ContainSingle();
        result.Should().ContainKey("Audit:Path").WhoseValue.Should().Be("/data/audit-log.jsonl");
    }

    [Fact]
    public void FindNearest_StartsAtDirectoryAndWalksUp()
    {
        var child = Path.Combine(_directory, "bin", "Debug");
        Directory.CreateDirectory(child);
        var path = Path.Combine(_directory, ".env");
        File.WriteAllText(path, "SQLMCP_Mcp__Port=6100");

        var result = DotEnvConfiguration.FindNearest(child);

        result.Should().Be(path);
    }

    public void Dispose()
    {
        if (Directory.Exists(_directory))
            Directory.Delete(_directory, recursive: true);
    }
}
