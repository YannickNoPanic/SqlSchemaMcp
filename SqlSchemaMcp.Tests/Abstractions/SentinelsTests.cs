using FluentAssertions;
using SqlSchemaMcp.Abstractions;
using Xunit;

namespace SqlSchemaMcp.Tests.Abstractions;

public sealed class SentinelsTests
{
    [Fact]
    public void UnknownDatabase_ListsAvailableNames()
    {
        var result = Sentinels.UnknownDatabase(["poc", "azure"], "reporting");

        result.Should().Be("ERROR: Unknown database 'reporting'. Available: poc, azure");
    }

    [Fact]
    public void Unsupported_NamesToolAndEngineAndMaintainerAction()
    {
        var result = Sentinels.Unsupported("AnalyzeWaitStats", DatabaseEngine.Postgres, "ISqlServerDiagnosticsCapability");

        result.Should().Be("UNSUPPORTED: Tool 'AnalyzeWaitStats' is not available for engine 'Postgres'. Ask the developer to add 'ISqlServerDiagnosticsCapability' support for this engine.");
    }
}
