using FluentAssertions;
using SqlSchemaMcp.Abstractions;
using SqlSchemaMcp.Abstractions.Capabilities;
using SqlSchemaMcp.Data;
using SqlSchemaMcp.Engines;
using Xunit;

namespace SqlSchemaMcp.Tests.Data;

public sealed class PipelineQueriesDispatcherTests
{
    [Fact]
    public async Task ListDataFeeds_KnownDatabaseWithCapability_Delegates()
    {
        var capability = new FakePipelineCapability();
        var sut = CreateSut(DatabaseEngine.SqlServer, capability);

        var result = await sut.ListDataFeeds("poc", CancellationToken.None);

        result.Should().Be("feeds");
        capability.Calls.Should().Be(1);
    }

    [Fact]
    public async Task ListDataFeeds_KnownDatabaseWithoutCapability_ReturnsUnsupported()
    {
        var sut = CreateSut(DatabaseEngine.Postgres, new FakeEngine(DatabaseEngine.Postgres));

        var result = await sut.ListDataFeeds("poc", CancellationToken.None);

        result.Should().Be("UNSUPPORTED: Tool 'ListDataFeeds' is not available for engine 'Postgres'. Ask the maintainer to add support if you need this.");
    }

    private static PipelineQueries CreateSut(DatabaseEngine engine, object implementation)
    {
        var resolver = new CapabilityResolver(
            [new DatabaseConfig("poc", engine, "cs")],
            new Dictionary<DatabaseEngine, object> { [engine] = implementation });

        return new PipelineQueries(resolver);
    }

    private sealed class FakeEngine(DatabaseEngine kind) : IDatabaseEngine
    {
        public DatabaseEngine Kind { get; } = kind;
    }

    private sealed class FakePipelineCapability : IDatabaseEngine, ISqlServerPipelineCapability
    {
        public int Calls { get; private set; }
        public DatabaseEngine Kind => DatabaseEngine.SqlServer;

        public Task<string> ListDataFeeds(string database, CancellationToken ct)
        {
            Calls++;
            return Task.FromResult("feeds");
        }

        public Task<string> AnalyzeStagingHealth(string database, CancellationToken ct) => Task.FromResult("health");
        public Task<string> CompareStagingToCurrentSchema(string database, string feedBaseName, string currentTableName, CancellationToken ct) => Task.FromResult("compare");
    }
}
