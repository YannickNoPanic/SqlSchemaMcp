using FluentAssertions;
using SqlSchemaMcp.Abstractions;
using SqlSchemaMcp.Abstractions.Capabilities;
using SqlSchemaMcp.Data;
using SqlSchemaMcp.Engines;
using Xunit;

namespace SqlSchemaMcp.Tests.Data;

public sealed class DiagnosticsQueriesDispatcherTests
{
    [Fact]
    public async Task ListAgentJobs_KnownDatabaseWithCapability_Delegates()
    {
        var capability = new FakeDiagnosticsCapability();
        var sut = CreateSut(DatabaseEngine.SqlServer, capability);

        var result = await sut.ListAgentJobs("poc", CancellationToken.None);

        result.Should().Be("jobs");
        capability.Calls.Should().Be(1);
        capability.LastDatabase.Should().Be("poc");
    }

    [Fact]
    public async Task ListAgentJobs_UnknownDatabase_ReturnsUnknownDatabaseError()
    {
        var sut = new DiagnosticsQueries(new CapabilityResolver([], new Dictionary<DatabaseEngine, object>()));

        var result = await sut.ListAgentJobs("missing", CancellationToken.None);

        result.Should().Be("ERROR: Unknown database 'missing'. Available: ");
    }

    [Fact]
    public async Task ListAgentJobs_KnownDatabaseWithoutCapability_ReturnsUnsupported()
    {
        var sut = CreateSut(DatabaseEngine.Postgres, new FakeEngine(DatabaseEngine.Postgres));

        var result = await sut.ListAgentJobs("poc", CancellationToken.None);

        result.Should().Be("UNSUPPORTED: Tool 'ListAgentJobs' is not available for engine 'Postgres'. Ask the developer to add 'ISqlServerDiagnosticsCapability' support for this engine.");
    }

    [Fact]
    public async Task AnalyzeWaitStats_KnownDatabaseWithoutCapability_ReturnsUnsupported()
    {
        var sut = CreateSut(DatabaseEngine.Postgres, new FakeEngine(DatabaseEngine.Postgres));

        var result = await sut.AnalyzeWaitStats("poc", CancellationToken.None);

        result.Should().Be("UNSUPPORTED: Tool 'AnalyzeWaitStats' is not available for engine 'Postgres'. Ask the developer to add 'ISqlServerDiagnosticsCapability' support for this engine.");
    }

    private static DiagnosticsQueries CreateSut(DatabaseEngine engine, object implementation)
    {
        var resolver = new CapabilityResolver(
            [new DatabaseConfig("poc", engine, "cs")],
            new Dictionary<DatabaseEngine, object> { [engine] = implementation });

        return new DiagnosticsQueries(resolver);
    }

    private sealed class FakeEngine(DatabaseEngine kind) : IDatabaseEngine
    {
        public DatabaseEngine Kind { get; } = kind;
    }

    private sealed class FakeDiagnosticsCapability : IDatabaseEngine, ISqlServerDiagnosticsCapability
    {
        public int Calls { get; private set; }
        public string? LastDatabase { get; private set; }
        public DatabaseEngine Kind => DatabaseEngine.SqlServer;

        public Task<string> ListAgentJobs(string database, CancellationToken ct)
        {
            Calls++;
            LastDatabase = database;

            return Task.FromResult("jobs");
        }

        public Task<string> GetFailingJobs(string database, CancellationToken ct) => Task.FromResult("failing");
        public Task<string> GetJobHistory(string database, string jobName, int maxRuns, CancellationToken ct) => Task.FromResult("history");
        public Task<string> AnalyzeTopExpensiveQueries(string database, int top, CancellationToken ct) => Task.FromResult("queries");
        public Task<string> AnalyzeWaitStats(string database, CancellationToken ct) => Task.FromResult("waits");
        public Task<string> ListLinkedServers(string database, CancellationToken ct) => Task.FromResult("linked");
        public Task<string> FindLinkedServerUsage(string database, string? linkedServerName, CancellationToken ct) => Task.FromResult("usage");
        public Task<string> ListServiceBroker(string database, CancellationToken ct) => Task.FromResult("broker");
        public Task<string> ListClrAssemblies(string database, CancellationToken ct) => Task.FromResult("clr");
    }
}
