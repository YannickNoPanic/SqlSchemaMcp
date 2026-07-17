using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using SqlSchemaMcp.Abstractions;
using SqlSchemaMcp.Abstractions.Capabilities;
using SqlSchemaMcp.Data;
using SqlSchemaMcp.Engines;
using SqlSchemaMcp.SqlServer.Configuration;
using Xunit;

namespace SqlSchemaMcp.Tests.Data;

public sealed class AnalysisQueriesSqlServerOnlyDispatcherTests
{
    [Fact]
    public async Task AnalyzeDuplicateIndexes_KnownDatabaseWithCapability_Delegates()
    {
        var capability = new FakeAnalysisCapability();
        var sut = CreateSut(DatabaseEngine.SqlServer, capability);

        var result = await sut.AnalyzeDuplicateIndexes("poc", CancellationToken.None);

        result.Should().Be("duplicate indexes");
        capability.Calls.Should().Be(1);
    }

    [Fact]
    public async Task AnalyzeDuplicateIndexes_KnownDatabaseWithoutCapability_ReturnsUnsupported()
    {
        var sut = CreateSut(DatabaseEngine.Postgres, new FakeEngine(DatabaseEngine.Postgres));

        var result = await sut.AnalyzeDuplicateIndexes("poc", CancellationToken.None);

        result.Should().Be("UNSUPPORTED: Tool 'AnalyzeDuplicateIndexes' is not available for engine 'Postgres'. Ask the developer to add 'ISqlServerAnalysisCapability' support for this engine.");
    }

    [Fact]
    public async Task AnalyzeNamingConventions_SnapshotFailure_ReturnsSafeError()
    {
        var sut = CreateSut(DatabaseEngine.SqlServer, new ThrowingSnapshotCapability());

        var result = await sut.AnalyzeNamingConventions("poc", CancellationToken.None);

        result.Should().Be("ERROR: the query failed. Check the server log for details.");
    }

    private static AnalysisQueries CreateSut(DatabaseEngine engine, object implementation)
    {
        var resolver = new CapabilityResolver(
            [new DatabaseConfig("poc", engine, "cs")],
            new Dictionary<DatabaseEngine, object> { [engine] = implementation });

        return new AnalysisQueries(
            Options.Create(new SqlServerEngineOptions()),
            NullLogger<AnalysisQueries>.Instance,
            resolver);
    }

    private sealed class FakeEngine(DatabaseEngine kind) : IDatabaseEngine
    {
        public DatabaseEngine Kind { get; } = kind;
    }

    private sealed class ThrowingSnapshotCapability : IDatabaseEngine, ISchemaSnapshotCapability
    {
        public DatabaseEngine Kind => DatabaseEngine.SqlServer;

        public Task<SchemaSnapshot> GetSchemaSnapshot(string database, CancellationToken ct) =>
            throw new InvalidOperationException("catalog failed");
    }

    private sealed class FakeAnalysisCapability : IDatabaseEngine, ISqlServerAnalysisCapability
    {
        public int Calls { get; private set; }
        public DatabaseEngine Kind => DatabaseEngine.SqlServer;

        public Task<string> AnalyzeDuplicateIndexes(string database, CancellationToken ct)
        {
            Calls++;
            return Task.FromResult("duplicate indexes");
        }

        public Task<string> AnalyzeProcComplexity(string database, string? nameFilter, CancellationToken ct) => Task.FromResult("proc");
        public Task<string> AnalyzeViewComplexity(string database, string? nameFilter, CancellationToken ct) => Task.FromResult("view");
        public Task<string> FindUnusedTables(string database, CancellationToken ct) => Task.FromResult("tables");
        public Task<string> FindUnusedProcedures(string database, CancellationToken ct) => Task.FromResult("procedures");
        public Task<string> AnalyzeIndexFragmentation(string database, string? nameFilter, CancellationToken ct) => Task.FromResult("fragmentation");
        public Task<string> AnalyzeTriggers(string database, CancellationToken ct) => Task.FromResult("triggers");
        public Task<string> AnalyzeIdentityColumns(string database, CancellationToken ct) => Task.FromResult("identity");
        public Task<string> AnalyzeTableSizes(string database, CancellationToken ct) => Task.FromResult("sizes");
        public Task<string> AnalyzeMissingIndexSuggestions(string database, CancellationToken ct) => Task.FromResult("suggestions");
        public Task<string> GetRecentObjectChanges(string database, int days, CancellationToken ct) => Task.FromResult("changes");
        public Task<string> AnalyzeTableQueryStats(string database, CancellationToken ct) => Task.FromResult("query stats");
        public Task<string> AnalyzeTableAccessStats(string database, CancellationToken ct) => Task.FromResult("access stats");
        public Task<string> GenerateDatabaseSummary(string database, CancellationToken ct) => Task.FromResult("summary");
    }
}
