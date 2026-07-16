using FluentAssertions;
using SqlSchemaMcp.Abstractions;
using SqlSchemaMcp.Abstractions.Capabilities;
using SqlSchemaMcp.Data;
using SqlSchemaMcp.Engines;
using Xunit;

namespace SqlSchemaMcp.Tests.Data;

public sealed class QueryQueriesDispatcherTests
{
    [Fact]
    public async Task ExecuteQuery_UnknownDatabase_ReturnsUnknownDatabaseError()
    {
        var sut = new QueryQueries(new CapabilityResolver([], new Dictionary<DatabaseEngine, object>()));

        var result = await sut.ExecuteQuery("missing", "SELECT 1", CancellationToken.None);

        result.Should().Be("ERROR: Unknown database 'missing'. Available: ");
    }

    [Fact]
    public async Task ExecuteQuery_KnownDatabaseWithoutCapability_ReturnsUnsupported()
    {
        var resolver = new CapabilityResolver(
            [new DatabaseConfig("analytics", DatabaseEngine.Postgres, "cs")],
            new Dictionary<DatabaseEngine, object> { [DatabaseEngine.Postgres] = new FakeEngine(DatabaseEngine.Postgres) });
        var sut = new QueryQueries(resolver);

        var result = await sut.ExecuteQuery("analytics", "SELECT 1", CancellationToken.None);

        result.Should().Be("UNSUPPORTED: Tool 'ExecuteQuery' is not available for engine 'Postgres'. Ask the maintainer to add support if you need this.");
    }

    [Fact]
    public async Task ExecuteQuery_KnownDatabaseWithCapability_Delegates()
    {
        var capability = new FakeQueryCapability();
        var resolver = new CapabilityResolver(
            [new DatabaseConfig("poc", DatabaseEngine.SqlServer, "cs")],
            new Dictionary<DatabaseEngine, object> { [DatabaseEngine.SqlServer] = capability });
        var sut = new QueryQueries(resolver);

        var result = await sut.ExecuteQuery("poc", "SELECT 1", CancellationToken.None);

        result.Should().Be("delegated");
        capability.Calls.Should().Be(1);
        capability.Database.Should().Be("poc");
        capability.Sql.Should().Be("SELECT 1");
    }

    private sealed class FakeEngine(DatabaseEngine kind) : IDatabaseEngine
    {
        public DatabaseEngine Kind { get; } = kind;
    }

    private sealed class FakeQueryCapability : IDatabaseEngine, IReadOnlyQueryCapability
    {
        public int Calls { get; private set; }
        public string? Database { get; private set; }
        public string? Sql { get; private set; }
        public DatabaseEngine Kind => DatabaseEngine.SqlServer;

        public Task<string> ExecuteQuery(string database, string sql, CancellationToken ct)
        {
            Calls++;
            Database = database;
            Sql = sql;

            return Task.FromResult("delegated");
        }
    }
}
