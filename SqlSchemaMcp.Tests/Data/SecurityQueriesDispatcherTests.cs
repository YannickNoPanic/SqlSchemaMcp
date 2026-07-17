using FluentAssertions;
using SqlSchemaMcp.Abstractions;
using SqlSchemaMcp.Abstractions.Capabilities;
using SqlSchemaMcp.Data;
using SqlSchemaMcp.Engines;
using Xunit;

namespace SqlSchemaMcp.Tests.Data;

public sealed class SecurityQueriesDispatcherTests
{
    [Fact]
    public async Task ListDatabaseUsers_KnownDatabaseWithCapability_Delegates()
    {
        var capability = new FakeSecurityCapability();
        var sut = CreateSut(DatabaseEngine.SqlServer, capability);

        var result = await sut.ListDatabaseUsers("poc", CancellationToken.None);

        result.Should().Be("users");
        capability.Calls.Should().Be(1);
    }

    [Fact]
    public async Task ListDatabaseUsers_KnownDatabaseWithoutCapability_ReturnsUnsupported()
    {
        var sut = CreateSut(DatabaseEngine.Postgres, new FakeEngine(DatabaseEngine.Postgres));

        var result = await sut.ListDatabaseUsers("poc", CancellationToken.None);

        result.Should().Be("UNSUPPORTED: Tool 'ListDatabaseUsers' is not available for engine 'Postgres'. Ask the developer to add 'ISqlServerSecurityCapability' support for this engine.");
    }

    private static SecurityQueries CreateSut(DatabaseEngine engine, object implementation)
    {
        var resolver = new CapabilityResolver(
            [new DatabaseConfig("poc", engine, "cs")],
            new Dictionary<DatabaseEngine, object> { [engine] = implementation });

        return new SecurityQueries(resolver);
    }

    private sealed class FakeEngine(DatabaseEngine kind) : IDatabaseEngine
    {
        public DatabaseEngine Kind { get; } = kind;
    }

    private sealed class FakeSecurityCapability : IDatabaseEngine, ISqlServerSecurityCapability
    {
        public int Calls { get; private set; }
        public DatabaseEngine Kind => DatabaseEngine.SqlServer;

        public Task<string> ListDatabaseUsers(string database, CancellationToken ct)
        {
            Calls++;
            return Task.FromResult("users");
        }

        public Task<string> ListObjectPermissions(string database, CancellationToken ct) => Task.FromResult("permissions");
    }
}
