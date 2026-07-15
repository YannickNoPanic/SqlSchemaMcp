using FluentAssertions;
using SqlSchemaMcp.Abstractions;
using SqlSchemaMcp.Abstractions.Capabilities;
using SqlSchemaMcp.Engines;
using Xunit;

namespace SqlSchemaMcp.Tests.Engines;

public sealed class CapabilityResolverTests
{
    [Fact]
    public void DatabaseNames_ReturnsConfiguredNames()
    {
        var resolver = new CapabilityResolver(
            [new DatabaseConfig("poc", DatabaseEngine.SqlServer, "cs")],
            new Dictionary<DatabaseEngine, object>());

        resolver.DatabaseNames.Should().BeEquivalentTo(["poc"]);
    }

    [Fact]
    public void TryGetEngine_KnownDatabase_ReturnsEngine()
    {
        var resolver = new CapabilityResolver(
            [new DatabaseConfig("reporting", DatabaseEngine.Postgres, "cs")],
            new Dictionary<DatabaseEngine, object>());

        var found = resolver.TryGetEngine("REPORTING", out var engine);

        found.Should().BeTrue();
        engine.Should().Be(DatabaseEngine.Postgres);
    }

    [Fact]
    public void TryResolve_EngineImplementsCapability_ReturnsCapability()
    {
        var engine = new FakeEngine();
        var resolver = new CapabilityResolver(
            [new DatabaseConfig("poc", DatabaseEngine.SqlServer, "cs")],
            new Dictionary<DatabaseEngine, object> { [DatabaseEngine.SqlServer] = engine });

        var found = resolver.TryResolve<IFakeCapability>("poc", out var kind, out var capability);

        found.Should().BeTrue();
        kind.Should().Be(DatabaseEngine.SqlServer);
        capability.Should().BeSameAs(engine);
    }

    [Fact]
    public void TryResolve_EngineDoesNotImplementCapability_ReturnsFalseWithEngine()
    {
        var resolver = new CapabilityResolver(
            [new DatabaseConfig("poc", DatabaseEngine.SqlServer, "cs")],
            new Dictionary<DatabaseEngine, object> { [DatabaseEngine.SqlServer] = new object() });

        var found = resolver.TryResolve<IFakeCapability>("poc", out var kind, out var capability);

        found.Should().BeFalse();
        kind.Should().Be(DatabaseEngine.SqlServer);
        capability.Should().BeNull();
    }

    [Fact]
    public void TryResolve_UnknownDatabase_ReturnsFalse()
    {
        var resolver = new CapabilityResolver([], new Dictionary<DatabaseEngine, object>());

        var found = resolver.TryResolve<IFakeCapability>("missing", out _, out var capability);

        found.Should().BeFalse();
        capability.Should().BeNull();
    }

    private interface IFakeCapability;

    private sealed class FakeEngine : IDatabaseEngine, IFakeCapability
    {
        public DatabaseEngine Kind => DatabaseEngine.SqlServer;
    }
}
