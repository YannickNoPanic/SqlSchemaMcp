using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using SqlSchemaMcp.Abstractions;
using SqlSchemaMcp.Abstractions.Capabilities;
using SqlSchemaMcp.Engines;
using SqlSchemaMcp.MariaDb;
using SqlSchemaMcp.MariaDb.Configuration;
using SqlSchemaMcp.MariaDb.Data;
using SqlSchemaMcp.Postgres;
using SqlSchemaMcp.Postgres.Configuration;
using SqlSchemaMcp.Postgres.Data;
using Xunit;

namespace SqlSchemaMcp.Tests.Engines;

public sealed class MultiEngineCapabilityRegistrationTests
{
    [Fact]
    public void TryResolve_PostgresSchemaCapability_ReturnsPostgresEngine()
    {
        var engine = CreatePostgresEngine();
        var resolver = new CapabilityResolver(
            [new DatabaseConfig("reporting", DatabaseEngine.Postgres, "Host=localhost;Database=reporting;")],
            new Dictionary<DatabaseEngine, object> { [DatabaseEngine.Postgres] = engine });

        var found = resolver.TryResolve<ISchemaCapability>("reporting", out var kind, out var capability);

        found.Should().BeTrue();
        kind.Should().Be(DatabaseEngine.Postgres);
        capability.Should().BeSameAs(engine);
    }

    [Fact]
    public void TryResolve_PostgresSqlServerDiagnosticsCapability_ReturnsFalse()
    {
        var resolver = new CapabilityResolver(
            [new DatabaseConfig("reporting", DatabaseEngine.Postgres, "Host=localhost;Database=reporting;")],
            new Dictionary<DatabaseEngine, object> { [DatabaseEngine.Postgres] = CreatePostgresEngine() });

        var found = resolver.TryResolve<ISqlServerDiagnosticsCapability>("reporting", out var kind, out var capability);

        found.Should().BeFalse();
        kind.Should().Be(DatabaseEngine.Postgres);
        capability.Should().BeNull();
    }

    [Fact]
    public void TryResolve_MariaDbSchemaSnapshotCapability_ReturnsMariaDbEngine()
    {
        var engine = CreateMariaDbEngine();
        var resolver = new CapabilityResolver(
            [new DatabaseConfig("legacy", DatabaseEngine.MariaDb, "Server=localhost;Database=legacy;")],
            new Dictionary<DatabaseEngine, object> { [DatabaseEngine.MariaDb] = engine });

        var found = resolver.TryResolve<ISchemaSnapshotCapability>("legacy", out var kind, out var capability);

        found.Should().BeTrue();
        kind.Should().Be(DatabaseEngine.MariaDb);
        capability.Should().BeSameAs(engine);
    }

    private static PostgresEngine CreatePostgresEngine()
    {
        var options = Options.Create(new PostgresEngineOptions());
        var schema = new PostgresSchema(options, NullLogger<PostgresSchema>.Instance);
        var snapshot = new PostgresSchemaSnapshot(options, NullLogger<PostgresSchemaSnapshot>.Instance);

        return new PostgresEngine(schema, snapshot);
    }

    private static MariaDbEngine CreateMariaDbEngine()
    {
        var options = Options.Create(new MariaDbEngineOptions());
        var schema = new MariaDbSchema(options, NullLogger<MariaDbSchema>.Instance);
        var snapshot = new MariaDbSchemaSnapshot(options, NullLogger<MariaDbSchemaSnapshot>.Instance);

        return new MariaDbEngine(schema, snapshot);
    }
}
