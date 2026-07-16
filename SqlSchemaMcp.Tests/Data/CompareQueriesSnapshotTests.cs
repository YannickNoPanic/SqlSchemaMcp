using FluentAssertions;
using SqlSchemaMcp.Abstractions;
using SqlSchemaMcp.Abstractions.Capabilities;
using SqlSchemaMcp.Data;
using SqlSchemaMcp.Engines;
using Xunit;

namespace SqlSchemaMcp.Tests.Data;

public sealed class CompareQueriesSnapshotTests
{
    [Fact]
    public async Task GetTableNames_UsesSchemaSnapshotCapability()
    {
        var sut = CreateSut(new SchemaSnapshot(
            [
                new SchemaObject("TABLE", "dbo", "Customers"),
                new SchemaObject("VIEW", "dbo", "CustomerView")
            ],
            [],
            EmptySet(),
            EmptySet(),
            EmptySet()));

        var result = await sut.GetTableNames("poc", CancellationToken.None);

        result.Should().BeEquivalentTo(["dbo.Customers"]);
    }

    [Fact]
    public async Task GetProcNames_UsesSchemaSnapshotCapability()
    {
        var sut = CreateSut(new SchemaSnapshot(
            [
                new SchemaObject("PROCEDURE", "dbo", "BuildOrders"),
                new SchemaObject("TABLE", "dbo", "Orders")
            ],
            [],
            EmptySet(),
            EmptySet(),
            EmptySet()));

        var result = await sut.GetProcNames("poc", CancellationToken.None);

        result.Should().BeEquivalentTo(["dbo.BuildOrders"]);
    }

    [Fact]
    public async Task GetViewNames_UsesSchemaSnapshotCapability()
    {
        var sut = CreateSut(new SchemaSnapshot(
            [
                new SchemaObject("VIEW", "dbo", "OrderSummary"),
                new SchemaObject("TABLE", "dbo", "Orders")
            ],
            [],
            EmptySet(),
            EmptySet(),
            EmptySet()));

        var result = await sut.GetViewNames("poc", CancellationToken.None);

        result.Should().BeEquivalentTo(["dbo.OrderSummary"]);
    }

    [Fact]
    public async Task GetTableColumns_UsesSchemaSnapshotCapability()
    {
        var sut = CreateSut(new SchemaSnapshot(
            [],
            [new SchemaColumn("dbo", "Customers", "Name", ColumnTypeCategory.Text, "nvarchar(100)", "YES")],
            EmptySet(),
            EmptySet(),
            EmptySet()));

        var result = await sut.GetTableColumns("poc", "dbo.Customers", CancellationToken.None);

        result.Should().ContainSingle()
            .Which.Should().Be(new ColumnInfo("Name", "nvarchar(100)", "YES"));
    }

    private static CompareQueries CreateSut(SchemaSnapshot snapshot)
    {
        var capability = new FakeSnapshotCapability(snapshot);
        var resolver = new CapabilityResolver(
            [new DatabaseConfig("poc", DatabaseEngine.SqlServer, "cs")],
            new Dictionary<DatabaseEngine, object> { [DatabaseEngine.SqlServer] = capability });

        return new CompareQueries(resolver);
    }

    private static HashSet<string> EmptySet() => new(StringComparer.OrdinalIgnoreCase);

    private sealed class FakeSnapshotCapability(SchemaSnapshot snapshot) : IDatabaseEngine, ISchemaSnapshotCapability
    {
        public DatabaseEngine Kind => DatabaseEngine.SqlServer;

        public Task<SchemaSnapshot> GetSchemaSnapshot(string database, CancellationToken ct) =>
            Task.FromResult(snapshot);
    }
}
