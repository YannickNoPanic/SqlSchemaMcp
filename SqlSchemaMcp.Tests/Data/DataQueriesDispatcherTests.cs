using FluentAssertions;
using SqlSchemaMcp.Abstractions;
using SqlSchemaMcp.Abstractions.Capabilities;
using SqlSchemaMcp.Data;
using SqlSchemaMcp.Engines;
using Xunit;

namespace SqlSchemaMcp.Tests.Data;

public sealed class DataQueriesDispatcherTests
{
    [Fact]
    public async Task SampleTableData_KnownDatabaseWithCapability_Delegates()
    {
        var capability = new FakeDataSamplingCapability();
        var sut = CreateSut(DatabaseEngine.SqlServer, capability);

        var result = await sut.SampleTableData("poc", "dbo.Customers", 25, CancellationToken.None);

        result.Should().Be("sample");
        capability.Calls.Should().Be(1);
        capability.LastDatabase.Should().Be("poc");
        capability.LastTable.Should().Be("dbo.Customers");
    }

    [Fact]
    public async Task SampleTableData_UnknownDatabase_ReturnsUnknownDatabaseError()
    {
        var sut = new DataQueries(new CapabilityResolver([], new Dictionary<DatabaseEngine, object>()));

        var result = await sut.SampleTableData("missing", "dbo.Customers", 25, CancellationToken.None);

        result.Should().Be("ERROR: Unknown database 'missing'. Available: ");
    }

    [Fact]
    public async Task SampleTableData_KnownDatabaseWithoutCapability_ReturnsUnsupported()
    {
        var sut = CreateSut(DatabaseEngine.Postgres, new FakeEngine(DatabaseEngine.Postgres));

        var result = await sut.SampleTableData("poc", "dbo.Customers", 25, CancellationToken.None);

        result.Should().Be("UNSUPPORTED: Tool 'SampleTableData' is not available for engine 'Postgres'. Ask the maintainer to add support if you need this.");
    }

    private static DataQueries CreateSut(DatabaseEngine engine, object implementation)
    {
        var resolver = new CapabilityResolver(
            [new DatabaseConfig("poc", engine, "cs")],
            new Dictionary<DatabaseEngine, object> { [engine] = implementation });

        return new DataQueries(resolver);
    }

    private sealed class FakeEngine(DatabaseEngine kind) : IDatabaseEngine
    {
        public DatabaseEngine Kind { get; } = kind;
    }

    private sealed class FakeDataSamplingCapability : IDatabaseEngine, IDataSamplingCapability
    {
        public int Calls { get; private set; }
        public string? LastDatabase { get; private set; }
        public string? LastTable { get; private set; }
        public DatabaseEngine Kind => DatabaseEngine.SqlServer;

        public Task<string> SampleTableData(string database, string tableName, int rows, CancellationToken ct)
        {
            Calls++;
            LastDatabase = database;
            LastTable = tableName;

            return Task.FromResult("sample");
        }

        public Task<string> AnalyzeColumnDistribution(string database, string tableName, string columnName, CancellationToken ct) =>
            Task.FromResult("distribution");

        public Task<string> FindNullableColumnsWithNoNulls(string database, string tableName, CancellationToken ct) =>
            Task.FromResult("nullable");

        public Task<string> FindDuplicateRows(string database, string tableName, string columns, int top, CancellationToken ct) =>
            Task.FromResult("duplicates");
    }
}
