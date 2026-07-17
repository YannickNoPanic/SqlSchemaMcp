using FluentAssertions;
using SqlSchemaMcp.Abstractions;
using SqlSchemaMcp.Abstractions.Capabilities;
using SqlSchemaMcp.Data;
using SqlSchemaMcp.Engines;
using Xunit;

namespace SqlSchemaMcp.Tests.Data;

public sealed class SchemaQueriesDispatcherTests
{
    [Fact]
    public async Task ListTables_KnownDatabaseWithSchemaCapability_Delegates()
    {
        var capability = new FakeSchemaCapability();
        var sut = CreateSut(DatabaseEngine.SqlServer, capability);

        var result = await sut.ListTables("poc", "dbo", "Customer", CancellationToken.None);

        result.Should().Be("tables");
        capability.Calls.Should().Be(1);
        capability.LastDatabase.Should().Be("poc");
    }

    [Fact]
    public async Task ListTriggers_KnownDatabaseWithExtrasCapability_Delegates()
    {
        var capability = new FakeSqlServerSchemaExtrasCapability();
        var sut = CreateSut(DatabaseEngine.SqlServer, capability);

        var result = await sut.ListTriggers("poc", "audit", CancellationToken.None);

        result.Should().Be("triggers");
        capability.Calls.Should().Be(1);
        capability.LastDatabase.Should().Be("poc");
    }

    [Fact]
    public async Task ListTriggers_KnownDatabaseWithoutExtrasCapability_ReturnsUnsupported()
    {
        var sut = CreateSut(DatabaseEngine.Postgres, new FakeSchemaCapability());

        var result = await sut.ListTriggers("poc", "audit", CancellationToken.None);

        result.Should().Be("UNSUPPORTED: Tool 'ListTriggers' is not available for engine 'Postgres'. Ask the developer to add 'ISqlServerSchemaExtrasCapability' support for this engine.");
    }

    [Fact]
    public async Task ListTables_KnownDatabaseWithoutSchemaCapability_ReturnsUnsupported()
    {
        var sut = CreateSut(DatabaseEngine.Postgres, new FakeEngine(DatabaseEngine.Postgres));

        var result = await sut.ListTables("poc", null, null, CancellationToken.None);

        result.Should().Be("UNSUPPORTED: Tool 'ListTables' is not available for engine 'Postgres'. Ask the developer to add 'ISchemaCapability' support for this engine.");
    }

    [Fact]
    public async Task ListTables_UnknownDatabase_ReturnsUnknownDatabaseError()
    {
        var sut = new SchemaQueries(new CapabilityResolver([], new Dictionary<DatabaseEngine, object>()));

        var result = await sut.ListTables("missing", null, null, CancellationToken.None);

        result.Should().Be("ERROR: Unknown database 'missing'. Available: ");
    }

    private static SchemaQueries CreateSut(DatabaseEngine engine, object implementation)
    {
        var resolver = new CapabilityResolver(
            [new DatabaseConfig("poc", engine, "cs")],
            new Dictionary<DatabaseEngine, object> { [engine] = implementation });

        return new SchemaQueries(resolver);
    }

    private sealed class FakeEngine(DatabaseEngine kind) : IDatabaseEngine
    {
        public DatabaseEngine Kind { get; } = kind;
    }

    private sealed class FakeSchemaCapability : IDatabaseEngine, ISchemaCapability
    {
        public int Calls { get; private set; }
        public string? LastDatabase { get; private set; }
        public DatabaseEngine Kind => DatabaseEngine.SqlServer;

        public Task<string> ListTables(string database, string? schemaFilter, string? nameFilter, CancellationToken ct)
        {
            Calls++;
            LastDatabase = database;

            return Task.FromResult("tables");
        }

        public Task<string> ListViews(string database, string? nameFilter, CancellationToken ct) => Task.FromResult("views");
        public Task<string> ListProcedures(string database, string? nameFilter, CancellationToken ct) => Task.FromResult("procedures");
        public Task<string> ListFunctions(string database, string? nameFilter, CancellationToken ct) => Task.FromResult("functions");
        public Task<string> GetTableSchema(string database, string tableName, CancellationToken ct) => Task.FromResult("table schema");
        public Task<string> GetViewDefinition(string database, string viewName, CancellationToken ct) => Task.FromResult("view definition");
        public Task<string> GetProcedureDefinition(string database, string procName, CancellationToken ct) => Task.FromResult("proc definition");
        public Task<string> GetFunctionDefinition(string database, string functionName, CancellationToken ct) => Task.FromResult("function definition");
        public Task<string> FindReferences(string database, string objectName, CancellationToken ct) => Task.FromResult("references");
        public Task<string> SearchDefinitions(string database, string keyword, CancellationToken ct) => Task.FromResult("definitions");
    }

    private sealed class FakeSqlServerSchemaExtrasCapability : IDatabaseEngine, ISqlServerSchemaExtrasCapability
    {
        public int Calls { get; private set; }
        public string? LastDatabase { get; private set; }
        public DatabaseEngine Kind => DatabaseEngine.SqlServer;

        public Task<string> ListTriggers(string database, string? nameFilter, CancellationToken ct)
        {
            Calls++;
            LastDatabase = database;

            return Task.FromResult("triggers");
        }

        public Task<string> GetTriggerDefinition(string database, string triggerName, CancellationToken ct) => Task.FromResult("trigger definition");
        public Task<string> ListSynonyms(string database, string? nameFilter, CancellationToken ct) => Task.FromResult("synonyms");
        public Task<string> ListCheckConstraints(string database, string? nameFilter, CancellationToken ct) => Task.FromResult("checks");
        public Task<string> ListDdlTriggers(string database, CancellationToken ct) => Task.FromResult("ddl triggers");
        public Task<string> GetDdlTriggerDefinition(string database, string triggerName, CancellationToken ct) => Task.FromResult("ddl trigger definition");
    }
}
