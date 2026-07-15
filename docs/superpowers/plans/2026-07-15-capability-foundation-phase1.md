# Capability Foundation Phase 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the stable capability-based foundation primitives without moving the existing SQL Server query implementation yet.

**Architecture:** Create `SqlSchemaMcp.Abstractions` for pure contracts and shared models, add a backward-compatible mixed database config loader in the host, add a capability resolver, and teach auditing that `UNSUPPORTED:` is a failed tool outcome. This phase deliberately leaves `Tools/*.cs` and current `Data/*Queries.cs` behavior unchanged.

**Tech Stack:** .NET 10, C#, xUnit, FluentAssertions, NSubstitute, Microsoft.Extensions.Configuration.

## Global Constraints

- Follow `docs/superpowers/specs/2026-07-15-capability-based-multi-engine-design.md`.
- No god interface.
- `Tools/*.cs` must remain unchanged.
- Existing 35 tests must remain green.
- Use `UNSUPPORTED:` only as a clear user-facing message; no remote notification or tracking.
- Keep existing `SqlServer:Databases` config shape backward compatible.
- Use file-scoped namespaces and primary constructors where applicable.
- Async methods must accept/pass `CancellationToken`.
- Do not move SQL Server query classes in this phase.

---

## File Structure

Created:

- `SqlSchemaMcp.Abstractions/SqlSchemaMcp.Abstractions.csproj` - pure contract project.
- `SqlSchemaMcp.Abstractions/DatabaseEngine.cs` - engine enum.
- `SqlSchemaMcp.Abstractions/DatabaseConfig.cs` - normalized configured database record.
- `SqlSchemaMcp.Abstractions/Sentinels.cs` - unknown database and unsupported messages.
- `SqlSchemaMcp.Abstractions/SchemaModels.cs` - shared schema snapshot records.
- `SqlSchemaMcp.Abstractions/Capabilities/IDatabaseEngine.cs` - minimal engine identity.
- `SqlSchemaMcp.Abstractions/Capabilities/ISchemaCapability.cs` - schema browsing capability.
- `SqlSchemaMcp.Abstractions/Capabilities/ISqlServerSchemaExtrasCapability.cs` - SQL Server schema extras.
- `SqlSchemaMcp.Abstractions/Capabilities/IReadOnlyQueryCapability.cs` - read-only query capability.
- `SqlSchemaMcp.Abstractions/Capabilities/IDataSamplingCapability.cs` - data sampling capability.
- `SqlSchemaMcp.Abstractions/Capabilities/ISchemaSnapshotCapability.cs` - shared snapshot capability.
- `SqlSchemaMcp.Abstractions/Capabilities/ISqlServerDiagnosticsCapability.cs` - SQL Server diagnostics capability.
- `SqlSchemaMcp.Abstractions/ICapabilityResolver.cs` - database-to-capability resolver contract.
- `Configuration/DatabaseConfigLoader.cs` - backward-compatible config loader.
- `Engines/CapabilityResolver.cs` - host resolver implementation.
- `SqlSchemaMcp.Tests/Abstractions/SentinelsTests.cs`
- `SqlSchemaMcp.Tests/Configuration/DatabaseConfigLoaderTests.cs`
- `SqlSchemaMcp.Tests/Engines/CapabilityResolverTests.cs`

Modified:

- `SqlSchemaMcp.sln` - add abstractions project.
- `SqlSchemaMcp.csproj` - reference abstractions and exclude nested project sources.
- `SqlSchemaMcp.Tests/SqlSchemaMcp.Tests.csproj` - reference abstractions.
- `Auditing/FileAuditLog.cs` - treat `UNSUPPORTED:` as failure.
- `Auditing/IAuditLog.cs` - update success semantics comment.
- `SqlSchemaMcp.Tests/Auditing/FileAuditLogTests.cs` - add unsupported audit test.

---

### Task 1: Add Abstractions Project and Core Contracts

**Files:**
- Create: `SqlSchemaMcp.Abstractions/SqlSchemaMcp.Abstractions.csproj`
- Create: `SqlSchemaMcp.Abstractions/DatabaseEngine.cs`
- Create: `SqlSchemaMcp.Abstractions/DatabaseConfig.cs`
- Create: `SqlSchemaMcp.Abstractions/Sentinels.cs`
- Create: `SqlSchemaMcp.Abstractions/SchemaModels.cs`
- Create: `SqlSchemaMcp.Abstractions/Capabilities/IDatabaseEngine.cs`
- Create: `SqlSchemaMcp.Abstractions/Capabilities/ISchemaCapability.cs`
- Create: `SqlSchemaMcp.Abstractions/Capabilities/ISqlServerSchemaExtrasCapability.cs`
- Create: `SqlSchemaMcp.Abstractions/Capabilities/IReadOnlyQueryCapability.cs`
- Create: `SqlSchemaMcp.Abstractions/Capabilities/IDataSamplingCapability.cs`
- Create: `SqlSchemaMcp.Abstractions/Capabilities/ISchemaSnapshotCapability.cs`
- Create: `SqlSchemaMcp.Abstractions/Capabilities/ISqlServerDiagnosticsCapability.cs`
- Create: `SqlSchemaMcp.Abstractions/ICapabilityResolver.cs`
- Modify: `SqlSchemaMcp.sln`
- Modify: `SqlSchemaMcp.csproj`
- Modify: `SqlSchemaMcp.Tests/SqlSchemaMcp.Tests.csproj`
- Test: `SqlSchemaMcp.Tests/Abstractions/SentinelsTests.cs`

**Interfaces:**
- Produces: `DatabaseEngine`, `DatabaseConfig`, `Sentinels`, `SchemaSnapshot`, all capability interfaces, `ICapabilityResolver`.
- Consumes: none.

- [ ] **Step 1: Create the abstractions project**

Create `SqlSchemaMcp.Abstractions/SqlSchemaMcp.Abstractions.csproj`:

```xml
<Project Sdk="Microsoft.NET.Sdk">

  <PropertyGroup>
    <TargetFramework>net10.0</TargetFramework>
    <Nullable>enable</Nullable>
    <ImplicitUsings>enable</ImplicitUsings>
    <RootNamespace>SqlSchemaMcp.Abstractions</RootNamespace>
  </PropertyGroup>

</Project>
```

- [ ] **Step 2: Add engine and database config contracts**

Create `SqlSchemaMcp.Abstractions/DatabaseEngine.cs`:

```csharp
namespace SqlSchemaMcp.Abstractions;

public enum DatabaseEngine
{
    SqlServer,
    Postgres,
    MariaDb
}
```

Create `SqlSchemaMcp.Abstractions/DatabaseConfig.cs`:

```csharp
namespace SqlSchemaMcp.Abstractions;

public sealed record DatabaseConfig(
    string Name,
    DatabaseEngine Engine,
    string ConnectionString);
```

- [ ] **Step 3: Add sentinel helper**

Create `SqlSchemaMcp.Abstractions/Sentinels.cs`:

```csharp
namespace SqlSchemaMcp.Abstractions;

public static class Sentinels
{
    public static string UnknownDatabase(IEnumerable<string> availableNames, string database) =>
        $"ERROR: Unknown database '{database}'. Available: {string.Join(", ", availableNames)}";

    public static string Unsupported(string toolName, DatabaseEngine engine) =>
        $"UNSUPPORTED: Tool '{toolName}' is not available for engine '{engine}'. Ask the maintainer to add support if you need this.";
}
```

- [ ] **Step 4: Add shared schema models**

Create `SqlSchemaMcp.Abstractions/SchemaModels.cs`:

```csharp
namespace SqlSchemaMcp.Abstractions;

public enum ColumnTypeCategory
{
    Integer,
    Guid,
    Text,
    Boolean,
    Temporal,
    Decimal,
    Other
}

public sealed record SchemaObject(string Type, string Schema, string Name);

public sealed record SchemaColumn(
    string Schema,
    string Table,
    string Column,
    ColumnTypeCategory TypeCategory,
    string FormattedType,
    string Nullable);

public sealed record SchemaSnapshot(
    IReadOnlyList<SchemaObject> Objects,
    IReadOnlyList<SchemaColumn> Columns,
    IReadOnlySet<string> ForeignKeyColumnKeys,
    IReadOnlySet<string> PrimaryKeyColumnKeys,
    IReadOnlySet<string> IndexedColumnKeys);
```

- [ ] **Step 5: Add capability interfaces**

Create `SqlSchemaMcp.Abstractions/Capabilities/IDatabaseEngine.cs`:

```csharp
namespace SqlSchemaMcp.Abstractions.Capabilities;

public interface IDatabaseEngine
{
    DatabaseEngine Kind { get; }
}
```

Create `SqlSchemaMcp.Abstractions/Capabilities/ISchemaCapability.cs`:

```csharp
namespace SqlSchemaMcp.Abstractions.Capabilities;

public interface ISchemaCapability
{
    Task<string> ListTables(string database, string? schemaFilter, string? nameFilter, CancellationToken ct);
    Task<string> ListViews(string database, string? nameFilter, CancellationToken ct);
    Task<string> ListProcedures(string database, string? nameFilter, CancellationToken ct);
    Task<string> ListFunctions(string database, string? nameFilter, CancellationToken ct);
    Task<string> GetTableSchema(string database, string tableName, CancellationToken ct);
    Task<string> GetViewDefinition(string database, string viewName, CancellationToken ct);
    Task<string> GetProcedureDefinition(string database, string procName, CancellationToken ct);
    Task<string> GetFunctionDefinition(string database, string functionName, CancellationToken ct);
    Task<string> FindReferences(string database, string objectName, CancellationToken ct);
    Task<string> SearchDefinitions(string database, string keyword, CancellationToken ct);
}
```

Create `SqlSchemaMcp.Abstractions/Capabilities/ISqlServerSchemaExtrasCapability.cs`:

```csharp
namespace SqlSchemaMcp.Abstractions.Capabilities;

public interface ISqlServerSchemaExtrasCapability
{
    Task<string> ListTriggers(string database, string? nameFilter, CancellationToken ct);
    Task<string> GetTriggerDefinition(string database, string triggerName, CancellationToken ct);
    Task<string> ListSynonyms(string database, string? nameFilter, CancellationToken ct);
    Task<string> ListCheckConstraints(string database, string? nameFilter, CancellationToken ct);
    Task<string> ListDdlTriggers(string database, CancellationToken ct);
    Task<string> GetDdlTriggerDefinition(string database, string triggerName, CancellationToken ct);
}
```

Create `SqlSchemaMcp.Abstractions/Capabilities/IReadOnlyQueryCapability.cs`:

```csharp
namespace SqlSchemaMcp.Abstractions.Capabilities;

public interface IReadOnlyQueryCapability
{
    Task<string> ExecuteQuery(string database, string sql, CancellationToken ct);
}
```

Create `SqlSchemaMcp.Abstractions/Capabilities/IDataSamplingCapability.cs`:

```csharp
namespace SqlSchemaMcp.Abstractions.Capabilities;

public interface IDataSamplingCapability
{
    Task<string> SampleTableData(string database, string tableName, int rows, CancellationToken ct);
    Task<string> AnalyzeColumnDistribution(string database, string tableName, string columnName, CancellationToken ct);
    Task<string> FindNullableColumnsWithNoNulls(string database, string tableName, CancellationToken ct);
    Task<string> FindDuplicateRows(string database, string tableName, string columns, int top, CancellationToken ct);
}
```

Create `SqlSchemaMcp.Abstractions/Capabilities/ISchemaSnapshotCapability.cs`:

```csharp
namespace SqlSchemaMcp.Abstractions.Capabilities;

public interface ISchemaSnapshotCapability
{
    Task<SchemaSnapshot> GetSchemaSnapshot(string database, CancellationToken ct);
}
```

Create `SqlSchemaMcp.Abstractions/Capabilities/ISqlServerDiagnosticsCapability.cs`:

```csharp
namespace SqlSchemaMcp.Abstractions.Capabilities;

public interface ISqlServerDiagnosticsCapability
{
    Task<string> ListAgentJobs(string database, CancellationToken ct);
    Task<string> GetFailingJobs(string database, CancellationToken ct);
    Task<string> GetJobHistory(string database, string jobName, int maxRuns, CancellationToken ct);
    Task<string> AnalyzeTopExpensiveQueries(string database, int top, CancellationToken ct);
    Task<string> AnalyzeWaitStats(string database, CancellationToken ct);
    Task<string> ListLinkedServers(string database, CancellationToken ct);
    Task<string> FindLinkedServerUsage(string database, string? linkedServerName, CancellationToken ct);
    Task<string> ListServiceBroker(string database, CancellationToken ct);
    Task<string> ListClrAssemblies(string database, CancellationToken ct);
}
```

- [ ] **Step 6: Add resolver contract**

Create `SqlSchemaMcp.Abstractions/ICapabilityResolver.cs`:

```csharp
namespace SqlSchemaMcp.Abstractions;

public interface ICapabilityResolver
{
    IReadOnlyCollection<string> DatabaseNames { get; }
    IReadOnlyList<DatabaseConfig> Databases { get; }

    bool TryGetEngine(string database, out DatabaseEngine engine);

    bool TryResolve<TCapability>(
        string database,
        out DatabaseEngine engine,
        out TCapability? capability)
        where TCapability : class;
}
```

- [ ] **Step 7: Add sentinel tests**

Create `SqlSchemaMcp.Tests/Abstractions/SentinelsTests.cs`:

```csharp
using FluentAssertions;
using SqlSchemaMcp.Abstractions;
using Xunit;

namespace SqlSchemaMcp.Tests.Abstractions;

public sealed class SentinelsTests
{
    [Fact]
    public void UnknownDatabase_ListsAvailableNames()
    {
        var result = Sentinels.UnknownDatabase(["poc", "azure"], "reporting");

        result.Should().Be("ERROR: Unknown database 'reporting'. Available: poc, azure");
    }

    [Fact]
    public void Unsupported_NamesToolAndEngineAndMaintainerAction()
    {
        var result = Sentinels.Unsupported("AnalyzeWaitStats", DatabaseEngine.Postgres);

        result.Should().Be("UNSUPPORTED: Tool 'AnalyzeWaitStats' is not available for engine 'Postgres'. Ask the maintainer to add support if you need this.");
    }
}
```

- [ ] **Step 8: Add project references**

Run:

```powershell
dotnet sln SqlSchemaMcp.sln add SqlSchemaMcp.Abstractions/SqlSchemaMcp.Abstractions.csproj
dotnet add SqlSchemaMcp.csproj reference SqlSchemaMcp.Abstractions/SqlSchemaMcp.Abstractions.csproj
dotnet add SqlSchemaMcp.Tests/SqlSchemaMcp.Tests.csproj reference SqlSchemaMcp.Abstractions/SqlSchemaMcp.Abstractions.csproj
```

Expected: each command reports that the project/reference was added.

- [ ] **Step 9: Exclude nested abstraction sources from host compilation**

Modify `SqlSchemaMcp.csproj` so the compile removal group includes the new project:

```xml
  <ItemGroup>
    <Compile Remove="SqlSchemaMcp.Tests\**" />
    <Compile Remove="SqlSchemaMcp.Abstractions\**" />
  </ItemGroup>
```

- [ ] **Step 10: Run abstraction tests**

Run:

```powershell
dotnet test SqlSchemaMcp.sln --filter SentinelsTests
```

Expected: `Passed!`.

- [ ] **Step 11: Commit**

Run:

```powershell
git add SqlSchemaMcp.Abstractions SqlSchemaMcp.sln SqlSchemaMcp.csproj SqlSchemaMcp.Tests/SqlSchemaMcp.Tests.csproj SqlSchemaMcp.Tests/Abstractions/SentinelsTests.cs
git commit -m "Add capability abstraction contracts"
```

---

### Task 2: Add Backward-Compatible Database Config Loader

**Files:**
- Create: `Configuration/DatabaseConfigLoader.cs`
- Test: `SqlSchemaMcp.Tests/Configuration/DatabaseConfigLoaderTests.cs`

**Interfaces:**
- Consumes: `DatabaseConfig`, `DatabaseEngine`.
- Produces: `DatabaseConfigLoader.Load(IConfiguration configuration)`.

- [ ] **Step 1: Write config loader tests**

Create `SqlSchemaMcp.Tests/Configuration/DatabaseConfigLoaderTests.cs`:

```csharp
using FluentAssertions;
using Microsoft.Extensions.Configuration;
using SqlSchemaMcp.Abstractions;
using SqlSchemaMcp.Configuration;
using Xunit;

namespace SqlSchemaMcp.Tests.Configuration;

public sealed class DatabaseConfigLoaderTests
{
    private static IConfiguration Build(Dictionary<string, string?> values) =>
        new ConfigurationBuilder().AddInMemoryCollection(values).Build();

    [Fact]
    public void Load_BareString_ImpliesSqlServer()
    {
        var configuration = Build(new()
        {
            ["SqlServer:Databases:poc"] = "Server=x;Database=y;"
        });

        var result = DatabaseConfigLoader.Load(configuration);

        result.Should().ContainSingle();
        result[0].Should().Be(new DatabaseConfig("poc", DatabaseEngine.SqlServer, "Server=x;Database=y;"));
    }

    [Fact]
    public void Load_ObjectForm_UsesDeclaredEngine()
    {
        var configuration = Build(new()
        {
            ["SqlServer:Databases:reporting:Engine"] = "Postgres",
            ["SqlServer:Databases:reporting:ConnectionString"] = "Host=h;Database=d;"
        });

        var result = DatabaseConfigLoader.Load(configuration);

        result.Should().ContainSingle();
        result[0].Should().Be(new DatabaseConfig("reporting", DatabaseEngine.Postgres, "Host=h;Database=d;"));
    }

    [Fact]
    public void Load_MixedForms_LoadsAllDatabases()
    {
        var configuration = Build(new()
        {
            ["SqlServer:Databases:poc"] = "Server=x;",
            ["SqlServer:Databases:legacy:Engine"] = "MariaDb",
            ["SqlServer:Databases:legacy:ConnectionString"] = "Server=m;"
        });

        var result = DatabaseConfigLoader.Load(configuration);

        result.Should().BeEquivalentTo([
            new DatabaseConfig("poc", DatabaseEngine.SqlServer, "Server=x;"),
            new DatabaseConfig("legacy", DatabaseEngine.MariaDb, "Server=m;")
        ]);
    }

    [Fact]
    public void Load_ObjectFormMissingConnectionString_ThrowsClearError()
    {
        var configuration = Build(new()
        {
            ["SqlServer:Databases:bad:Engine"] = "Postgres"
        });

        var act = () => DatabaseConfigLoader.Load(configuration);

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("Database 'bad' declares an engine but no connection string.");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```powershell
dotnet test SqlSchemaMcp.sln --filter DatabaseConfigLoaderTests
```

Expected: fails because `DatabaseConfigLoader` does not exist.

- [ ] **Step 3: Implement config loader**

Create `Configuration/DatabaseConfigLoader.cs`:

```csharp
using Microsoft.Extensions.Configuration;
using SqlSchemaMcp.Abstractions;

namespace SqlSchemaMcp.Configuration;

public static class DatabaseConfigLoader
{
    public static IReadOnlyList<DatabaseConfig> Load(IConfiguration configuration)
    {
        var section = configuration.GetSection("SqlServer:Databases");
        var databases = new List<DatabaseConfig>();

        foreach (var child in section.GetChildren())
        {
            var bareValue = child.Value;
            if (!string.IsNullOrWhiteSpace(bareValue))
            {
                databases.Add(new DatabaseConfig(child.Key, DatabaseEngine.SqlServer, bareValue));
                continue;
            }

            var engineValue = child["Engine"];
            var connectionString = child["ConnectionString"];

            if (string.IsNullOrWhiteSpace(engineValue) && string.IsNullOrWhiteSpace(connectionString))
                continue;

            if (string.IsNullOrWhiteSpace(engineValue))
                throw new InvalidOperationException($"Database '{child.Key}' declares a connection string but no engine.");

            if (string.IsNullOrWhiteSpace(connectionString))
                throw new InvalidOperationException($"Database '{child.Key}' declares an engine but no connection string.");

            if (!Enum.TryParse<DatabaseEngine>(engineValue, ignoreCase: true, out var engine))
                throw new InvalidOperationException($"Database '{child.Key}' declares unsupported engine '{engineValue}'.");

            databases.Add(new DatabaseConfig(child.Key, engine, connectionString));
        }

        return databases;
    }
}
```

- [ ] **Step 4: Run config loader tests**

Run:

```powershell
dotnet test SqlSchemaMcp.sln --filter DatabaseConfigLoaderTests
```

Expected: `Passed!`.

- [ ] **Step 5: Commit**

Run:

```powershell
git add Configuration/DatabaseConfigLoader.cs SqlSchemaMcp.Tests/Configuration/DatabaseConfigLoaderTests.cs
git commit -m "Add mixed-form database config loader"
```

---

### Task 3: Add Capability Resolver

**Files:**
- Create: `Engines/CapabilityResolver.cs`
- Test: `SqlSchemaMcp.Tests/Engines/CapabilityResolverTests.cs`

**Interfaces:**
- Consumes: `DatabaseConfig`, `DatabaseEngine`, `ICapabilityResolver`.
- Produces: `CapabilityResolver`.

- [ ] **Step 1: Write resolver tests**

Create `SqlSchemaMcp.Tests/Engines/CapabilityResolverTests.cs`:

```csharp
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
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```powershell
dotnet test SqlSchemaMcp.sln --filter CapabilityResolverTests
```

Expected: fails because `CapabilityResolver` does not exist.

- [ ] **Step 3: Implement resolver**

Create `Engines/CapabilityResolver.cs`:

```csharp
using SqlSchemaMcp.Abstractions;

namespace SqlSchemaMcp.Engines;

public sealed class CapabilityResolver : ICapabilityResolver
{
    private readonly IReadOnlyDictionary<string, DatabaseConfig> _databasesByName;
    private readonly IReadOnlyDictionary<DatabaseEngine, object> _enginesByKind;

    public CapabilityResolver(
        IReadOnlyList<DatabaseConfig> databases,
        IReadOnlyDictionary<DatabaseEngine, object> enginesByKind)
    {
        Databases = databases;
        _databasesByName = databases.ToDictionary(d => d.Name, StringComparer.OrdinalIgnoreCase);
        _enginesByKind = enginesByKind;
        DatabaseNames = _databasesByName.Keys.ToArray();
    }

    public IReadOnlyCollection<string> DatabaseNames { get; }
    public IReadOnlyList<DatabaseConfig> Databases { get; }

    public bool TryGetEngine(string database, out DatabaseEngine engine)
    {
        if (_databasesByName.TryGetValue(database, out var config))
        {
            engine = config.Engine;
            return true;
        }

        engine = default;
        return false;
    }

    public bool TryResolve<TCapability>(
        string database,
        out DatabaseEngine engine,
        out TCapability? capability)
        where TCapability : class
    {
        capability = null;

        if (!TryGetEngine(database, out engine))
            return false;

        if (!_enginesByKind.TryGetValue(engine, out var implementation))
            return false;

        capability = implementation as TCapability;
        return capability is not null;
    }
}
```

- [ ] **Step 4: Run resolver tests**

Run:

```powershell
dotnet test SqlSchemaMcp.sln --filter CapabilityResolverTests
```

Expected: `Passed!`.

- [ ] **Step 5: Commit**

Run:

```powershell
git add Engines/CapabilityResolver.cs SqlSchemaMcp.Tests/Engines/CapabilityResolverTests.cs
git commit -m "Add capability resolver"
```

---

### Task 4: Mark UNSUPPORTED Results as Audit Failures

**Files:**
- Modify: `Auditing/FileAuditLog.cs`
- Modify: `Auditing/IAuditLog.cs`
- Test: `SqlSchemaMcp.Tests/Auditing/FileAuditLogTests.cs`

**Interfaces:**
- Consumes: existing `IAuditLog`.
- Produces: audit success logic that treats `ERROR:` and `UNSUPPORTED:` as failures.

- [ ] **Step 1: Add failing audit test**

Append this test inside `FileAuditLogTests`:

```csharp
[Fact]
public async Task Invoke_BodyReturnsUnsupportedString_RecordsFailure()
{
    var sut = new FileAuditLog(Options.Create(new AuditOptions { Enabled = true, Path = _path }));

    var result = await sut.Invoke(
        "AnalyzeWaitStats",
        "reporting",
        "",
        () => Task.FromResult("UNSUPPORTED: Tool 'AnalyzeWaitStats' is not available for engine 'Postgres'. Ask the maintainer to add support if you need this."));

    result.Should().StartWith("UNSUPPORTED:");

    var line = (await File.ReadAllLinesAsync(_path))[0];
    var entry = JsonSerializer.Deserialize<AuditEntry>(line, JsonSerializerOptions.Web);
    entry!.Success.Should().BeFalse();
}
```

- [ ] **Step 2: Run audit tests to verify failure**

Run:

```powershell
dotnet test SqlSchemaMcp.sln --filter FileAuditLogTests
```

Expected: the new test fails because `UNSUPPORTED:` is currently logged as success.

- [ ] **Step 3: Update success logic**

In `Auditing/FileAuditLog.cs`, replace:

```csharp
success = !result.StartsWith("ERROR:", StringComparison.Ordinal);
```

with:

```csharp
success = !result.StartsWith("ERROR:", StringComparison.Ordinal)
    && !result.StartsWith("UNSUPPORTED:", StringComparison.Ordinal);
```

- [ ] **Step 4: Update audit comment**

In `Auditing/IAuditLog.cs`, replace the final summary sentence with:

```csharp
/// Success reflects both the absence of a thrown exception and that the result does not
/// begin with the ERROR: or UNSUPPORTED: sentinel used by this codebase's Result-as-string convention.
```

- [ ] **Step 5: Run audit tests**

Run:

```powershell
dotnet test SqlSchemaMcp.sln --filter FileAuditLogTests
```

Expected: `Passed!`.

- [ ] **Step 6: Commit**

Run:

```powershell
git add Auditing/FileAuditLog.cs Auditing/IAuditLog.cs SqlSchemaMcp.Tests/Auditing/FileAuditLogTests.cs
git commit -m "Record unsupported tool results as audit failures"
```

---

### Task 5: Wire Nothing Yet, Verify Foundation Integrity

**Files:**
- Verify: `Tools/*.cs`
- Verify: `Data/*.cs`
- Verify: `SqlSchemaMcp.sln`

**Interfaces:**
- Consumes: Tasks 1-4.
- Produces: a green, behavior-preserving foundation checkpoint.

- [ ] **Step 1: Verify tool layer unchanged**

Run:

```powershell
git diff -- Tools/
```

Expected: no output.

- [ ] **Step 2: Verify existing SQL Server data implementation remains in place**

Run:

```powershell
git diff -- Data/
```

Expected: no output.

- [ ] **Step 3: Run full test suite**

Run:

```powershell
dotnet test SqlSchemaMcp.sln
```

Expected: all tests pass. The original 35 tests still pass, plus the new foundation tests.

- [ ] **Step 4: Build the solution**

Run:

```powershell
dotnet build SqlSchemaMcp.sln
```

Expected: `Build succeeded`.

- [ ] **Step 5: Commit any missed project metadata**

If `git status --short` shows only expected solution/project metadata from this plan, run:

```powershell
git add SqlSchemaMcp.sln SqlSchemaMcp.csproj SqlSchemaMcp.Tests/SqlSchemaMcp.Tests.csproj
git commit -m "Verify capability foundation wiring"
```

If there are no remaining expected changes, skip this commit.

---

## Self-Review

**Spec coverage:** This plan implements the approved spec's abstraction contracts, sentinel behavior, mixed config loader, capability resolver, and unsupported audit semantics. It intentionally does not migrate SQL Server query implementations, dispatchers, or shared analyzers; those belong in the next plan after this safe checkpoint is green.

**Placeholder scan:** No TBD/TODO placeholders. Every created file has concrete code. Every test command has expected output.

**Type consistency:** `DatabaseEngine`, `DatabaseConfig`, `Sentinels`, `ICapabilityResolver`, and capability interface names match the approved design spec. The resolver stores engine implementations as `object` so future SQL Server/Postgres/MariaDB engine classes can implement multiple small capability interfaces without a shared god interface.
