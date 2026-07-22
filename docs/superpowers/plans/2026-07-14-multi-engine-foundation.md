# Multi-Engine Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Introduce a database-engine abstraction, shared schema models, and a new config shape, then migrate 100% of the existing SQL Server logic behind that abstraction with zero behaviour change — so PostgreSQL and MariaDB can be added later as drop-in engine projects.

**Architecture:** A new `SqlSchemaMcp.Abstractions` class library defines `DatabaseEngine`, the shared schema-model records, `IDbEngine`, `IPermissionProbe`, and `IEngineResolver`. A new `SqlSchemaMcp.SqlServer` class library holds the migrated SQL Server implementation (all current `Data/*Queries.cs` logic, `SqlStatementValidator`, `SqlServerPermissionProbe`). The host project keeps `Tools/*.cs` completely unchanged and turns each `Data/*Queries.cs` class into a thin **dispatcher** that resolves which engine backs a `database` name and delegates to it. Cross-engine analysis and compare are rewritten once to consume the shared `SchemaSnapshot` model instead of raw `sys.*` SQL.

**Tech Stack:** .NET 10, C# 13, `Microsoft.Data.SqlClient`, `Microsoft.SqlServer.TransactSql.ScriptDom`, `ModelContextProtocol`, xUnit + FluentAssertions + NSubstitute.

## Global Constraints

- .NET 10, C# 13, file-scoped namespaces, primary constructors (`class Foo(IBar bar)`), `CancellationToken` on every async method, structured logging only
- xUnit + FluentAssertions + NSubstitute — no MSTest, no Moq
- No comments except one-line non-obvious WHY — never multi-line comment blocks, never explain WHAT
- Plain ASCII text output (headers, dashes, aligned columns) — no JSON, no markdown, matching the existing `SchemaQueries`/`AnalysisQueries` formatting style
- Result-as-string convention: methods return `"ERROR: ..."` for expected failures, never throw for expected failures; new `"UNSUPPORTED: ..."` sentinel as specified below
- Every existing test in `SqlSchemaMcp.Tests` must still pass after this plan, unchanged, with zero modification to their assertions (only namespace/project-reference updates if a type moved) — this is the regression gate proving zero behaviour change
- `Tools/*.cs` files require zero code changes across this plan and the two engine plans that follow

## Naming and sentinel contract (used by all three plans)

- Unknown database: `ERROR: Unknown database '{database}'. Available: {comma-separated names}` (unchanged wording from today's `SqlQueryBase.UnknownDatabase`)
- Unsupported tool for an engine: `UNSUPPORTED: {ToolName} is not implemented for engine '{engine}' yet. Tell the maintainer if you need it.`
- Generic failure (unchanged wording from today's `SqlQueryBase.SafeError`): `ERROR: the query failed. Check the server log for details.`

## Design decisions (locked — do not re-litigate during execution)

1. **Engines are config-holding, not stateless.** Each engine keeps its own `Dictionary<string,string>` of the databases it owns (populated from a per-engine options object), so the migrated SQL Server query methods keep their exact `(string database, ...args, CancellationToken ct)` signatures and their internal `_databases.TryGetValue` lookup. This is what makes the migration a near-verbatim move.
2. **Interface methods carry `database` (a display name), never a connection string** — except `ProbePermissionsAsync`, which the startup gate calls before the DI graph is fully exercised and which therefore receives the connection string explicitly.
3. **`SqlServerOptions` keeps its shape** (`Dictionary<string,string> Databases`) so `SafeErrorTests` (which does `new SqlServerOptions()`) needs only a namespace-update. It moves to `SqlSchemaMcp.Abstractions` to break the host↔engine reference cycle.
4. **The audit `UNSUPPORTED:` result is recorded as `Success = false`** (it did not perform the requested operation) but is deliberately *not* treated as an `ERROR:`; the only code change is adding the `UNSUPPORTED:` prefix to `FileAuditLog`'s success check so an unsupported call is not falsely logged as success. No new `AuditOutcome` type — YAGNI for a PoC; the boolean already answers "did the tool do the thing?".
5. **Cross-engine tools:** Schema browsing, data sampling, `execute_query`, the three shared analyses (naming / missing FK / missing index), and all Compare operations. These route through `IDbEngine`.
6. **SQL-Server-only tools:** everything in `DiagnosticsTools`, `PipelineTools`, `SecurityTools`, and every `AnalysisTools` method except the three shared analyses. Their dispatchers return the `UNSUPPORTED` sentinel for any non-SqlServer engine.

## File Structure

Rationale for the project split (per writing-plans "map files first"):

- `SqlSchemaMcp.Abstractions` (classlib, **no ADO.NET package refs**) — the contract every engine and the host share. Pure types + interfaces so engines never reference the host and the host never references a concrete engine's ADO.NET types.
- `SqlSchemaMcp.SqlServer` (classlib, refs `Microsoft.Data.SqlClient` + `Microsoft.SqlServer.TransactSql.ScriptDom` + Abstractions) — the migrated SQL Server engine, moved out of the host so the host has no direct `SqlConnection` dependency in its dispatchers.
- `SqlSchemaMcp` (host, `Sdk.Web`, refs Abstractions + SqlServer) — `Tools/*.cs` (unchanged), `Program.cs`, DI, the config loader, the dispatcher `Data/*Queries.cs` classes, the shared analyzers, `EngineResolver`, `Auditing/`, `Security/ReadOnlyStartupGate.cs`.
- **Test project structure:** keep the single `SqlSchemaMcp.Tests` project for host + SqlServer + Abstractions unit tests. Rationale: the current 35 tests are pure in-process unit tests with no live database, and the SqlServer engine's unit-testable surface (validator, SafeError, permission-gate) has no container dependency. When Plans 2/3 add `Testcontainers.PostgreSql` / `Testcontainers.MariaDb` integration tests, those get their **own** test projects (`SqlSchemaMcp.Postgres.Tests`, `SqlSchemaMcp.MariaDb.Tests`) so the heavy container dependencies never load into the fast host test project.

Files created/modified in this plan are listed per task.

---

### Task 1: Abstractions project — enum, models, interfaces

**Files:**
- Create: `SqlSchemaMcp.Abstractions/SqlSchemaMcp.Abstractions.csproj`
- Create: `SqlSchemaMcp.Abstractions/DatabaseEngine.cs`
- Create: `SqlSchemaMcp.Abstractions/SchemaModels.cs`
- Create: `SqlSchemaMcp.Abstractions/Sentinels.cs`
- Create: `SqlSchemaMcp.Abstractions/IPermissionProbe.cs`
- Create: `SqlSchemaMcp.Abstractions/IDbEngine.cs`
- Create: `SqlSchemaMcp.Abstractions/IEngineResolver.cs`
- Create: `SqlSchemaMcp.Abstractions/SqlServerOptions.cs` (moved from `Configuration/SqlServerOptions.cs`)
- Modify: `SqlSchemaMcp.sln` (add project)
- Test: `SqlSchemaMcp.Tests/Abstractions/SentinelsTests.cs`

**Interfaces:**
- Produces (consumed by every later task and by Plans 2 & 3 verbatim): the `DatabaseEngine` enum, the model records, `Sentinels`, `IPermissionProbe`, `IDbEngine`, `IEngineResolver`, and `SqlServerOptions`.

- [ ] **Step 1: Create the project file**

`SqlSchemaMcp.Abstractions/SqlSchemaMcp.Abstractions.csproj`:

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

- [ ] **Step 2: Write the enum and column-type category**

`SqlSchemaMcp.Abstractions/DatabaseEngine.cs`:

```csharp
namespace SqlSchemaMcp.Abstractions;

public enum DatabaseEngine
{
    SqlServer,
    Postgres,
    MariaDb
}

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
```

- [ ] **Step 3: Write the shared model records**

`SqlSchemaMcp.Abstractions/SchemaModels.cs`:

```csharp
using System.Collections.Generic;

namespace SqlSchemaMcp.Abstractions;

public sealed record DatabaseConfig(string Name, DatabaseEngine Engine, string ConnectionString);

public sealed record SchemaObject(string Type, string Schema, string Name);

public sealed record SchemaColumn(string Schema, string Table, string Column, ColumnTypeCategory TypeCategory);

public sealed record SchemaSnapshot(
    IReadOnlyList<SchemaObject> Objects,
    IReadOnlyList<SchemaColumn> Columns,
    IReadOnlySet<string> ForeignKeyColumnKeys,
    IReadOnlySet<string> PrimaryKeyColumnKeys,
    IReadOnlySet<string> IndexedColumnKeys);

public sealed record DbColumn(string Name, string Type, string Nullable);

public sealed record RoutineStats(int LineCount, IReadOnlyList<string> TablesReferenced);
```

Note for implementers: `SchemaObject.Type` is one of the literals `"TABLE"`, `"VIEW"`, `"PROCEDURE"`. The three `*ColumnKeys` sets contain keys formatted `"{schema}.{table}.{column}"` and must use `StringComparer.OrdinalIgnoreCase`. `DbColumn.Nullable` is the literal `"YES"` or `"NO"` (preserves the exact Compare output). `DbColumn.Type` is the already-formatted type string (e.g. `nvarchar(255)`).

- [ ] **Step 4: Write the sentinel helper**

`SqlSchemaMcp.Abstractions/Sentinels.cs`:

```csharp
using System.Collections.Generic;

namespace SqlSchemaMcp.Abstractions;

public static class Sentinels
{
    public static string UnknownDatabase(IEnumerable<string> availableNames, string database) =>
        $"ERROR: Unknown database '{database}'. Available: {string.Join(", ", availableNames)}";

    public static string Unsupported(string toolName, DatabaseEngine engine) =>
        $"UNSUPPORTED: {toolName} is not implemented for engine '{engine}' yet. Tell the maintainer if you need it.";
}
```

- [ ] **Step 5: Move `IPermissionProbe` + `LoginPermissionResult` into Abstractions**

`SqlSchemaMcp.Abstractions/IPermissionProbe.cs` (content moved verbatim from `Security/IPermissionProbe.cs`, only the namespace changes):

```csharp
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace SqlSchemaMcp.Abstractions;

public sealed record LoginPermissionResult(
    string Database,
    bool Reachable,
    bool CanWrite,
    IReadOnlyList<string> GrantedWrites);

public interface IPermissionProbe
{
    Task<LoginPermissionResult> ProbeAsync(string database, string connectionString, CancellationToken ct);
}
```

Delete `Security/IPermissionProbe.cs` from the host project.

- [ ] **Step 6: Move `SqlServerOptions` into Abstractions**

`SqlSchemaMcp.Abstractions/SqlServerOptions.cs` (moved from `Configuration/SqlServerOptions.cs`, namespace changes to `SqlSchemaMcp.Abstractions`):

```csharp
using System;
using System.Collections.Generic;

namespace SqlSchemaMcp.Abstractions;

public sealed class SqlServerOptions
{
    public Dictionary<string, string> Databases { get; init; } = new(StringComparer.OrdinalIgnoreCase);
}
```

Delete `Configuration/SqlServerOptions.cs`.

- [ ] **Step 7: Write `IDbEngine`**

`SqlSchemaMcp.Abstractions/IDbEngine.cs`:

```csharp
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace SqlSchemaMcp.Abstractions;

public interface IDbEngine
{
    DatabaseEngine Engine { get; }

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
    Task<string> ListTriggers(string database, string? nameFilter, CancellationToken ct);
    Task<string> GetTriggerDefinition(string database, string triggerName, CancellationToken ct);
    Task<string> ListSynonyms(string database, string? nameFilter, CancellationToken ct);
    Task<string> ListCheckConstraints(string database, string? nameFilter, CancellationToken ct);
    Task<string> ListDdlTriggers(string database, CancellationToken ct);
    Task<string> GetDdlTriggerDefinition(string database, string triggerName, CancellationToken ct);

    Task<string> SampleTableData(string database, string tableName, int rows, CancellationToken ct);
    Task<string> AnalyzeColumnDistribution(string database, string tableName, string columnName, CancellationToken ct);
    Task<string> FindNullableColumnsWithNoNulls(string database, string tableName, CancellationToken ct);
    Task<string> FindDuplicateRows(string database, string tableName, string columns, int top, CancellationToken ct);

    Task<string> ExecuteQuery(string database, string sql, CancellationToken ct);

    Task<SchemaSnapshot> GetSchemaSnapshot(string database, CancellationToken ct);
    Task<IReadOnlyCollection<string>> GetTableNames(string database, CancellationToken ct);
    Task<IReadOnlyCollection<string>> GetViewNames(string database, CancellationToken ct);
    Task<IReadOnlyCollection<string>> GetProcedureNames(string database, CancellationToken ct);
    Task<IReadOnlyList<DbColumn>> GetTableColumns(string database, string tableName, CancellationToken ct);
    Task<RoutineStats> GetProcedureStats(string database, string procName, CancellationToken ct);
    Task<RoutineStats> GetViewStats(string database, string viewName, CancellationToken ct);

    Task<LoginPermissionResult> ProbePermissionsAsync(string database, string connectionString, CancellationToken ct);
}
```

- [ ] **Step 8: Write `IEngineResolver`**

`SqlSchemaMcp.Abstractions/IEngineResolver.cs`:

```csharp
using System.Collections.Generic;

namespace SqlSchemaMcp.Abstractions;

public interface IEngineResolver
{
    IReadOnlyCollection<string> DatabaseNames { get; }
    IReadOnlyList<DatabaseConfig> Databases { get; }
    bool TryResolve(string database, out IDbEngine engine);
    bool TryGetKind(string database, out DatabaseEngine kind);
}
```

- [ ] **Step 9: Add the project to the solution**

Run: `dotnet sln SqlSchemaMcp.sln add SqlSchemaMcp.Abstractions/SqlSchemaMcp.Abstractions.csproj`
Expected: `Project ... added to the solution.`

- [ ] **Step 10: Write the failing test for `Sentinels`**

`SqlSchemaMcp.Tests/Abstractions/SentinelsTests.cs`:

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
        var result = Sentinels.UnknownDatabase(["poc", "azure"], "nope");

        result.Should().StartWith("ERROR:");
        result.Should().Contain("nope").And.Contain("poc").And.Contain("azure");
    }

    [Fact]
    public void Unsupported_NamesToolAndEngine()
    {
        var result = Sentinels.Unsupported("ListAgentJobs", DatabaseEngine.Postgres);

        result.Should().StartWith("UNSUPPORTED:");
        result.Should().Contain("ListAgentJobs").And.Contain("Postgres");
    }
}
```

- [ ] **Step 11: Add the Abstractions reference to the test project and build**

Run: `dotnet sln` is unaffected; add the reference:
`dotnet add SqlSchemaMcp.Tests/SqlSchemaMcp.Tests.csproj reference SqlSchemaMcp.Abstractions/SqlSchemaMcp.Abstractions.csproj`
Then: `dotnet build SqlSchemaMcp.Abstractions/SqlSchemaMcp.Abstractions.csproj`
Expected: `Build succeeded`. (The host project will not build yet — that is fixed in Task 3+; run `dotnet test --filter SentinelsTests` only after Task 11.)

- [ ] **Step 12: Commit**

```bash
git add SqlSchemaMcp.Abstractions SqlSchemaMcp.sln SqlSchemaMcp.Tests
git commit -m "Add SqlSchemaMcp.Abstractions: engine enum, schema models, IDbEngine, IEngineResolver"
```

---

### Task 2: Config loader for the mixed string/object shape

**Files:**
- Create: `Configuration/DatabaseConfigLoader.cs`
- Test: `SqlSchemaMcp.Tests/Configuration/DatabaseConfigLoaderTests.cs`

**Interfaces:**
- Consumes: `DatabaseConfig`, `DatabaseEngine` (Task 1)
- Produces: `DatabaseConfigLoader.Load(IConfiguration) -> IReadOnlyList<DatabaseConfig>`, consumed by `Program.cs` (Task 11)

- [ ] **Step 1: Write the failing test**

`SqlSchemaMcp.Tests/Configuration/DatabaseConfigLoaderTests.cs`:

```csharp
using System.Collections.Generic;
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
        var config = Build(new() { ["SqlServer:Databases:poc"] = "Server=x;Database=y;" });

        var result = DatabaseConfigLoader.Load(config);

        result.Should().ContainSingle();
        result[0].Name.Should().Be("poc");
        result[0].Engine.Should().Be(DatabaseEngine.SqlServer);
        result[0].ConnectionString.Should().Be("Server=x;Database=y;");
    }

    [Fact]
    public void Load_ObjectForm_UsesDeclaredEngine()
    {
        var config = Build(new()
        {
            ["SqlServer:Databases:pg:Engine"] = "Postgres",
            ["SqlServer:Databases:pg:ConnectionString"] = "Host=h;Database=d;",
        });

        var result = DatabaseConfigLoader.Load(config);

        result.Should().ContainSingle();
        result[0].Name.Should().Be("pg");
        result[0].Engine.Should().Be(DatabaseEngine.Postgres);
        result[0].ConnectionString.Should().Be("Host=h;Database=d;");
    }

    [Fact]
    public void Load_MixedForms_LoadsBoth()
    {
        var config = Build(new()
        {
            ["SqlServer:Databases:poc"] = "Server=x;",
            ["SqlServer:Databases:maria:Engine"] = "MariaDb",
            ["SqlServer:Databases:maria:ConnectionString"] = "Server=m;",
        });

        var result = DatabaseConfigLoader.Load(config);

        result.Should().HaveCount(2);
        result.Should().Contain(d => d.Name == "poc" && d.Engine == DatabaseEngine.SqlServer);
        result.Should().Contain(d => d.Name == "maria" && d.Engine == DatabaseEngine.MariaDb);
    }

    [Fact]
    public void Load_ObjectFormMissingConnectionString_Throws()
    {
        var config = Build(new() { ["SqlServer:Databases:bad:Engine"] = "Postgres" });

        var act = () => DatabaseConfigLoader.Load(config);

        act.Should().Throw<System.InvalidOperationException>().WithMessage("*ConnectionString*");
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `dotnet test SqlSchemaMcp.sln --filter DatabaseConfigLoaderTests`
Expected: FAIL — `DatabaseConfigLoader` does not exist.

- [ ] **Step 3: Write the loader (verbatim — this is the tricky manual binding)**

`Configuration/DatabaseConfigLoader.cs`:

```csharp
using System;
using System.Collections.Generic;
using Microsoft.Extensions.Configuration;
using SqlSchemaMcp.Abstractions;

namespace SqlSchemaMcp.Configuration;

public static class DatabaseConfigLoader
{
    public static IReadOnlyList<DatabaseConfig> Load(IConfiguration configuration)
    {
        var result = new List<DatabaseConfig>();
        var section = configuration.GetSection("SqlServer:Databases");

        foreach (var child in section.GetChildren())
        {
            string name = child.Key;

            // Leaf/bare-string form: "poc": "Server=..." — implies Engine=SqlServer for backward compatibility.
            if (child.Value is not null)
            {
                result.Add(new DatabaseConfig(name, DatabaseEngine.SqlServer, child.Value));
                continue;
            }

            // Object form: { "Engine": "Postgres", "ConnectionString": "..." }
            string? engineText = child["Engine"];
            string? connectionString = child["ConnectionString"];

            if (string.IsNullOrWhiteSpace(connectionString))
                throw new InvalidOperationException(
                    $"Database '{name}' is configured as an object but has no ConnectionString.");

            result.Add(new DatabaseConfig(name, ParseEngine(engineText, name), connectionString));
        }

        return result;
    }

    private static DatabaseEngine ParseEngine(string? text, string database)
    {
        if (string.IsNullOrWhiteSpace(text))
            return DatabaseEngine.SqlServer;

        return text.Trim().ToLowerInvariant() switch
        {
            "sqlserver" or "mssql" or "sql" => DatabaseEngine.SqlServer,
            "postgres" or "postgresql" or "pg" => DatabaseEngine.Postgres,
            "mariadb" or "mysql" => DatabaseEngine.MariaDb,
            _ => throw new InvalidOperationException(
                $"Database '{database}' has unknown Engine '{text}'. Valid: SqlServer, Postgres, MariaDb.")
        };
    }
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `dotnet test SqlSchemaMcp.sln --filter DatabaseConfigLoaderTests`
Expected: PASS (4 tests). If the host project does not yet compile, this task's file can be validated after Task 11; sequence the commit now and let the full green come at Task 12.

- [ ] **Step 5: Commit**

```bash
git add Configuration/DatabaseConfigLoader.cs SqlSchemaMcp.Tests/Configuration
git commit -m "Add DatabaseConfigLoader: manual binding for mixed string/object database config"
```

---

### Task 3: Create the SqlServer project and move the shared SQL Server primitives

**Files:**
- Create: `SqlSchemaMcp.SqlServer/SqlSchemaMcp.SqlServer.csproj`
- Move: `Data/SqlQueryBase.cs` → `SqlSchemaMcp.SqlServer/SqlQueryBase.cs` (namespace → `SqlSchemaMcp.SqlServer`)
- Move: `Data/SqlStatementValidator.cs` → `SqlSchemaMcp.SqlServer/SqlStatementValidator.cs` (namespace → `SqlSchemaMcp.SqlServer`)
- Move: `Security/SqlServerPermissionProbe.cs` → `SqlSchemaMcp.SqlServer/SqlServerPermissionProbe.cs` (namespace → `SqlSchemaMcp.SqlServer`)
- Modify: `SqlSchemaMcp.Tests/Data/SqlStatementValidatorTests.cs` (using update)
- Modify: `SqlSchemaMcp.Tests/Data/SafeErrorTests.cs` (using update)
- Modify: `SqlSchemaMcp.Tests/SqlSchemaMcp.Tests.csproj` (add SqlServer + Abstractions project refs)
- Modify: `SqlSchemaMcp.sln`

**Interfaces:**
- Consumes: `SqlServerOptions`, `IPermissionProbe`, `LoginPermissionResult` (Task 1)
- Produces: `SqlQueryBase` (base for all migrated SQL Server query classes), `SqlStatementValidator`, `SqlServerPermissionProbe` — all in namespace `SqlSchemaMcp.SqlServer`

- [ ] **Step 1: Create the project file**

`SqlSchemaMcp.SqlServer/SqlSchemaMcp.SqlServer.csproj`:

```xml
<Project Sdk="Microsoft.NET.Sdk">

  <PropertyGroup>
    <TargetFramework>net10.0</TargetFramework>
    <Nullable>enable</Nullable>
    <ImplicitUsings>enable</ImplicitUsings>
    <RootNamespace>SqlSchemaMcp.SqlServer</RootNamespace>
  </PropertyGroup>

  <ItemGroup>
    <PackageReference Include="Microsoft.SqlServer.TransactSql.ScriptDom" Version="180.37.3" />
    <PackageReference Include="Microsoft.Data.SqlClient" Version="7.0.0" />
    <PackageReference Include="Microsoft.Extensions.Logging.Abstractions" Version="10.0.0" />
    <PackageReference Include="Microsoft.Extensions.Options" Version="10.0.0" />
  </ItemGroup>

  <ItemGroup>
    <ProjectReference Include="..\SqlSchemaMcp.Abstractions\SqlSchemaMcp.Abstractions.csproj" />
  </ItemGroup>

</Project>
```

- [ ] **Step 2: Move `SqlQueryBase` verbatim, changing only the namespace**

Move `Data/SqlQueryBase.cs` to `SqlSchemaMcp.SqlServer/SqlQueryBase.cs`. Apply exactly these edits to the moved file:
- Change `namespace SqlSchemaMcp.Data;` → `namespace SqlSchemaMcp.SqlServer;`
- Add `using SqlSchemaMcp.Abstractions;` (for `SqlServerOptions`), keeping System usings first.
- Leave every member (`_databases`, `SafeError`, `UnknownDatabase`, `ParseSchemaTable`, `TableExists`, `FormatColumnType`, `BoolFlag`, `FormatKb`, `StagingExcludeLike`, `StagingRegex`, `IsStaging`) unchanged.

- [ ] **Step 3: Move `SqlStatementValidator` verbatim, changing only the namespace**

Move `Data/SqlStatementValidator.cs` to `SqlSchemaMcp.SqlServer/SqlStatementValidator.cs`. Change `namespace SqlSchemaMcp.Data;` → `namespace SqlSchemaMcp.SqlServer;`. Nothing else changes.

- [ ] **Step 4: Move `SqlServerPermissionProbe` verbatim, changing only namespace + implemented-interface origin**

Move `Security/SqlServerPermissionProbe.cs` to `SqlSchemaMcp.SqlServer/SqlServerPermissionProbe.cs`. Change `namespace SqlSchemaMcp.Security;` → `namespace SqlSchemaMcp.SqlServer;`, add `using SqlSchemaMcp.Abstractions;` (for `IPermissionProbe` and `LoginPermissionResult`). The class body is unchanged.

- [ ] **Step 5: Add the project to the solution and wire references**

```bash
dotnet sln SqlSchemaMcp.sln add SqlSchemaMcp.SqlServer/SqlSchemaMcp.SqlServer.csproj
dotnet add SqlSchemaMcp.Tests/SqlSchemaMcp.Tests.csproj reference SqlSchemaMcp.SqlServer/SqlSchemaMcp.SqlServer.csproj
```

The test project now references Abstractions (Task 1) and SqlServer.

- [ ] **Step 6: Update the two affected test usings (namespace-only changes)**

In `SqlSchemaMcp.Tests/Data/SqlStatementValidatorTests.cs`: change `using SqlSchemaMcp.Data;` → `using SqlSchemaMcp.SqlServer;`. Assertions unchanged.

In `SqlSchemaMcp.Tests/Data/SafeErrorTests.cs`: change `using SqlSchemaMcp.Configuration;` → `using SqlSchemaMcp.Abstractions;` and `using SqlSchemaMcp.Data;` → `using SqlSchemaMcp.SqlServer;`. The class body (`TestQueries : SqlQueryBase(options, NullLogger<TestQueries>.Instance)`, the assertions) is unchanged.

- [ ] **Step 7: Build the SqlServer project**

Run: `dotnet build SqlSchemaMcp.SqlServer/SqlSchemaMcp.SqlServer.csproj`
Expected: `Build succeeded`.

- [ ] **Step 8: Commit**

```bash
git add SqlSchemaMcp.SqlServer SqlSchemaMcp.sln SqlSchemaMcp.Tests
git commit -m "Move SqlQueryBase, SqlStatementValidator, SqlServerPermissionProbe into SqlSchemaMcp.SqlServer"
```

---

### Task 4: Migrate SQL Server schema/data/query classes into the engine

**Files:**
- Move: `Data/SchemaQueries.cs` → `SqlSchemaMcp.SqlServer/SqlServerSchema.cs` (rename class `SchemaQueries` → `SqlServerSchema`)
- Move: `Data/DataQueries.cs` → `SqlSchemaMcp.SqlServer/SqlServerData.cs` (rename class `DataQueries` → `SqlServerData`)
- Move: `Data/QueryQueries.cs` → `SqlSchemaMcp.SqlServer/SqlServerQuery.cs` (rename class `QueryQueries` → `SqlServerQuery`)
- Create: `SqlSchemaMcp.SqlServer/SqlServerSchemaModels.cs`
- Create: `SqlSchemaMcp.SqlServer/SqlServerEngine.cs`

**Interfaces:**
- Consumes: `SqlQueryBase`, `SqlStatementValidator` (Task 3); `IDbEngine`, `SchemaSnapshot`, `DbColumn`, `RoutineStats`, `ColumnTypeCategory`, `LoginPermissionResult` (Task 1)
- Produces: `SqlServerEngine : IDbEngine` — the concrete SQL Server engine, later registered in DI (Task 11)

- [ ] **Step 1: Move the three query classes with mechanical edits only**

For each of `SchemaQueries`, `DataQueries`, `QueryQueries`, apply exactly:
- Move the file into `SqlSchemaMcp.SqlServer/` with the new filename above.
- Change `namespace SqlSchemaMcp.Data;` → `namespace SqlSchemaMcp.SqlServer;`.
- Rename the class (`SchemaQueries` → `SqlServerSchema`, `DataQueries` → `SqlServerData`, `QueryQueries` → `SqlServerQuery`), including in the `ILogger<...>` generic and the base-constructor call.
- Add `using SqlSchemaMcp.Abstractions;` (for `SqlServerOptions`).
- Every method body, SQL string, and output format stays **byte-for-byte identical**. Method signatures stay exactly `(string database, ...args, CancellationToken cancellationToken = default)`.

`SqlServerQuery` keeps its `using` of the (now-relocated) `SqlStatementValidator` automatically because it is in the same namespace.

- [ ] **Step 2: Write the schema-model provider (new code)**

`SqlSchemaMcp.SqlServer/SqlServerSchemaModels.cs`. This is the only genuinely new SQL Server code; it feeds the shared analyzers and Compare. The SQL reuses the exact filters from today's `AnalysisQueries`/`CompareQueries` so the resulting analyses are byte-identical.

```csharp
using System.Collections.Generic;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SqlSchemaMcp.Abstractions;

namespace SqlSchemaMcp.SqlServer;

public sealed class SqlServerSchemaModels(IOptions<SqlServerOptions> options, ILogger<SqlServerSchemaModels> logger)
    : SqlQueryBase(options, logger)
{
    public async Task<SchemaSnapshot> GetSchemaSnapshot(string database, CancellationToken cancellationToken)
    {
        if (!_databases.TryGetValue(database, out var connectionString))
            return new SchemaSnapshot([], [], new HashSet<string>(), new HashSet<string>(), new HashSet<string>());

        const string objectSql = """
            SELECT 'TABLE' AS ObjectType, TABLE_SCHEMA AS SchemaName, TABLE_NAME AS ObjectName
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME NOT LIKE @stagingPattern
            UNION ALL
            SELECT 'VIEW', TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS
            UNION ALL
            SELECT 'PROCEDURE', ROUTINE_SCHEMA, ROUTINE_NAME
            FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_TYPE = 'PROCEDURE'
            ORDER BY ObjectType, ObjectName
            """;

        const string columnSql = """
            SELECT c.TABLE_SCHEMA, c.TABLE_NAME, c.COLUMN_NAME, c.DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS c
            JOIN INFORMATION_SCHEMA.TABLES t
                ON t.TABLE_NAME = c.TABLE_NAME AND t.TABLE_SCHEMA = c.TABLE_SCHEMA
            WHERE t.TABLE_TYPE = 'BASE TABLE' AND c.TABLE_NAME NOT LIKE @stagingPattern
            ORDER BY c.TABLE_SCHEMA, c.TABLE_NAME, c.ORDINAL_POSITION
            """;

        const string fkSql = """
            SELECT OBJECT_SCHEMA_NAME(fk.parent_object_id), OBJECT_NAME(fk.parent_object_id),
                   COL_NAME(fkc.parent_object_id, fkc.parent_column_id)
            FROM sys.foreign_keys fk
            JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id
            """;

        const string pkSql = """
            SELECT tc.TABLE_SCHEMA, tc.TABLE_NAME, ku.COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku
                ON ku.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
                AND ku.TABLE_SCHEMA = tc.TABLE_SCHEMA AND ku.TABLE_NAME = tc.TABLE_NAME
            WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
            """;

        const string indexSql = """
            SELECT OBJECT_SCHEMA_NAME(ic.object_id), OBJECT_NAME(ic.object_id), c.name
            FROM sys.index_columns ic
            JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
            WHERE ic.is_included_column = 0
            """;

        try
        {
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(cancellationToken);

            var objects = new List<SchemaObject>();
            await using (var cmd = new SqlCommand(objectSql, conn))
            {
                cmd.Parameters.AddWithValue("@stagingPattern", StagingExcludeLike);
                await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                    objects.Add(new SchemaObject(reader.GetString(0), reader.GetString(1), reader.GetString(2)));
            }

            var columns = new List<SchemaColumn>();
            await using (var cmd = new SqlCommand(columnSql, conn))
            {
                cmd.Parameters.AddWithValue("@stagingPattern", StagingExcludeLike);
                await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                    columns.Add(new SchemaColumn(
                        reader.GetString(0), reader.GetString(1), reader.GetString(2),
                        MapType(reader.GetString(3))));
            }

            var fkKeys = await ReadKeySet(conn, fkSql, cancellationToken);
            var pkKeys = await ReadKeySet(conn, pkSql, cancellationToken);
            var indexKeys = await ReadKeySet(conn, indexSql, cancellationToken);

            return new SchemaSnapshot(objects, columns, fkKeys, pkKeys, indexKeys);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "GetSchemaSnapshot failed for {Database}", database);
            throw;
        }
    }

    private static async Task<IReadOnlySet<string>> ReadKeySet(SqlConnection conn, string sql, CancellationToken ct)
    {
        var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        await using var cmd = new SqlCommand(sql, conn);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            set.Add($"{reader.GetString(0)}.{reader.GetString(1)}.{reader.GetString(2)}");
        return set;
    }

    // SQL Server type -> shared category. tinyint deliberately maps to Other so that the missing-FK
    // analysis keeps its original type set of int/bigint/smallint/uniqueidentifier only.
    private static ColumnTypeCategory MapType(string dataType) =>
        dataType.ToLowerInvariant() switch
        {
            "int" or "bigint" or "smallint" => ColumnTypeCategory.Integer,
            "uniqueidentifier" => ColumnTypeCategory.Guid,
            "bit" => ColumnTypeCategory.Boolean,
            "decimal" or "numeric" or "money" or "smallmoney" or "float" or "real" => ColumnTypeCategory.Decimal,
            "date" or "datetime" or "datetime2" or "smalldatetime" or "datetimeoffset" or "time" => ColumnTypeCategory.Temporal,
            "char" or "varchar" or "nchar" or "nvarchar" or "text" or "ntext" => ColumnTypeCategory.Text,
            _ => ColumnTypeCategory.Other
        };

    public async Task<IReadOnlyCollection<string>> GetTableNames(string database, CancellationToken ct) =>
        await ReadNameSet(database, """
            SELECT TABLE_SCHEMA + '.' + TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_SCHEMA, TABLE_NAME
            """, ct);

    public async Task<IReadOnlyCollection<string>> GetViewNames(string database, CancellationToken ct) =>
        await ReadNameSet(database, """
            SELECT TABLE_SCHEMA + '.' + TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS
            ORDER BY TABLE_SCHEMA, TABLE_NAME
            """, ct);

    public async Task<IReadOnlyCollection<string>> GetProcedureNames(string database, CancellationToken ct) =>
        await ReadNameSet(database, """
            SELECT ROUTINE_SCHEMA + '.' + ROUTINE_NAME FROM INFORMATION_SCHEMA.ROUTINES
            WHERE ROUTINE_TYPE = 'PROCEDURE' ORDER BY ROUTINE_SCHEMA, ROUTINE_NAME
            """, ct);

    private async Task<IReadOnlyCollection<string>> ReadNameSet(string database, string sql, CancellationToken ct)
    {
        if (!_databases.TryGetValue(database, out var connectionString))
            return [];
        try
        {
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(ct);
            await using var cmd = new SqlCommand(sql, conn);
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            while (await reader.ReadAsync(ct))
                set.Add(reader.GetString(0));
            return set;
        }
        catch
        {
            return [];
        }
    }

    public async Task<IReadOnlyList<DbColumn>> GetTableColumns(string database, string tableName, CancellationToken ct)
    {
        if (!_databases.TryGetValue(database, out var connectionString))
            return [];

        var (schema, table) = ParseSchemaTable(tableName);
        const string sql = """
            SELECT c.COLUMN_NAME, c.DATA_TYPE, c.CHARACTER_MAXIMUM_LENGTH,
                   c.NUMERIC_PRECISION, c.NUMERIC_SCALE, c.IS_NULLABLE
            FROM INFORMATION_SCHEMA.COLUMNS c
            WHERE c.TABLE_SCHEMA = @schema AND c.TABLE_NAME = @table
            ORDER BY c.ORDINAL_POSITION
            """;
        try
        {
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(ct);
            await using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@schema", schema);
            cmd.Parameters.AddWithValue("@table", table);
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            var result = new List<DbColumn>();
            while (await reader.ReadAsync(ct))
            {
                int? maxLen = reader.IsDBNull(2) ? null : reader.GetInt32(2);
                int? precision = reader.IsDBNull(3) ? null : (int)reader.GetByte(3);
                int? scale = reader.IsDBNull(4) ? null : (int)reader.GetByte(4);
                result.Add(new DbColumn(
                    reader.GetString(0),
                    FormatColumnType(reader.GetString(1), maxLen, precision, scale),
                    reader.GetString(5)));
            }
            return result;
        }
        catch
        {
            return [];
        }
    }

    public Task<RoutineStats> GetProcedureStats(string database, string procName, CancellationToken ct) =>
        GetRoutineStats(database, procName, 'P', ct);

    public Task<RoutineStats> GetViewStats(string database, string viewName, CancellationToken ct) =>
        GetRoutineStats(database, viewName, 'V', ct);

    private async Task<RoutineStats> GetRoutineStats(string database, string objectName, char objectType, CancellationToken ct)
    {
        if (!_databases.TryGetValue(database, out var connectionString))
            return new RoutineStats(0, []);

        string defSql = $"""
            SELECT m.definition FROM sys.sql_modules m
            JOIN sys.objects o ON o.object_id = m.object_id
            WHERE o.type = '{objectType}' AND o.name = @name
            """;
        const string refSql = """
            SELECT DISTINCT re.referenced_entity_name
            FROM sys.dm_sql_referenced_entities(@qualifiedName, 'OBJECT') re
            JOIN sys.objects o ON o.name = re.referenced_entity_name
                AND o.schema_id = COALESCE(SCHEMA_ID(re.referenced_schema_name), SCHEMA_ID('dbo'))
            WHERE o.type IN ('U', 'V')
            ORDER BY re.referenced_entity_name
            """;
        try
        {
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(ct);

            string def;
            await using (var cmd = new SqlCommand(defSql, conn))
            {
                cmd.Parameters.AddWithValue("@name", objectName.Trim('[', ']'));
                def = await cmd.ExecuteScalarAsync(ct) as string ?? "";
            }
            if (string.IsNullOrEmpty(def))
                return new RoutineStats(0, []);

            int lines = def.Split('\n').Length;
            var tables = new List<string>();
            string qualified = objectName.Contains('.') ? objectName : $"dbo.{objectName}";
            await using (var cmd = new SqlCommand(refSql, conn))
            {
                cmd.Parameters.AddWithValue("@qualifiedName", qualified);
                try
                {
                    await using var reader = await cmd.ExecuteReaderAsync(ct);
                    while (await reader.ReadAsync(ct))
                        tables.Add(reader.GetString(0));
                }
                catch
                {
                    // dm_sql_referenced_entities can fail for some objects; treat as empty.
                }
            }
            return new RoutineStats(lines, tables);
        }
        catch
        {
            return new RoutineStats(0, []);
        }
    }
}
```

- [ ] **Step 3: Write the engine facade**

`SqlSchemaMcp.SqlServer/SqlServerEngine.cs`:

```csharp
using System.Collections.Generic;
using SqlSchemaMcp.Abstractions;

namespace SqlSchemaMcp.SqlServer;

public sealed class SqlServerEngine(
    SqlServerSchema schema,
    SqlServerData data,
    SqlServerQuery query,
    SqlServerSchemaModels models,
    SqlServerPermissionProbe probe) : IDbEngine
{
    public DatabaseEngine Engine => DatabaseEngine.SqlServer;

    public Task<string> ListTables(string database, string? schemaFilter, string? nameFilter, CancellationToken ct) =>
        schema.ListTables(database, schemaFilter, nameFilter, ct);
    public Task<string> ListViews(string database, string? nameFilter, CancellationToken ct) =>
        schema.ListViews(database, nameFilter, ct);
    public Task<string> ListProcedures(string database, string? nameFilter, CancellationToken ct) =>
        schema.ListProcedures(database, nameFilter, ct);
    public Task<string> ListFunctions(string database, string? nameFilter, CancellationToken ct) =>
        schema.ListFunctions(database, nameFilter, ct);
    public Task<string> GetTableSchema(string database, string tableName, CancellationToken ct) =>
        schema.GetTableSchema(database, tableName, ct);
    public Task<string> GetViewDefinition(string database, string viewName, CancellationToken ct) =>
        schema.GetViewDefinition(database, viewName, ct);
    public Task<string> GetProcedureDefinition(string database, string procName, CancellationToken ct) =>
        schema.GetProcedureDefinition(database, procName, ct);
    public Task<string> GetFunctionDefinition(string database, string functionName, CancellationToken ct) =>
        schema.GetFunctionDefinition(database, functionName, ct);
    public Task<string> FindReferences(string database, string objectName, CancellationToken ct) =>
        schema.FindReferences(database, objectName, ct);
    public Task<string> SearchDefinitions(string database, string keyword, CancellationToken ct) =>
        schema.SearchDefinitions(database, keyword, ct);
    public Task<string> ListTriggers(string database, string? nameFilter, CancellationToken ct) =>
        schema.ListTriggers(database, nameFilter, ct);
    public Task<string> GetTriggerDefinition(string database, string triggerName, CancellationToken ct) =>
        schema.GetTriggerDefinition(database, triggerName, ct);
    public Task<string> ListSynonyms(string database, string? nameFilter, CancellationToken ct) =>
        schema.ListSynonyms(database, nameFilter, ct);
    public Task<string> ListCheckConstraints(string database, string? nameFilter, CancellationToken ct) =>
        schema.ListCheckConstraints(database, nameFilter, ct);
    public Task<string> ListDdlTriggers(string database, CancellationToken ct) =>
        schema.ListDdlTriggers(database, ct);
    public Task<string> GetDdlTriggerDefinition(string database, string triggerName, CancellationToken ct) =>
        schema.GetDdlTriggerDefinition(database, triggerName, ct);

    public Task<string> SampleTableData(string database, string tableName, int rows, CancellationToken ct) =>
        data.SampleTableData(database, tableName, rows, ct);
    public Task<string> AnalyzeColumnDistribution(string database, string tableName, string columnName, CancellationToken ct) =>
        data.AnalyzeColumnDistribution(database, tableName, columnName, ct);
    public Task<string> FindNullableColumnsWithNoNulls(string database, string tableName, CancellationToken ct) =>
        data.FindNullableColumnsWithNoNulls(database, tableName, ct);
    public Task<string> FindDuplicateRows(string database, string tableName, string columns, int top, CancellationToken ct) =>
        data.FindDuplicateRows(database, tableName, columns, top, ct);

    public Task<string> ExecuteQuery(string database, string sql, CancellationToken ct) =>
        query.ExecuteQuery(database, sql, ct);

    public Task<SchemaSnapshot> GetSchemaSnapshot(string database, CancellationToken ct) =>
        models.GetSchemaSnapshot(database, ct);
    public Task<IReadOnlyCollection<string>> GetTableNames(string database, CancellationToken ct) =>
        models.GetTableNames(database, ct);
    public Task<IReadOnlyCollection<string>> GetViewNames(string database, CancellationToken ct) =>
        models.GetViewNames(database, ct);
    public Task<IReadOnlyCollection<string>> GetProcedureNames(string database, CancellationToken ct) =>
        models.GetProcedureNames(database, ct);
    public Task<IReadOnlyList<DbColumn>> GetTableColumns(string database, string tableName, CancellationToken ct) =>
        models.GetTableColumns(database, tableName, ct);
    public Task<RoutineStats> GetProcedureStats(string database, string procName, CancellationToken ct) =>
        models.GetProcedureStats(database, procName, ct);
    public Task<RoutineStats> GetViewStats(string database, string viewName, CancellationToken ct) =>
        models.GetViewStats(database, viewName, ct);

    public Task<LoginPermissionResult> ProbePermissionsAsync(string database, string connectionString, CancellationToken ct) =>
        probe.ProbeAsync(database, connectionString, ct);
}
```

- [ ] **Step 4: Build the SqlServer project**

Run: `dotnet build SqlSchemaMcp.SqlServer/SqlSchemaMcp.SqlServer.csproj`
Expected: `Build succeeded`. (The host still references the old `Data/*Queries.cs` — it will not build until Task 8. Do not run the full solution build yet.)

- [ ] **Step 5: Commit**

```bash
git add SqlSchemaMcp.SqlServer
git commit -m "Migrate SQL Server schema/data/query classes and add SqlServerEngine facade"
```

---

### Task 5: Migrate the SQL-Server-only query classes into the engine project

**Files:**
- Move: `Data/AnalysisQueries.cs` → `SqlSchemaMcp.SqlServer/SqlServerAnalysis.cs` (rename class → `SqlServerAnalysis`; **remove** the three shared methods `AnalyzeNamingConventions`, `AnalyzeMissingForeignKeys`, `AnalyzeMissingIndexes` and their private helpers `BuildNamingReport`/`AppendViolationSection` — those move to the shared analyzers in Task 6)
- Move: `Data/DiagnosticsQueries.cs` → `SqlSchemaMcp.SqlServer/SqlServerDiagnostics.cs` (rename class → `SqlServerDiagnostics`)
- Move: `Data/PipelineQueries.cs` → `SqlSchemaMcp.SqlServer/SqlServerPipeline.cs` (rename class → `SqlServerPipeline`)
- Move: `Data/SecurityQueries.cs` → `SqlSchemaMcp.SqlServer/SqlServerSecurity.cs` (rename class → `SqlServerSecurity`)

**Interfaces:**
- Consumes: `SqlQueryBase`, `SqlServerOptions` (Tasks 1, 3)
- Produces: `SqlServerAnalysis`, `SqlServerDiagnostics`, `SqlServerPipeline`, `SqlServerSecurity` — concrete SQL-Server-only classes the host dispatchers delegate to (Task 9). Public method signatures are unchanged from today, so the dispatchers can call them 1:1.

- [ ] **Step 1: Move the four classes with mechanical edits only**

For each file: move into `SqlSchemaMcp.SqlServer/`, change `namespace SqlSchemaMcp.Data;` → `namespace SqlSchemaMcp.SqlServer;`, rename the class + its `ILogger<...>` + base call, add `using SqlSchemaMcp.Abstractions;`. All SQL and output formatting stays byte-for-byte identical.

- [ ] **Step 2: Remove the three shared analyses from `SqlServerAnalysis`**

Delete only these members from `SqlServerAnalysis.cs` (they are re-implemented as shared analyzers in Task 6): `AnalyzeNamingConventions`, `BuildNamingReport`, `AppendViolationSection`, `AnalyzeMissingForeignKeys`, `AnalyzeMissingIndexes`. Keep every other method (`AnalyzeDuplicateIndexes`, `FindUnusedTables`, `FindUnusedProcedures`, `AnalyzeProcComplexity`, `AnalyzeViewComplexity`, `AnalyzeIndexFragmentation`, `AnalyzeTriggers`, `AppendTriggerSection`, `AnalyzeIdentityColumns`, `AnalyzeTableSizes`, `AnalyzeMissingIndexSuggestions`, `GetRecentObjectChanges`, `AnalyzeTableQueryStats`, `AnalyzeTableAccessStats`, `GenerateDatabaseSummary`) exactly as-is.

- [ ] **Step 3: Build the SqlServer project**

Run: `dotnet build SqlSchemaMcp.SqlServer/SqlSchemaMcp.SqlServer.csproj`
Expected: `Build succeeded`.

- [ ] **Step 4: Commit**

```bash
git add SqlSchemaMcp.SqlServer
git commit -m "Migrate SQL-Server-only analysis/diagnostics/pipeline/security classes into engine project"
```

---

### Task 6: Shared analyzers (naming / missing FK / missing index) over `SchemaSnapshot`

**Files:**
- Create: `Analysis/NamingAnalyzer.cs`
- Create: `Analysis/MissingForeignKeyAnalyzer.cs`
- Create: `Analysis/MissingIndexAnalyzer.cs`
- Create: `Analysis/AnalyzerHelpers.cs`
- Test: `SqlSchemaMcp.Tests/Analysis/SharedAnalyzerTests.cs`

**Interfaces:**
- Consumes: `SchemaSnapshot`, `SchemaObject`, `SchemaColumn`, `ColumnTypeCategory` (Task 1)
- Produces: `NamingAnalyzer.Build(string database, SchemaSnapshot snapshot) -> string`, `MissingForeignKeyAnalyzer.Build(...)`, `MissingIndexAnalyzer.Build(...)` — consumed by the `AnalysisQueries` dispatcher (Task 8). Output strings reproduce today's SQL Server output exactly.

- [ ] **Step 1: Write the failing test**

`SqlSchemaMcp.Tests/Analysis/SharedAnalyzerTests.cs`:

```csharp
using System.Collections.Generic;
using FluentAssertions;
using SqlSchemaMcp.Abstractions;
using SqlSchemaMcp.Analysis;
using Xunit;

namespace SqlSchemaMcp.Tests.Analysis;

public sealed class SharedAnalyzerTests
{
    private static SchemaSnapshot Snapshot(
        IReadOnlyList<SchemaObject>? objects = null,
        IReadOnlyList<SchemaColumn>? columns = null,
        IReadOnlySet<string>? fk = null,
        IReadOnlySet<string>? pk = null,
        IReadOnlySet<string>? idx = null) =>
        new(objects ?? [], columns ?? [],
            fk ?? new HashSet<string>(), pk ?? new HashSet<string>(), idx ?? new HashSet<string>());

    [Fact]
    public void Naming_FlagsHungarianPrefixAndVersionSuffix()
    {
        var snapshot = Snapshot(objects:
        [
            new SchemaObject("TABLE", "dbo", "tbl_Orders"),
            new SchemaObject("VIEW", "dbo", "Orders_v2"),
        ]);

        var report = NamingAnalyzer.Build("poc", snapshot);

        report.Should().Contain("NAMING CONVENTION ANALYSIS: [poc]");
        report.Should().Contain("tbl_Orders");
        report.Should().Contain("Orders_v2");
    }

    [Fact]
    public void MissingForeignKey_FlagsIdColumnWithNoConstraint()
    {
        var snapshot = Snapshot(columns:
        [
            new SchemaColumn("dbo", "Orders", "CustomerId", ColumnTypeCategory.Integer),
        ]);

        var report = MissingForeignKeyAnalyzer.Build("poc", snapshot);

        report.Should().Contain("[dbo].[Orders].CustomerId");
        report.Should().Contain("1 potential missing FK");
    }

    [Fact]
    public void MissingForeignKey_SkipsColumnThatAlreadyHasFk()
    {
        var snapshot = Snapshot(
            columns: [new SchemaColumn("dbo", "Orders", "CustomerId", ColumnTypeCategory.Integer)],
            fk: new HashSet<string>(System.StringComparer.OrdinalIgnoreCase) { "dbo.Orders.CustomerId" });

        var report = MissingForeignKeyAnalyzer.Build("poc", snapshot);

        report.Should().Contain("0 potential missing FK");
    }

    [Fact]
    public void MissingIndex_FlagsUnindexedFilterColumn()
    {
        var snapshot = Snapshot(columns:
        [
            new SchemaColumn("dbo", "Orders", "Status", ColumnTypeCategory.Text),
        ]);

        var report = MissingIndexAnalyzer.Build("poc", snapshot);

        report.Should().Contain("[dbo].[Orders].Status");
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `dotnet test SqlSchemaMcp.sln --filter SharedAnalyzerTests`
Expected: FAIL — analyzers do not exist.

- [ ] **Step 3: Write the analyzer helpers**

`Analysis/AnalyzerHelpers.cs`:

```csharp
using System;
using System.Collections.Generic;
using SqlSchemaMcp.Abstractions;

namespace SqlSchemaMcp.Analysis;

internal static class AnalyzerHelpers
{
    // Reproduces the SQL LIKE set '%Id' / '%ID' / '%_id' (case-insensitive), i.e. names ending in "id".
    public static bool IsIdName(string column) =>
        column.Length >= 2 && column.EndsWith("id", StringComparison.OrdinalIgnoreCase);

    public static readonly HashSet<string> CommonFilterColumns = new(StringComparer.OrdinalIgnoreCase)
    {
        "IsActive", "IsDeleted", "Status", "CreatedAt", "DeletedAt",
        "TenantId", "OrganisationId", "OrganizationId", "AccountId"
    };

    public static string Key(SchemaColumn c) => $"{c.Schema}.{c.Table}.{c.Column}";
}
```

- [ ] **Step 4: Write `NamingAnalyzer` (logic moved verbatim from `BuildNamingReport`)**

`Analysis/NamingAnalyzer.cs`:

```csharp
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using SqlSchemaMcp.Abstractions;

namespace SqlSchemaMcp.Analysis;

public static class NamingAnalyzer
{
    public static string Build(string database, SchemaSnapshot snapshot)
    {
        var hungarian = new List<string>();
        var versionSuffix = new List<string>();
        var allCaps = new List<string>();
        var snakeCase = new List<string>();

        string[] hungarianPrefixes = ["tbl_", "sp_", "vw_", "col_", "f_", "fn_", "usp_"];
        string[] versionSuffixes = ["_v2", "_v3", "_v4", "_v5", "_final", "_old", "_backup", "_copy", "_new", "_temp", "_bak"];

        foreach (var o in snapshot.Objects)
        {
            string lower = o.Name.ToLowerInvariant();
            string label = $"  [{o.Schema}].[{o.Name}] ({o.Type})";

            if (hungarianPrefixes.Any(p => lower.StartsWith(p, StringComparison.OrdinalIgnoreCase)))
                hungarian.Add(label);
            if (versionSuffixes.Any(s => lower.EndsWith(s, StringComparison.OrdinalIgnoreCase)))
                versionSuffix.Add(label);
            if (string.Equals(o.Name, o.Name.ToUpperInvariant(), StringComparison.Ordinal) && o.Name.Length > 1)
                allCaps.Add(label);
            if (o.Name.Contains('_') && !hungarianPrefixes.Any(p => lower.StartsWith(p, StringComparison.OrdinalIgnoreCase)))
                snakeCase.Add(label);
        }

        var colHungarian = new List<string>();
        var colAllCaps = new List<string>();
        var colSnakeCase = new List<string>();

        foreach (var c in snapshot.Columns)
        {
            string lower = c.Column.ToLowerInvariant();
            string label = $"  [{c.Schema}].[{c.Table}].{c.Column}";

            if (hungarianPrefixes.Any(p => lower.StartsWith(p, StringComparison.OrdinalIgnoreCase)))
                colHungarian.Add(label);
            if (string.Equals(c.Column, c.Column.ToUpperInvariant(), StringComparison.Ordinal) && c.Column.Length > 1)
                colAllCaps.Add(label);
            if (c.Column.Contains('_'))
                colSnakeCase.Add(label);
        }

        var sb = new StringBuilder();
        sb.AppendLine($"NAMING CONVENTION ANALYSIS: [{database}]");
        sb.AppendLine(new string('─', 70));

        AppendViolationSection(sb, "HUNGARIAN PREFIXES (objects)", hungarian);
        AppendViolationSection(sb, "HUNGARIAN PREFIXES (columns)", colHungarian);
        AppendViolationSection(sb, "VERSION SUFFIXES (_v2, _OLD, _FINAL, etc.)", versionSuffix);
        AppendViolationSection(sb, "ALL_CAPS OBJECTS", allCaps);
        AppendViolationSection(sb, "ALL_CAPS COLUMNS", colAllCaps);
        AppendViolationSection(sb, "snake_case OBJECTS", snakeCase);
        AppendViolationSection(sb, "snake_case COLUMNS", colSnakeCase);

        int total = hungarian.Count + colHungarian.Count + versionSuffix.Count
            + allCaps.Count + colAllCaps.Count + snakeCase.Count + colSnakeCase.Count;
        sb.AppendLine($"Total violations: {total}");

        return sb.ToString();
    }

    private static void AppendViolationSection(StringBuilder sb, string header, List<string> items)
    {
        sb.AppendLine();
        sb.AppendLine($"{header} ({items.Count})");
        sb.AppendLine(new string('-', 60));
        if (items.Count == 0)
            sb.AppendLine("  (none)");
        else
            foreach (var item in items)
                sb.AppendLine(item);
    }
}
```

- [ ] **Step 5: Write `MissingForeignKeyAnalyzer` (logic moved verbatim from `AnalyzeMissingForeignKeys`)**

`Analysis/MissingForeignKeyAnalyzer.cs`:

```csharp
using System.Text;
using SqlSchemaMcp.Abstractions;

namespace SqlSchemaMcp.Analysis;

public static class MissingForeignKeyAnalyzer
{
    public static string Build(string database, SchemaSnapshot snapshot)
    {
        var sb = new StringBuilder();
        sb.AppendLine($"MISSING FOREIGN KEY ANALYSIS: [{database}]");
        sb.AppendLine(new string('─', 80));
        sb.AppendLine("Columns matching FK name patterns with no FK constraint defined:");
        sb.AppendLine();

        int count = 0;
        foreach (var c in snapshot.Columns)
        {
            bool candidateType = c.TypeCategory is ColumnTypeCategory.Integer or ColumnTypeCategory.Guid;
            if (!candidateType || !AnalyzerHelpers.IsIdName(c.Column))
                continue;

            string key = AnalyzerHelpers.Key(c);
            if (snapshot.ForeignKeyColumnKeys.Contains(key) || snapshot.PrimaryKeyColumnKeys.Contains(key))
                continue;

            count++;
            string typeLabel = c.TypeCategory == ColumnTypeCategory.Guid ? "uniqueidentifier" : "int";
            sb.AppendLine($"  [{c.Schema}].[{c.Table}].{c.Column} ({typeLabel})");
        }

        if (count == 0)
            sb.AppendLine("  (none found — all FK-pattern columns have constraints)");

        sb.AppendLine();
        sb.AppendLine($"  {count} potential missing FK(s)");
        return sb.ToString();
    }
}
```

Note for implementers: today's report printed the raw SQL data type in parentheses (e.g. `(int)`, `(bigint)`, `(uniqueidentifier)`). The shared model collapses integer types into `ColumnTypeCategory.Integer`, so the label is normalized to `int`/`uniqueidentifier`. This is the one intentional, documented cosmetic difference in this migration — the set of flagged columns is identical; only the parenthetical type label for `bigint`/`smallint` columns changes to `int`. Call this out in the PR description.

- [ ] **Step 6: Write `MissingIndexAnalyzer` (logic moved verbatim from `AnalyzeMissingIndexes`)**

`Analysis/MissingIndexAnalyzer.cs`:

```csharp
using System.Text;
using SqlSchemaMcp.Abstractions;

namespace SqlSchemaMcp.Analysis;

public static class MissingIndexAnalyzer
{
    public static string Build(string database, SchemaSnapshot snapshot)
    {
        var sb = new StringBuilder();
        sb.AppendLine($"MISSING INDEX ANALYSIS: [{database}]");
        sb.AppendLine(new string('─', 80));
        sb.AppendLine("FK-pattern and common filter columns with no index:");
        sb.AppendLine();

        int count = 0;
        foreach (var c in snapshot.Columns)
        {
            bool candidate = AnalyzerHelpers.IsIdName(c.Column) || AnalyzerHelpers.CommonFilterColumns.Contains(c.Column);
            if (!candidate)
                continue;
            if (snapshot.IndexedColumnKeys.Contains(AnalyzerHelpers.Key(c)))
                continue;

            count++;
            sb.AppendLine($"  [{c.Schema}].[{c.Table}].{c.Column}");
        }

        if (count == 0)
            sb.AppendLine("  (none found — all candidate columns are indexed)");

        sb.AppendLine();
        sb.AppendLine($"  {count} potentially unindexed column(s)");
        return sb.ToString();
    }
}
```

- [ ] **Step 7: Run the test to verify it passes**

Run: `dotnet test SqlSchemaMcp.sln --filter SharedAnalyzerTests`
Expected: PASS (4 tests). (Full-solution build still blocked until Task 8; if the host does not compile, defer this run to Task 12 and keep the commit.)

- [ ] **Step 8: Commit**

```bash
git add Analysis SqlSchemaMcp.Tests/Analysis
git commit -m "Add shared engine-agnostic analyzers for naming, missing FK, missing index"
```

---

### Task 7: Engine resolver

**Files:**
- Create: `Engines/EngineResolver.cs`
- Test: `SqlSchemaMcp.Tests/Engines/EngineResolverTests.cs`

**Interfaces:**
- Consumes: `IEngineResolver`, `IDbEngine`, `DatabaseConfig`, `DatabaseEngine` (Task 1)
- Produces: `EngineResolver(IReadOnlyList<DatabaseConfig> databases, IReadOnlyDictionary<DatabaseEngine, IDbEngine> engines) : IEngineResolver` — registered in DI (Task 11)

- [ ] **Step 1: Write the failing test**

`SqlSchemaMcp.Tests/Engines/EngineResolverTests.cs`:

```csharp
using System.Collections.Generic;
using FluentAssertions;
using NSubstitute;
using SqlSchemaMcp.Abstractions;
using SqlSchemaMcp.Engines;
using Xunit;

namespace SqlSchemaMcp.Tests.Engines;

public sealed class EngineResolverTests
{
    private static IDbEngine EngineOf(DatabaseEngine kind)
    {
        var e = Substitute.For<IDbEngine>();
        e.Engine.Returns(kind);
        return e;
    }

    [Fact]
    public void TryResolve_KnownDatabase_ReturnsOwningEngine()
    {
        var sql = EngineOf(DatabaseEngine.SqlServer);
        var resolver = new EngineResolver(
            [new DatabaseConfig("poc", DatabaseEngine.SqlServer, "cs")],
            new Dictionary<DatabaseEngine, IDbEngine> { [DatabaseEngine.SqlServer] = sql });

        resolver.TryResolve("poc", out var engine).Should().BeTrue();
        engine.Should().BeSameAs(sql);
    }

    [Fact]
    public void TryResolve_UnknownDatabase_ReturnsFalse()
    {
        var resolver = new EngineResolver([], new Dictionary<DatabaseEngine, IDbEngine>());

        resolver.TryResolve("missing", out _).Should().BeFalse();
    }

    [Fact]
    public void TryGetKind_IsCaseInsensitive()
    {
        var resolver = new EngineResolver(
            [new DatabaseConfig("Poc", DatabaseEngine.Postgres, "cs")],
            new Dictionary<DatabaseEngine, IDbEngine> { [DatabaseEngine.Postgres] = EngineOf(DatabaseEngine.Postgres) });

        resolver.TryGetKind("poc", out var kind).Should().BeTrue();
        kind.Should().Be(DatabaseEngine.Postgres);
    }

    [Fact]
    public void DatabaseNames_ExposesAllConfiguredNames()
    {
        var resolver = new EngineResolver(
            [new DatabaseConfig("a", DatabaseEngine.SqlServer, "cs"), new DatabaseConfig("b", DatabaseEngine.SqlServer, "cs")],
            new Dictionary<DatabaseEngine, IDbEngine> { [DatabaseEngine.SqlServer] = EngineOf(DatabaseEngine.SqlServer) });

        resolver.DatabaseNames.Should().BeEquivalentTo("a", "b");
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `dotnet test SqlSchemaMcp.sln --filter EngineResolverTests`
Expected: FAIL — `EngineResolver` does not exist.

- [ ] **Step 3: Write `EngineResolver`**

`Engines/EngineResolver.cs`:

```csharp
using System;
using System.Collections.Generic;
using System.Linq;
using SqlSchemaMcp.Abstractions;

namespace SqlSchemaMcp.Engines;

public sealed class EngineResolver : IEngineResolver
{
    private readonly Dictionary<string, IDbEngine> _byName;
    private readonly Dictionary<string, DatabaseEngine> _kindByName;

    public EngineResolver(
        IReadOnlyList<DatabaseConfig> databases,
        IReadOnlyDictionary<DatabaseEngine, IDbEngine> engines)
    {
        Databases = databases;
        _byName = new Dictionary<string, IDbEngine>(StringComparer.OrdinalIgnoreCase);
        _kindByName = new Dictionary<string, DatabaseEngine>(StringComparer.OrdinalIgnoreCase);

        foreach (var db in databases)
        {
            if (!engines.TryGetValue(db.Engine, out var engine))
                throw new InvalidOperationException(
                    $"Database '{db.Name}' declares engine '{db.Engine}' but no implementation for that engine is registered.");
            _byName[db.Name] = engine;
            _kindByName[db.Name] = db.Engine;
        }
    }

    public IReadOnlyList<DatabaseConfig> Databases { get; }

    public IReadOnlyCollection<string> DatabaseNames => _byName.Keys;

    public bool TryResolve(string database, out IDbEngine engine) =>
        _byName.TryGetValue(database, out engine!);

    public bool TryGetKind(string database, out DatabaseEngine kind) =>
        _kindByName.TryGetValue(database, out kind);
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `dotnet test SqlSchemaMcp.sln --filter EngineResolverTests`
Expected: PASS (4 tests). (If the host still does not build, defer to Task 12 and keep the commit.)

- [ ] **Step 5: Commit**

```bash
git add Engines SqlSchemaMcp.Tests/Engines
git commit -m "Add EngineResolver mapping database names to owning engines"
```

---

### Task 8: Rewrite the cross-engine dispatchers (Schema, Data, Query, Analysis, Compare)

**Files:**
- Rewrite: `Data/SchemaQueries.cs`
- Rewrite: `Data/DataQueries.cs`
- Rewrite: `Data/QueryQueries.cs`
- Rewrite: `Data/AnalysisQueries.cs`
- Rewrite: `Data/CompareQueries.cs`

**Interfaces:**
- Consumes: `IEngineResolver`, `IDbEngine`, `Sentinels`, `SchemaSnapshot`, `DbColumn`, `RoutineStats` (Tasks 1, 7); the shared analyzers (Task 6)
- Produces: dispatcher classes named exactly `SchemaQueries`, `DataQueries`, `QueryQueries`, `AnalysisQueries`, `CompareQueries` in namespace `SqlSchemaMcp.Data` — so `Tools/*.cs` compile unchanged. `CompareQueries` still exposes `GetTableNames`/`GetProcNames`/`GetViewNames`/`GetTableColumns`/`GetProcStats`/`GetViewStats` returning the exact types `CompareTools` expects, and `ColumnInfo` stays defined here.

- [ ] **Step 1: Rewrite `SchemaQueries` as a dispatcher**

`Data/SchemaQueries.cs`:

```csharp
using System.Threading;
using System.Threading.Tasks;
using SqlSchemaMcp.Abstractions;

namespace SqlSchemaMcp.Data;

public sealed class SchemaQueries(IEngineResolver resolver)
{
    private Task<string> Route(string database, System.Func<IDbEngine, Task<string>> call) =>
        resolver.TryResolve(database, out var engine)
            ? call(engine)
            : Task.FromResult(Sentinels.UnknownDatabase(resolver.DatabaseNames, database));

    public Task<string> ListTables(string database, string? schemaFilter, string? nameFilter, CancellationToken ct = default) =>
        Route(database, e => e.ListTables(database, schemaFilter, nameFilter, ct));
    public Task<string> ListViews(string database, string? nameFilter, CancellationToken ct = default) =>
        Route(database, e => e.ListViews(database, nameFilter, ct));
    public Task<string> ListProcedures(string database, string? nameFilter, CancellationToken ct = default) =>
        Route(database, e => e.ListProcedures(database, nameFilter, ct));
    public Task<string> ListFunctions(string database, string? nameFilter, CancellationToken ct = default) =>
        Route(database, e => e.ListFunctions(database, nameFilter, ct));
    public Task<string> GetTableSchema(string database, string tableName, CancellationToken ct = default) =>
        Route(database, e => e.GetTableSchema(database, tableName, ct));
    public Task<string> GetViewDefinition(string database, string viewName, CancellationToken ct = default) =>
        Route(database, e => e.GetViewDefinition(database, viewName, ct));
    public Task<string> GetProcedureDefinition(string database, string procName, CancellationToken ct = default) =>
        Route(database, e => e.GetProcedureDefinition(database, procName, ct));
    public Task<string> GetFunctionDefinition(string database, string functionName, CancellationToken ct = default) =>
        Route(database, e => e.GetFunctionDefinition(database, functionName, ct));
    public Task<string> FindReferences(string database, string objectName, CancellationToken ct = default) =>
        Route(database, e => e.FindReferences(database, objectName, ct));
    public Task<string> SearchDefinitions(string database, string keyword, CancellationToken ct = default) =>
        Route(database, e => e.SearchDefinitions(database, keyword, ct));
    public Task<string> ListTriggers(string database, string? nameFilter, CancellationToken ct = default) =>
        Route(database, e => e.ListTriggers(database, nameFilter, ct));
    public Task<string> GetTriggerDefinition(string database, string triggerName, CancellationToken ct = default) =>
        Route(database, e => e.GetTriggerDefinition(database, triggerName, ct));
    public Task<string> ListSynonyms(string database, string? nameFilter, CancellationToken ct = default) =>
        Route(database, e => e.ListSynonyms(database, nameFilter, ct));
    public Task<string> ListCheckConstraints(string database, string? nameFilter, CancellationToken ct = default) =>
        Route(database, e => e.ListCheckConstraints(database, nameFilter, ct));
    public Task<string> ListDdlTriggers(string database, CancellationToken ct = default) =>
        Route(database, e => e.ListDdlTriggers(database, ct));
    public Task<string> GetDdlTriggerDefinition(string database, string triggerName, CancellationToken ct = default) =>
        Route(database, e => e.GetDdlTriggerDefinition(database, triggerName, ct));
}
```

- [ ] **Step 2: Rewrite `DataQueries` and `QueryQueries` as dispatchers**

`Data/DataQueries.cs`:

```csharp
using System;
using System.Threading;
using System.Threading.Tasks;
using SqlSchemaMcp.Abstractions;

namespace SqlSchemaMcp.Data;

public sealed class DataQueries(IEngineResolver resolver)
{
    private Task<string> Route(string database, Func<IDbEngine, Task<string>> call) =>
        resolver.TryResolve(database, out var engine)
            ? call(engine)
            : Task.FromResult(Sentinels.UnknownDatabase(resolver.DatabaseNames, database));

    public Task<string> SampleTableData(string database, string tableName, int rows, CancellationToken ct = default) =>
        Route(database, e => e.SampleTableData(database, tableName, rows, ct));
    public Task<string> AnalyzeColumnDistribution(string database, string tableName, string columnName, CancellationToken ct = default) =>
        Route(database, e => e.AnalyzeColumnDistribution(database, tableName, columnName, ct));
    public Task<string> FindNullableColumnsWithNoNulls(string database, string tableName, CancellationToken ct = default) =>
        Route(database, e => e.FindNullableColumnsWithNoNulls(database, tableName, ct));
    public Task<string> FindDuplicateRows(string database, string tableName, string columns, int top, CancellationToken ct = default) =>
        Route(database, e => e.FindDuplicateRows(database, tableName, columns, top, ct));
}
```

`Data/QueryQueries.cs`:

```csharp
using System;
using System.Threading;
using System.Threading.Tasks;
using SqlSchemaMcp.Abstractions;

namespace SqlSchemaMcp.Data;

public sealed class QueryQueries(IEngineResolver resolver)
{
    public Task<string> ExecuteQuery(string database, string sql, CancellationToken ct = default) =>
        resolver.TryResolve(database, out var engine)
            ? engine.ExecuteQuery(database, sql, ct)
            : Task.FromResult(Sentinels.UnknownDatabase(resolver.DatabaseNames, database));
}
```

- [ ] **Step 3: Rewrite `AnalysisQueries` as a mixed dispatcher**

`Data/AnalysisQueries.cs`:

```csharp
using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using SqlSchemaMcp.Abstractions;
using SqlSchemaMcp.Analysis;
using SqlSchemaMcp.SqlServer;

namespace SqlSchemaMcp.Data;

public sealed class AnalysisQueries(
    IEngineResolver resolver,
    SqlServerAnalysis sqlServer,
    ILogger<AnalysisQueries> logger)
{
    public Task<string> AnalyzeNamingConventions(string database, CancellationToken ct = default) =>
        Shared(database, NamingAnalyzer.Build, ct);
    public Task<string> AnalyzeMissingForeignKeys(string database, CancellationToken ct = default) =>
        Shared(database, MissingForeignKeyAnalyzer.Build, ct);
    public Task<string> AnalyzeMissingIndexes(string database, CancellationToken ct = default) =>
        Shared(database, MissingIndexAnalyzer.Build, ct);

    public Task<string> AnalyzeDuplicateIndexes(string database, CancellationToken ct = default) =>
        SqlOnly(database, nameof(AnalyzeDuplicateIndexes), () => sqlServer.AnalyzeDuplicateIndexes(database, ct));
    public Task<string> FindUnusedTables(string database, CancellationToken ct = default) =>
        SqlOnly(database, nameof(FindUnusedTables), () => sqlServer.FindUnusedTables(database, ct));
    public Task<string> FindUnusedProcedures(string database, CancellationToken ct = default) =>
        SqlOnly(database, nameof(FindUnusedProcedures), () => sqlServer.FindUnusedProcedures(database, ct));
    public Task<string> AnalyzeProcComplexity(string database, string? nameFilter, CancellationToken ct = default) =>
        SqlOnly(database, nameof(AnalyzeProcComplexity), () => sqlServer.AnalyzeProcComplexity(database, nameFilter, ct));
    public Task<string> AnalyzeViewComplexity(string database, string? nameFilter, CancellationToken ct = default) =>
        SqlOnly(database, nameof(AnalyzeViewComplexity), () => sqlServer.AnalyzeViewComplexity(database, nameFilter, ct));
    public Task<string> AnalyzeIndexFragmentation(string database, string? nameFilter, CancellationToken ct = default) =>
        SqlOnly(database, nameof(AnalyzeIndexFragmentation), () => sqlServer.AnalyzeIndexFragmentation(database, nameFilter, ct));
    public Task<string> AnalyzeTriggers(string database, CancellationToken ct = default) =>
        SqlOnly(database, nameof(AnalyzeTriggers), () => sqlServer.AnalyzeTriggers(database, ct));
    public Task<string> AnalyzeIdentityColumns(string database, CancellationToken ct = default) =>
        SqlOnly(database, nameof(AnalyzeIdentityColumns), () => sqlServer.AnalyzeIdentityColumns(database, ct));
    public Task<string> AnalyzeTableSizes(string database, CancellationToken ct = default) =>
        SqlOnly(database, nameof(AnalyzeTableSizes), () => sqlServer.AnalyzeTableSizes(database, ct));
    public Task<string> AnalyzeMissingIndexSuggestions(string database, CancellationToken ct = default) =>
        SqlOnly(database, nameof(AnalyzeMissingIndexSuggestions), () => sqlServer.AnalyzeMissingIndexSuggestions(database, ct));
    public Task<string> GetRecentObjectChanges(string database, int days, CancellationToken ct = default) =>
        SqlOnly(database, nameof(GetRecentObjectChanges), () => sqlServer.GetRecentObjectChanges(database, days, ct));
    public Task<string> AnalyzeTableQueryStats(string database, CancellationToken ct = default) =>
        SqlOnly(database, nameof(AnalyzeTableQueryStats), () => sqlServer.AnalyzeTableQueryStats(database, ct));
    public Task<string> AnalyzeTableAccessStats(string database, CancellationToken ct = default) =>
        SqlOnly(database, nameof(AnalyzeTableAccessStats), () => sqlServer.AnalyzeTableAccessStats(database, ct));
    public Task<string> GenerateDatabaseSummary(string database, CancellationToken ct = default) =>
        SqlOnly(database, nameof(GenerateDatabaseSummary), () => sqlServer.GenerateDatabaseSummary(database, ct));

    private async Task<string> Shared(string database, Func<string, SchemaSnapshot, string> analyzer, CancellationToken ct)
    {
        if (!resolver.TryResolve(database, out var engine))
            return Sentinels.UnknownDatabase(resolver.DatabaseNames, database);
        try
        {
            var snapshot = await engine.GetSchemaSnapshot(database, ct);
            return analyzer(database, snapshot);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Shared analysis failed for {Database}", database);
            return "ERROR: the query failed. Check the server log for details.";
        }
    }

    private Task<string> SqlOnly(string database, string tool, Func<Task<string>> sqlServerCall)
    {
        if (!resolver.TryGetKind(database, out var kind))
            return Task.FromResult(Sentinels.UnknownDatabase(resolver.DatabaseNames, database));
        return kind == DatabaseEngine.SqlServer
            ? sqlServerCall()
            : Task.FromResult(Sentinels.Unsupported(tool, kind));
    }
}
```

- [ ] **Step 4: Rewrite `CompareQueries` as a dispatcher (keep the exact return types `CompareTools` uses)**

`Data/CompareQueries.cs`:

```csharp
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using SqlSchemaMcp.Abstractions;

namespace SqlSchemaMcp.Data;

public sealed class CompareQueries(IEngineResolver resolver)
{
    public async Task<HashSet<string>> GetTableNames(string database, CancellationToken ct = default) =>
        resolver.TryResolve(database, out var e)
            ? ToSet(await e.GetTableNames(database, ct))
            : [];

    public async Task<HashSet<string>> GetProcNames(string database, CancellationToken ct = default) =>
        resolver.TryResolve(database, out var e)
            ? ToSet(await e.GetProcedureNames(database, ct))
            : [];

    public async Task<HashSet<string>> GetViewNames(string database, CancellationToken ct = default) =>
        resolver.TryResolve(database, out var e)
            ? ToSet(await e.GetViewNames(database, ct))
            : [];

    public async Task<List<ColumnInfo>> GetTableColumns(string database, string tableName, CancellationToken ct = default)
    {
        if (!resolver.TryResolve(database, out var e))
            return [];
        var columns = await e.GetTableColumns(database, tableName, ct);
        return [.. columns.Select(c => new ColumnInfo(c.Name, c.Type, c.Nullable))];
    }

    public async Task<(int LineCount, List<string> TablesReferenced)> GetProcStats(string database, string procName, CancellationToken ct = default)
    {
        if (!resolver.TryResolve(database, out var e))
            return (0, []);
        var stats = await e.GetProcedureStats(database, procName, ct);
        return (stats.LineCount, [.. stats.TablesReferenced]);
    }

    public async Task<(int LineCount, List<string> TablesReferenced)> GetViewStats(string database, string viewName, CancellationToken ct = default)
    {
        if (!resolver.TryResolve(database, out var e))
            return (0, []);
        var stats = await e.GetViewStats(database, viewName, ct);
        return (stats.LineCount, [.. stats.TablesReferenced]);
    }

    private static HashSet<string> ToSet(IReadOnlyCollection<string> names) =>
        new(names, StringComparer.OrdinalIgnoreCase);
}

public sealed record ColumnInfo(string Name, string Type, string Nullable);
```

- [ ] **Step 5: Commit**

```bash
git add Data/SchemaQueries.cs Data/DataQueries.cs Data/QueryQueries.cs Data/AnalysisQueries.cs Data/CompareQueries.cs
git commit -m "Rewrite cross-engine Data dispatchers to route through IDbEngine"
```

---

### Task 9: Rewrite the SQL-Server-only dispatchers (Diagnostics, Pipeline, Security)

**Files:**
- Rewrite: `Data/DiagnosticsQueries.cs`
- Rewrite: `Data/PipelineQueries.cs`
- Rewrite: `Data/SecurityQueries.cs`

**Interfaces:**
- Consumes: `IEngineResolver`, `Sentinels`, `DatabaseEngine` (Tasks 1, 7); `SqlServerDiagnostics`, `SqlServerPipeline`, `SqlServerSecurity` (Task 5)
- Produces: dispatcher classes named exactly `DiagnosticsQueries`, `PipelineQueries`, `SecurityQueries` in namespace `SqlSchemaMcp.Data` — `Tools/*.cs` compile unchanged. Every method returns the `UNSUPPORTED` sentinel for a non-SqlServer database.

- [ ] **Step 1: Rewrite `DiagnosticsQueries`**

`Data/DiagnosticsQueries.cs`:

```csharp
using System;
using System.Threading;
using System.Threading.Tasks;
using SqlSchemaMcp.Abstractions;
using SqlSchemaMcp.SqlServer;

namespace SqlSchemaMcp.Data;

public sealed class DiagnosticsQueries(IEngineResolver resolver, SqlServerDiagnostics sqlServer)
{
    public Task<string> ListAgentJobs(string database, CancellationToken ct = default) =>
        Route(database, nameof(ListAgentJobs), () => sqlServer.ListAgentJobs(database, ct));
    public Task<string> GetFailingJobs(string database, CancellationToken ct = default) =>
        Route(database, nameof(GetFailingJobs), () => sqlServer.GetFailingJobs(database, ct));
    public Task<string> GetJobHistory(string database, string jobName, int maxRuns, CancellationToken ct = default) =>
        Route(database, nameof(GetJobHistory), () => sqlServer.GetJobHistory(database, jobName, maxRuns, ct));
    public Task<string> ListLinkedServers(string database, CancellationToken ct = default) =>
        Route(database, nameof(ListLinkedServers), () => sqlServer.ListLinkedServers(database, ct));
    public Task<string> FindLinkedServerUsage(string database, CancellationToken ct = default) =>
        Route(database, nameof(FindLinkedServerUsage), () => sqlServer.FindLinkedServerUsage(database, ct));
    public Task<string> ListServiceBroker(string database, CancellationToken ct = default) =>
        Route(database, nameof(ListServiceBroker), () => sqlServer.ListServiceBroker(database, ct));
    public Task<string> ListClrAssemblies(string database, CancellationToken ct = default) =>
        Route(database, nameof(ListClrAssemblies), () => sqlServer.ListClrAssemblies(database, ct));
    public Task<string> AnalyzeTopExpensiveQueries(string database, int top, CancellationToken ct = default) =>
        Route(database, nameof(AnalyzeTopExpensiveQueries), () => sqlServer.AnalyzeTopExpensiveQueries(database, top, ct));
    public Task<string> AnalyzeWaitStats(string database, CancellationToken ct = default) =>
        Route(database, nameof(AnalyzeWaitStats), () => sqlServer.AnalyzeWaitStats(database, ct));

    private Task<string> Route(string database, string tool, Func<Task<string>> sqlServerCall)
    {
        if (!resolver.TryGetKind(database, out var kind))
            return Task.FromResult(Sentinels.UnknownDatabase(resolver.DatabaseNames, database));
        return kind == DatabaseEngine.SqlServer
            ? sqlServerCall()
            : Task.FromResult(Sentinels.Unsupported(tool, kind));
    }
}
```

Note for implementers: confirm the exact parameter lists of `GetJobHistory` and `AnalyzeTopExpensiveQueries` against `Tools/DiagnosticsTools.cs` and `SqlServerDiagnostics` when wiring — copy their signatures verbatim from the tool layer so the delegating calls match. The `Route` shape is identical for every method.

- [ ] **Step 2: Rewrite `PipelineQueries`**

`Data/PipelineQueries.cs`:

```csharp
using System;
using System.Threading;
using System.Threading.Tasks;
using SqlSchemaMcp.Abstractions;
using SqlSchemaMcp.SqlServer;

namespace SqlSchemaMcp.Data;

public sealed class PipelineQueries(IEngineResolver resolver, SqlServerPipeline sqlServer)
{
    public Task<string> ListDataFeeds(string database, CancellationToken ct = default) =>
        Route(database, nameof(ListDataFeeds), () => sqlServer.ListDataFeeds(database, ct));
    public Task<string> AnalyzeStagingHealth(string database, CancellationToken ct = default) =>
        Route(database, nameof(AnalyzeStagingHealth), () => sqlServer.AnalyzeStagingHealth(database, ct));
    public Task<string> CompareStagingToCurrentSchema(string database, string baseName, CancellationToken ct = default) =>
        Route(database, nameof(CompareStagingToCurrentSchema), () => sqlServer.CompareStagingToCurrentSchema(database, baseName, ct));

    private Task<string> Route(string database, string tool, Func<Task<string>> sqlServerCall)
    {
        if (!resolver.TryGetKind(database, out var kind))
            return Task.FromResult(Sentinels.UnknownDatabase(resolver.DatabaseNames, database));
        return kind == DatabaseEngine.SqlServer
            ? sqlServerCall()
            : Task.FromResult(Sentinels.Unsupported(tool, kind));
    }
}
```

Note: confirm `CompareStagingToCurrentSchema`'s exact parameters against `Tools/PipelineTools.cs` and copy them verbatim.

- [ ] **Step 3: Rewrite `SecurityQueries`**

`Data/SecurityQueries.cs`:

```csharp
using System;
using System.Threading;
using System.Threading.Tasks;
using SqlSchemaMcp.Abstractions;
using SqlSchemaMcp.SqlServer;

namespace SqlSchemaMcp.Data;

public sealed class SecurityQueries(IEngineResolver resolver, SqlServerSecurity sqlServer)
{
    public Task<string> ListDatabaseUsers(string database, CancellationToken ct = default) =>
        Route(database, nameof(ListDatabaseUsers), () => sqlServer.ListDatabaseUsers(database, ct));
    public Task<string> ListObjectPermissions(string database, string objectName, CancellationToken ct = default) =>
        Route(database, nameof(ListObjectPermissions), () => sqlServer.ListObjectPermissions(database, objectName, ct));

    private Task<string> Route(string database, string tool, Func<Task<string>> sqlServerCall)
    {
        if (!resolver.TryGetKind(database, out var kind))
            return Task.FromResult(Sentinels.UnknownDatabase(resolver.DatabaseNames, database));
        return kind == DatabaseEngine.SqlServer
            ? sqlServerCall()
            : Task.FromResult(Sentinels.Unsupported(tool, kind));
    }
}
```

Note: confirm `ListObjectPermissions`'s exact parameters against `Tools/SecurityTools.cs` and copy them verbatim.

- [ ] **Step 4: Commit**

```bash
git add Data/DiagnosticsQueries.cs Data/PipelineQueries.cs Data/SecurityQueries.cs
git commit -m "Rewrite SQL-Server-only Data dispatchers with UNSUPPORTED routing"
```

---

### Task 10: Teach the audit log about the `UNSUPPORTED:` sentinel

**Files:**
- Modify: `Auditing/FileAuditLog.cs:34`
- Modify: `Auditing/IAuditLog.cs` (doc comment)
- Test: `SqlSchemaMcp.Tests/Auditing/FileAuditLogTests.cs` (add one test — existing tests untouched)

**Interfaces:**
- Consumes: nothing new
- Produces: `FileAuditLog` records an `UNSUPPORTED:` result as `Success = false`

- [ ] **Step 1: Add a failing test (new test — existing four are unchanged)**

Append to `SqlSchemaMcp.Tests/Auditing/FileAuditLogTests.cs`, inside the class:

```csharp
    [Fact]
    public async Task Invoke_BodyReturnsUnsupportedString_RecordsFailure()
    {
        var sut = new FileAuditLog(Options.Create(new AuditOptions { Enabled = true, Path = _path }));

        var result = await sut.Invoke("ListAgentJobs", "pg", "",
            () => Task.FromResult("UNSUPPORTED: ListAgentJobs is not implemented for engine 'Postgres' yet. Tell the maintainer if you need it."));

        result.Should().StartWith("UNSUPPORTED:");
        var line = (await File.ReadAllLinesAsync(_path))[0];
        var entry = JsonSerializer.Deserialize<AuditEntry>(line, JsonSerializerOptions.Web);
        entry!.Success.Should().BeFalse();
    }
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `dotnet test SqlSchemaMcp.sln --filter FileAuditLogTests`
Expected: FAIL — the new test sees `Success = true` because the current check only looks for `ERROR:`.

- [ ] **Step 3: Update the success check**

In `Auditing/FileAuditLog.cs`, change line 34 from:

```csharp
            success = !result.StartsWith("ERROR:", StringComparison.Ordinal);
```

to:

```csharp
            success = !result.StartsWith("ERROR:", StringComparison.Ordinal)
                && !result.StartsWith("UNSUPPORTED:", StringComparison.Ordinal);
```

- [ ] **Step 4: Update the `IAuditLog` doc comment**

In `Auditing/IAuditLog.cs`, extend the summary's last sentence so it reads:

```csharp
    /// Success reflects both the absence of a thrown exception and that the result does not
    /// begin with the ERROR: or UNSUPPORTED: sentinels used by this codebase's Result-as-string convention.
```

- [ ] **Step 5: Run all audit tests to verify they pass**

Run: `dotnet test SqlSchemaMcp.sln --filter FileAuditLogTests`
Expected: PASS (5 tests — the original 4 plus the new one).

- [ ] **Step 6: Commit**

```bash
git add Auditing/FileAuditLog.cs Auditing/IAuditLog.cs SqlSchemaMcp.Tests/Auditing/FileAuditLogTests.cs
git commit -m "Record UNSUPPORTED tool results as audit failures"
```

---

### Task 11: Wire DI and the startup gate; finish project references and solution

**Files:**
- Modify: `Program.cs` (`RegisterServices`, `RunStartupGateAsync`, using directives)
- Modify: `SqlSchemaMcp.csproj` (add project refs, drop moved package refs)
- Modify: `SqlSchemaMcp.sln` (already has all four projects after Tasks 1/3; verify)

**Interfaces:**
- Consumes: `DatabaseConfigLoader` (Task 2), `EngineResolver` (Task 7), `SqlServerEngine` + its parts (Tasks 4/5), `SqlServerPermissionProbe` (Task 3), all dispatchers (Tasks 8/9)
- Produces: a runnable host that builds the engine graph, resolver, and startup gate over all configured databases

- [ ] **Step 1: Update the host csproj to reference the new projects**

`SqlSchemaMcp.csproj` — replace the `<ItemGroup>` blocks so ScriptDom/SqlClient are no longer direct host references (they come transitively via the SqlServer project) and add project references:

```xml
  <ItemGroup>
    <PackageReference Include="ModelContextProtocol" Version="1.1.0" />
    <PackageReference Include="ModelContextProtocol.AspNetCore" Version="1.1.0" />
  </ItemGroup>

  <ItemGroup>
    <ProjectReference Include="SqlSchemaMcp.Abstractions\SqlSchemaMcp.Abstractions.csproj" />
    <ProjectReference Include="SqlSchemaMcp.SqlServer\SqlSchemaMcp.SqlServer.csproj" />
  </ItemGroup>

  <ItemGroup>
    <Compile Remove="SqlSchemaMcp.Tests\**" />
    <Compile Remove="SqlSchemaMcp.Abstractions\**" />
    <Compile Remove="SqlSchemaMcp.SqlServer\**" />
  </ItemGroup>
```

- [ ] **Step 2: Rewrite `RegisterServices` and `RunStartupGateAsync` in `Program.cs`**

Replace the `RegisterServices` and `RunStartupGateAsync` static local functions (and add the needed usings `using System.Linq;`, `using SqlSchemaMcp.Abstractions;`, `using SqlSchemaMcp.Engines;`, `using SqlSchemaMcp.SqlServer;`) with:

```csharp
static void RegisterServices(IConfiguration configuration, IServiceCollection services)
{
    services.Configure<SecurityOptions>(configuration.GetSection("Security"));
    services.Configure<AuditOptions>(configuration.GetSection("Audit"));

    var databases = DatabaseConfigLoader.Load(configuration);

    var sqlServerDbs = databases
        .Where(d => d.Engine == DatabaseEngine.SqlServer)
        .ToDictionary(d => d.Name, d => d.ConnectionString, StringComparer.OrdinalIgnoreCase);
    services.Configure<SqlServerOptions>(o =>
    {
        foreach (var (name, cs) in sqlServerDbs)
            o.Databases[name] = cs;
    });

    // SQL Server engine parts
    services.AddSingleton<SqlServerSchema>();
    services.AddSingleton<SqlServerData>();
    services.AddSingleton<SqlServerQuery>();
    services.AddSingleton<SqlServerSchemaModels>();
    services.AddSingleton<SqlServerPermissionProbe>();
    services.AddSingleton<SqlServerEngine>();

    // SQL-Server-only concrete classes the dispatchers delegate to
    services.AddSingleton<SqlServerAnalysis>();
    services.AddSingleton<SqlServerDiagnostics>();
    services.AddSingleton<SqlServerPipeline>();
    services.AddSingleton<SqlServerSecurity>();

    services.AddSingleton<IEngineResolver>(sp =>
    {
        var engines = new Dictionary<DatabaseEngine, IDbEngine>
        {
            [DatabaseEngine.SqlServer] = sp.GetRequiredService<SqlServerEngine>(),
        };
        return new EngineResolver(databases, engines);
    });

    // Dispatchers (names referenced unchanged by Tools/*.cs)
    services.AddSingleton<SchemaQueries>();
    services.AddSingleton<AnalysisQueries>();
    services.AddSingleton<PipelineQueries>();
    services.AddSingleton<CompareQueries>();
    services.AddSingleton<DiagnosticsQueries>();
    services.AddSingleton<DataQueries>();
    services.AddSingleton<SecurityQueries>();
    services.AddSingleton<QueryQueries>();

    services.AddSingleton<IAuditLog, FileAuditLog>();
}

static async Task<bool> RunStartupGateAsync(IServiceProvider services, CancellationToken ct)
{
    var security = services.GetRequiredService<IOptions<SecurityOptions>>().Value;
    var resolver = services.GetRequiredService<IEngineResolver>();

    var results = new List<LoginPermissionResult>();
    foreach (var db in resolver.Databases)
    {
        if (resolver.TryResolve(db.Name, out var engine))
            results.Add(await engine.ProbePermissionsAsync(db.Name, db.ConnectionString, ct));
    }

    var decision = ReadOnlyStartupGate.Evaluate(results, security);

    foreach (var warning in decision.Warnings)
        Console.Error.WriteLine($"[SqlSchemaMcp] WARN: {warning}");
    foreach (var error in decision.Errors)
        Console.Error.WriteLine($"[SqlSchemaMcp] CRITICAL: {error}");

    return decision.ShouldStart;
}
```

The two `WithTools<...>()` chains and the entire HTTP/stdio branching stay exactly as they are — `Tools/*.cs` are untouched.

- [ ] **Step 3: Fix the `ReadOnlyStartupGate` using for the relocated `LoginPermissionResult`**

`Security/ReadOnlyStartupGate.cs` references `LoginPermissionResult`, which moved to `SqlSchemaMcp.Abstractions` (Task 1). Add `using SqlSchemaMcp.Abstractions;` to the top of that file. `GateDecision`, `ReadOnlyStartupGate`, and `SecurityOptions` stay in the host. Also add `using SqlSchemaMcp.Abstractions;` to `SqlSchemaMcp.Tests/Security/ReadOnlyStartupGateTests.cs` (the test constructs `LoginPermissionResult`); its assertions are unchanged.

- [ ] **Step 4: Build the whole solution**

Run: `dotnet build SqlSchemaMcp.sln`
Expected: `Build succeeded` with 0 errors across all four projects.

- [ ] **Step 5: Commit**

```bash
git add Program.cs SqlSchemaMcp.csproj Security/ReadOnlyStartupGate.cs SqlSchemaMcp.Tests/Security/ReadOnlyStartupGateTests.cs
git commit -m "Wire multi-engine DI graph, resolver, and multi-database startup gate"
```

---

### Task 12: Regression gate, docs, and config examples

**Files:**
- Modify: `appsettings.example.json`
- Modify: `README.md` (config section)
- Modify: `docs/security-posture.md` (per-engine note)
- Test: full suite

**Interfaces:**
- Consumes: everything above
- Produces: green build + green tests + updated docs

- [ ] **Step 1: Run the full test suite — the regression gate**

Run: `dotnet test SqlSchemaMcp.sln`
Expected: PASS. Confirm the original 35 tests still pass (SmokeTest 1; SqlStatementValidator 22; ReadOnlyStartupGate 7; SafeError 1; FileAuditLog now 5 — original 4 plus the new UNSUPPORTED test) **plus** the new Sentinels/Loader/Analyzer/Resolver tests. Total ≥ 35 with no failures. If any original test needed more than a `using`/project-reference change to pass, stop — that is a behaviour-change regression and must be fixed before proceeding.

- [ ] **Step 2: Verify `Tools/*.cs` are byte-for-byte unchanged (acceptance check)**

Run: `git diff --stat main -- Tools/`
Expected: **no output** — zero files under `Tools/` changed across the whole plan. If anything appears, the dispatcher signatures drifted from the tool layer; reconcile the dispatcher to match the tool, not the tool to match the dispatcher.

- [ ] **Step 3: Update `appsettings.example.json` to show both config forms**

Replace the `SqlServer.Databases` block:

```json
  "SqlServer": {
    "Databases": {
      "poc":   "Server=YOUR_SERVER;Database=YOUR_POC_DB;User Id=sqlschema_ro;Password=YOUR_SECRET;TrustServerCertificate=true;",
      "azure": "Server=YOUR_SERVER.database.windows.net;Database=YOUR_AZURE_DB;Authentication=Active Directory Default;",
      "reporting": {
        "Engine": "Postgres",
        "ConnectionString": "Host=YOUR_PG_HOST;Database=YOUR_PG_DB;Username=sqlschema_ro;Password=YOUR_SECRET;"
      },
      "legacy": {
        "Engine": "MariaDb",
        "ConnectionString": "Server=YOUR_MARIA_HOST;Database=YOUR_MARIA_DB;Uid=sqlschema_ro;Pwd=YOUR_SECRET;"
      }
    }
  }
```

Add a one-line note above it in the file's neighbouring README/comment (or in README) that a bare string implies `Engine: SqlServer` and the object form is required for other engines.

- [ ] **Step 4: Add a "Multiple engines" note to `README.md` and `docs/security-posture.md`**

In `README.md`, under the configuration section, document: bare string = SqlServer (unchanged for existing deployments); object form `{ "Engine": "...", "ConnectionString": "..." }` for `Postgres` / `MariaDb`; and that Diagnostics, Pipeline, Security, and non-shared Analysis tools return an `UNSUPPORTED:` message for non-SqlServer databases.

In `docs/security-posture.md`, add a short subsection "Per-engine enforcement (Postgres/MariaDB)" stating that the startup permission gate runs for every configured database via its engine's probe, and that the SQL Server ScriptDom validator is SQL-Server-specific — Postgres and MariaDB engines will enforce read-only via native read-only transactions plus a single-statement guard (implemented in their respective plans). Mark it as "SQL Server today; other engines land with their engine plans."

- [ ] **Step 5: Manual smoke test against a real SQL Server (zero-behaviour-change confirmation)**

With a valid `appsettings.json` pointing at a SQL Server `db_datareader` login, run: `dotnet run --project SqlSchemaMcp -- --sse` and confirm it starts (startup gate passes) and that `curl http://localhost:5101/` returns `{"status":"ok",...}`. Spot-check that `ListTables`, `AnalyzeNamingConventions`, and `CompareTables` produce output identical to `main` for the same database. Stop the server.

- [ ] **Step 6: Commit**

```bash
git add appsettings.example.json README.md docs/security-posture.md
git commit -m "Document multi-engine config shape and per-engine security posture"
```

---

## Self-Review

**Spec coverage:** engine abstraction (Task 1), shared models (Task 1), new config shape + manual loader verbatim (Task 2), migrate 100% of existing SQL Server logic (Tasks 3–5, 8, 9), Analysis+Compare rewritten once over the shared model (Tasks 4, 6, 8), dispatcher keeps `SchemaQueries` name so Tools never change (Tasks 8, 9, 12 acceptance check), project split with exact csproj wiring and sln updates (Tasks 1, 3, 11), audit `UNSUPPORTED` decision made and justified (Task 10), 35 tests pass unchanged except allowed namespace/project-ref updates (Tasks 3, 11, 12), security-posture doc updated (Task 12). All covered.

**Placeholder scan:** no TBD/TODO; every new file has full code; relocations specify the exact edits to apply to existing verbatim files; the only "confirm against tool layer" notes are for parameter lists of a handful of Diagnostics/Pipeline/Security methods that were not fully read line-by-line, with an explicit instruction to copy them verbatim from the untouched `Tools/*.cs` — not an invitation to invent.

**Type consistency:** `IDbEngine`, `SchemaSnapshot`, `SchemaObject`, `SchemaColumn`, `ColumnTypeCategory`, `DbColumn`, `RoutineStats`, `LoginPermissionResult`, `IEngineResolver`, `Sentinels`, `DatabaseConfig` are all defined once in Task 1 and referenced with identical signatures in Tasks 4–9. `EngineResolver` ctor `(IReadOnlyList<DatabaseConfig>, IReadOnlyDictionary<DatabaseEngine, IDbEngine>)` matches Task 7 and Task 11 usage.
