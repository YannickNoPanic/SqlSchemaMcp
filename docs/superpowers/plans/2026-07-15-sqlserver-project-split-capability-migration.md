# SQL Server Project Split Capability Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Split SQL Server implementation into `SqlSchemaMcp.SqlServer` and migrate the host to capability dispatchers without changing the MCP tool surface or current SQL Server behavior.

**Architecture:** Keep `SqlSchemaMcp.Abstractions` as the pure contract layer. Add `SqlSchemaMcp.SqlServer` as the concrete SQL Server engine project. Keep `SqlSchemaMcp` as the host/composition project containing `Tools/*`, audit, config loading, resolver, and thin `Data/*Queries.cs` dispatchers.

**Tech Stack:** .NET 10, C#, Microsoft.Data.SqlClient, Microsoft.SqlServer.TransactSql.ScriptDom, ModelContextProtocol, xUnit, FluentAssertions, NSubstitute.

## Global Constraints

- Execute phases sequentially and automatically only while the current phase build, tests, and review are clean.
- Stop on any Critical or Important review finding, unexplained test failure, output drift, or need to change `Tools/*.cs`.
- Use project-local worktrees under `.codex/worktrees` or `.claude/worktrees`; if the harness cannot write there, ask before using another location.
- `Tools/*.cs` must remain byte-for-byte unchanged for all phases.
- No broad `IDbEngine` or god interface.
- Engines implement focused capability interfaces only.
- `UNSUPPORTED:` remains a user-facing message only; no remote notification or request tracking.
- `UNSUPPORTED:` is audited as `Success = false`.
- Existing SQL Server behavior must remain unchanged unless a phase explicitly documents a verified, intentional difference.
- Use CRLF line endings for all changed files.
- Use file-scoped namespaces.
- Async methods must accept/pass `CancellationToken`.
- Prefer comments only for non-obvious migration anchors. The user explicitly allows `TODO:` comments in this migration when they preserve important follow-up work, but do not leave TODOs for incomplete required functionality.
- Do not delete existing SQL Server behavior. Move or wrap it, then verify.
- `SqlServerOptions` stays SQL Server-specific and must not move into `SqlSchemaMcp.Abstractions`.

---

## Current Starting Point

Phase 1 is already merged:

- `SqlSchemaMcp.Abstractions` exists.
- `DatabaseConfigLoader` exists.
- `CapabilityResolver` exists.
- `Sentinels.Unsupported(...)` exists.
- Audit treats `UNSUPPORTED:` as failure.

Known local caveat before executing this plan:

- The current main checkout has unrelated dirty config-validation work and an untracked failing `SqlServerOptionsTests.cs`.
- Before executing this plan, either commit/stash that work or execute from a clean worktree based on the latest `main` commit.
- Do not use dirty local test failures as phase regressions.

---

## Target Project Structure

```text
SqlSchemaMcp.Abstractions/
  Capabilities/
    ISchemaCapability.cs
    ISqlServerSchemaExtrasCapability.cs
    IReadOnlyQueryCapability.cs
    IDataSamplingCapability.cs
    ISchemaSnapshotCapability.cs
    ISqlServerDiagnosticsCapability.cs
    ISqlServerPipelineCapability.cs
    ISqlServerSecurityCapability.cs
    ISqlServerAnalysisCapability.cs
  DatabaseConfig.cs
  DatabaseEngine.cs
  ICapabilityResolver.cs
  SchemaModels.cs
  Sentinels.cs

SqlSchemaMcp.SqlServer/
  Configuration/
    SqlServerEngineOptions.cs
  Data/
    SqlQueryBase.cs
    SqlServerQuery.cs
    SqlServerSchema.cs
    SqlServerSchemaExtras.cs
    SqlServerDataSampling.cs
    SqlServerDiagnostics.cs
    SqlServerPipeline.cs
    SqlServerSecurity.cs
    SqlServerAnalysis.cs
    SqlServerSchemaSnapshot.cs
    SqlStatementValidator.cs
  Security/
    SqlServerPermissionProbe.cs

SqlSchemaMcp/
  Configuration/
    DatabaseConfigLoader.cs
    SecurityOptions.cs
    AuditOptions.cs
  Data/
    QueryQueries.cs
    SchemaQueries.cs
    DataQueries.cs
    DiagnosticsQueries.cs
    PipelineQueries.cs
    SecurityQueries.cs
    AnalysisQueries.cs
    CompareQueries.cs
    ConstraintRepository.cs
  Engines/
    CapabilityResolver.cs
  Security/
    IPermissionProbe.cs
    ReadOnlyStartupGate.cs
  Tools/
    unchanged
```

Notes:

- `SqlSchemaMcp.SqlServer` owns `Microsoft.Data.SqlClient` and `Microsoft.SqlServer.TransactSql.ScriptDom`.
- The host should no longer need direct SqlClient or ScriptDom package references once all SQL Server code is migrated.
- `SqlSchemaMcp.Tests` stays as the fast unit test project for host + abstractions + SQL Server unit tests.

---

## Automatic Execution Gates

Every phase must end with:

```powershell
dotnet build SqlSchemaMcp.sln --no-restore
dotnet test SqlSchemaMcp.sln --no-restore
git diff -- Tools/
git diff --check
```

Expected:

- Build succeeds.
- Tests pass.
- `git diff -- Tools/` has no output.
- `git diff --check` has no output.

If a test fails because of unrelated dirty local work, stop and report. Do not guess.

Each phase must be committed separately. Do not batch phases into one commit.

---

## Phase 2A: Create `SqlSchemaMcp.SqlServer` and Move SQL Server Primitives

**Files:**
- Create: `SqlSchemaMcp.SqlServer/SqlSchemaMcp.SqlServer.csproj`
- Create: `SqlSchemaMcp.SqlServer/Configuration/SqlServerEngineOptions.cs`
- Move: `Data/SqlQueryBase.cs` -> `SqlSchemaMcp.SqlServer/Data/SqlQueryBase.cs`
- Move: `Data/SqlStatementValidator.cs` -> `SqlSchemaMcp.SqlServer/Data/SqlStatementValidator.cs`
- Move: `Security/SqlServerPermissionProbe.cs` -> `SqlSchemaMcp.SqlServer/Security/SqlServerPermissionProbe.cs`
- Modify: namespaces in moved files
- Modify: `SqlSchemaMcp.sln`
- Modify: `SqlSchemaMcp.csproj`
- Modify: `SqlSchemaMcp.Tests/SqlSchemaMcp.Tests.csproj`
- Modify: tests that reference moved types

**Interfaces:**
- Produces: `SqlSchemaMcp.SqlServer.Configuration.SqlServerEngineOptions`
- Produces: `SqlSchemaMcp.SqlServer.Data.SqlQueryBase`
- Produces: `SqlSchemaMcp.SqlServer.Data.SqlStatementValidator`
- Produces: `SqlSchemaMcp.SqlServer.Security.SqlServerPermissionProbe`
- Consumes: existing host `SqlServerOptions` for now only as migration reference.

- [ ] **Step 1: Create SQL Server project**

Create `SqlSchemaMcp.SqlServer/SqlSchemaMcp.SqlServer.csproj`:

```xml
<Project Sdk="Microsoft.NET.Sdk">

  <PropertyGroup>
    <TargetFramework>net10.0</TargetFramework>
    <Nullable>enable</Nullable>
    <ImplicitUsings>enable</ImplicitUsings>
    <RootNamespace>SqlSchemaMcp.SqlServer</RootNamespace>
  </PropertyGroup>

  <ItemGroup>
    <PackageReference Include="Microsoft.Data.SqlClient" Version="7.0.0" />
    <PackageReference Include="Microsoft.SqlServer.TransactSql.ScriptDom" Version="180.37.3" />
  </ItemGroup>

  <ItemGroup>
    <ProjectReference Include="..\SqlSchemaMcp.Abstractions\SqlSchemaMcp.Abstractions.csproj" />
  </ItemGroup>

</Project>
```

- [ ] **Step 2: Add SQL Server engine options**

Create `SqlSchemaMcp.SqlServer/Configuration/SqlServerEngineOptions.cs`:

```csharp
namespace SqlSchemaMcp.SqlServer.Configuration;

public sealed class SqlServerEngineOptions
{
    public Dictionary<string, string> Databases { get; init; } = new(StringComparer.OrdinalIgnoreCase);
}
```

- [ ] **Step 3: Move primitives**

Use `git mv`:

```powershell
New-Item -ItemType Directory -Force -Path SqlSchemaMcp.SqlServer/Data, SqlSchemaMcp.SqlServer/Security, SqlSchemaMcp.SqlServer/Configuration
git mv Data/SqlQueryBase.cs SqlSchemaMcp.SqlServer/Data/SqlQueryBase.cs
git mv Data/SqlStatementValidator.cs SqlSchemaMcp.SqlServer/Data/SqlStatementValidator.cs
git mv Security/SqlServerPermissionProbe.cs SqlSchemaMcp.SqlServer/Security/SqlServerPermissionProbe.cs
```

Update namespaces:

- `namespace SqlSchemaMcp.Data;` -> `namespace SqlSchemaMcp.SqlServer.Data;`
- `namespace SqlSchemaMcp.Security;` -> `namespace SqlSchemaMcp.SqlServer.Security;`

Update `SqlQueryBase` to consume `IOptions<SqlServerEngineOptions>` from `SqlSchemaMcp.SqlServer.Configuration`.

- [ ] **Step 4: Add solution/project references**

Run:

```powershell
dotnet sln SqlSchemaMcp.sln add SqlSchemaMcp.SqlServer/SqlSchemaMcp.SqlServer.csproj
dotnet add SqlSchemaMcp.csproj reference SqlSchemaMcp.SqlServer/SqlSchemaMcp.SqlServer.csproj
dotnet add SqlSchemaMcp.Tests/SqlSchemaMcp.Tests.csproj reference SqlSchemaMcp.SqlServer/SqlSchemaMcp.SqlServer.csproj
```

Update `SqlSchemaMcp.csproj` nested project exclusions:

```xml
<Compile Remove="SqlSchemaMcp.SqlServer\**" />
<Content Remove="SqlSchemaMcp.SqlServer\**" />
<EmbeddedResource Remove="SqlSchemaMcp.SqlServer\**" />
<None Remove="SqlSchemaMcp.SqlServer\**" />
```

- [ ] **Step 5: Fix moved type references**

Update tests:

- `SqlSchemaMcp.Tests/Data/SqlStatementValidatorTests.cs` uses `SqlSchemaMcp.SqlServer.Data`.
- `SqlSchemaMcp.Tests/Data/SafeErrorTests.cs` uses `SqlSchemaMcp.SqlServer.Data` and `SqlSchemaMcp.SqlServer.Configuration`.

Update `Program.cs` temporarily:

- Register `SqlServerPermissionProbe` from `SqlSchemaMcp.SqlServer.Security`.
- Configure `SqlServerEngineOptions` from existing `SqlServerOptions.Databases` until Phase 2B rewires config.

- [ ] **Step 6: Verify**

Run:

```powershell
dotnet build SqlSchemaMcp.sln --no-restore
dotnet test SqlSchemaMcp.sln --no-restore
git diff -- Tools/
git diff --check
```

- [ ] **Step 7: Commit**

```powershell
git add SqlSchemaMcp.SqlServer SqlSchemaMcp.sln SqlSchemaMcp.csproj SqlSchemaMcp.Tests Program.cs
git commit -m "Create SQL Server engine project and move SQL primitives"
```

---

## Phase 2B: Query Capability Slice

**Files:**
- Move/rewrite: `Data/QueryQueries.cs`
- Create: `SqlSchemaMcp.SqlServer/Data/SqlServerQuery.cs`
- Modify: `Program.cs`
- Test: `SqlSchemaMcp.Tests/Data/QueryQueriesDispatcherTests.cs`

**Interfaces:**
- Consumes: `IReadOnlyQueryCapability`
- Produces: `SqlServerQuery : SqlQueryBase, IReadOnlyQueryCapability`
- Produces: host `QueryQueries(ICapabilityResolver resolver)` dispatcher.

- [ ] **Step 1: Move SQL implementation**

Create `SqlSchemaMcp.SqlServer/Data/SqlServerQuery.cs` by moving the current SQL body from `Data/QueryQueries.cs`.

The public method remains:

```csharp
public Task<string> ExecuteQuery(string database, string sql, CancellationToken cancellationToken = default)
```

`SqlServerQuery` implements `IReadOnlyQueryCapability`.

- [ ] **Step 2: Replace host `QueryQueries` with dispatcher**

`Data/QueryQueries.cs` becomes:

```csharp
using SqlSchemaMcp.Abstractions;
using SqlSchemaMcp.Abstractions.Capabilities;

namespace SqlSchemaMcp.Data;

public sealed class QueryQueries(ICapabilityResolver resolver)
{
    public Task<string> ExecuteQuery(string database, string sql, CancellationToken cancellationToken = default)
    {
        if (resolver.TryResolve<IReadOnlyQueryCapability>(database, out _, out var capability))
            return capability.ExecuteQuery(database, sql, cancellationToken);

        return Task.FromResult(
            resolver.TryGetEngine(database, out var engine)
                ? Sentinels.Unsupported(nameof(ExecuteQuery), engine)
                : Sentinels.UnknownDatabase(resolver.DatabaseNames, database));
    }
}
```

- [ ] **Step 3: Add dispatcher tests**

Create `SqlSchemaMcp.Tests/Data/QueryQueriesDispatcherTests.cs` with tests for:

- unknown database returns `ERROR: Unknown database`.
- known database without `IReadOnlyQueryCapability` returns `UNSUPPORTED:`.
- known database with fake query capability delegates exactly once and returns result.

- [ ] **Step 4: Wire DI**

In `Program.cs`:

- Load `DatabaseConfigLoader.Load(configuration)`.
- Build SQL Server database dictionary from configs where `Engine == DatabaseEngine.SqlServer`.
- Configure `SqlServerEngineOptions`.
- Register `SqlServerQuery`.
- Register resolver engine map so SQL Server engine object includes `IReadOnlyQueryCapability`.

If a full `SqlServerEngine` aggregator is needed, create:

```csharp
public sealed class SqlServerEngine(SqlServerQuery query) : IDatabaseEngine, IReadOnlyQueryCapability
{
    public DatabaseEngine Kind => DatabaseEngine.SqlServer;
    public Task<string> ExecuteQuery(string database, string sql, CancellationToken ct) =>
        query.ExecuteQuery(database, sql, ct);
}
```

TODO allowed: add `// TODO(multi-engine): add the next SQL Server capability to this aggregator in the phase that migrates it.` only if it helps future phase tracking.

- [ ] **Step 5: Verify and commit**

Run phase gates and commit:

```powershell
git add Data/QueryQueries.cs SqlSchemaMcp.SqlServer Program.cs SqlSchemaMcp.Tests/Data/QueryQueriesDispatcherTests.cs
git commit -m "Route execute_query through SQL Server query capability"
```

---

## Phase 2C: Schema Capability Slice

**Files:**
- Move/rewrite: `Data/SchemaQueries.cs`
- Create: `SqlSchemaMcp.SqlServer/Data/SqlServerSchema.cs`
- Create: `SqlSchemaMcp.SqlServer/Data/SqlServerSchemaExtras.cs`
- Modify: `Program.cs`
- Test: `SqlSchemaMcp.Tests/Data/SchemaQueriesDispatcherTests.cs`

**Interfaces:**
- Consumes: `ISchemaCapability`, `ISqlServerSchemaExtrasCapability`
- Produces: SQL Server schema capability classes.

- [ ] **Step 1: Split schema implementation**

Move current `SchemaQueries` SQL bodies:

- Shared schema browsing methods to `SqlServerSchema : SqlQueryBase, ISchemaCapability`.
- SQL Server-specific methods to `SqlServerSchemaExtras : SqlQueryBase, ISqlServerSchemaExtrasCapability`.

Shared:

- `ListTables`
- `ListViews`
- `ListProcedures`
- `ListFunctions`
- `GetTableSchema`
- `GetViewDefinition`
- `GetProcedureDefinition`
- `GetFunctionDefinition`
- `FindReferences`
- `SearchDefinitions`

SQL Server extras:

- `ListTriggers`
- `GetTriggerDefinition`
- `ListSynonyms`
- `ListCheckConstraints`
- `ListDdlTriggers`
- `GetDdlTriggerDefinition`

- [ ] **Step 2: Replace host `SchemaQueries` with dispatcher**

`SchemaQueries` routes each method to the smallest required capability.

Unknown database:

```csharp
Sentinels.UnknownDatabase(resolver.DatabaseNames, database)
```

Known engine without capability:

```csharp
Sentinels.Unsupported(nameof(MethodName), engine)
```

- [ ] **Step 3: Add dispatcher tests**

Use fake capability objects and `CapabilityResolver`.

Cover:

- shared method delegates.
- SQL Server extra method delegates.
- extra method on fake Postgres engine returns `UNSUPPORTED:`.
- unknown database returns `ERROR:`.

- [ ] **Step 4: Wire DI and aggregator**

Extend `SqlServerEngine` to implement:

- `ISchemaCapability`
- `ISqlServerSchemaExtrasCapability`

Delegate to `SqlServerSchema` and `SqlServerSchemaExtras`.

- [ ] **Step 5: Verify and commit**

Run phase gates and commit:

```powershell
git add Data/SchemaQueries.cs SqlSchemaMcp.SqlServer Program.cs SqlSchemaMcp.Tests/Data/SchemaQueriesDispatcherTests.cs
git commit -m "Route schema tools through SQL Server capabilities"
```

---

## Phase 2D: Data Sampling Capability Slice

**Files:**
- Move/rewrite: `Data/DataQueries.cs`
- Create: `SqlSchemaMcp.SqlServer/Data/SqlServerDataSampling.cs`
- Modify: `Program.cs`
- Test: `SqlSchemaMcp.Tests/Data/DataQueriesDispatcherTests.cs`

**Interfaces:**
- Consumes: `IDataSamplingCapability`
- Produces: `SqlServerDataSampling : SqlQueryBase, IDataSamplingCapability`

- [ ] **Step 1: Move implementation**

Move current `DataQueries` SQL bodies to `SqlServerDataSampling`.

Methods:

- `SampleTableData`
- `AnalyzeColumnDistribution`
- `FindNullableColumnsWithNoNulls`
- `FindDuplicateRows`

- [ ] **Step 2: Replace host `DataQueries` with dispatcher**

Route each method through `IDataSamplingCapability`.

- [ ] **Step 3: Add dispatcher tests**

Cover delegation, unknown database, and unsupported capability.

- [ ] **Step 4: Wire DI and aggregator**

Extend `SqlServerEngine` to implement `IDataSamplingCapability`.

- [ ] **Step 5: Verify and commit**

```powershell
git add Data/DataQueries.cs SqlSchemaMcp.SqlServer Program.cs SqlSchemaMcp.Tests/Data/DataQueriesDispatcherTests.cs
git commit -m "Route data sampling tools through SQL Server capability"
```

---

## Phase 2E: SQL Server-Only Operational Capabilities

**Files:**
- Add abstractions:
  - `ISqlServerPipelineCapability`
  - `ISqlServerSecurityCapability`
  - `ISqlServerAnalysisCapability`
- Move/rewrite:
  - `Data/DiagnosticsQueries.cs`
  - `Data/PipelineQueries.cs`
  - `Data/SecurityQueries.cs`
  - SQL Server-only portions of `Data/AnalysisQueries.cs`
- Create:
  - `SqlSchemaMcp.SqlServer/Data/SqlServerDiagnostics.cs`
  - `SqlSchemaMcp.SqlServer/Data/SqlServerPipeline.cs`
  - `SqlSchemaMcp.SqlServer/Data/SqlServerSecurity.cs`
  - `SqlSchemaMcp.SqlServer/Data/SqlServerAnalysis.cs`
- Modify: `Program.cs`
- Tests:
  - `DiagnosticsQueriesDispatcherTests`
  - `PipelineQueriesDispatcherTests`
  - `SecurityQueriesDispatcherTests`
  - `AnalysisQueriesSqlServerOnlyDispatcherTests`

**Interfaces:**
- Produces focused SQL Server-only capabilities.
- Keeps non-SQL Server engines isolated from SQL Server operational concepts.

- [ ] **Step 1: Add SQL Server-only capability interfaces**

Create interfaces in `SqlSchemaMcp.Abstractions/Capabilities`.

`ISqlServerPipelineCapability`:

- `ListDataFeeds`
- `AnalyzeStagingHealth`
- `CompareStagingToCurrentSchema`

`ISqlServerSecurityCapability`:

- `ListDatabaseUsers`
- `ListObjectPermissions`

`ISqlServerAnalysisCapability` for SQL Server-only analysis methods:

- `AnalyzeProcComplexity`
- `AnalyzeViewComplexity`
- `AnalyzeDuplicateIndexes`
- `FindUnusedTables`
- `FindUnusedProcedures`
- `AnalyzeIndexFragmentation`
- `AnalyzeTriggers`
- `AnalyzeIdentityColumns`
- `AnalyzeTableSizes`
- `AnalyzeMissingIndexSuggestions`
- `GetRecentObjectChanges`
- `AnalyzeTableQueryStats`
- `AnalyzeTableAccessStats`
- `GenerateDatabaseSummary`

- [ ] **Step 2: Move SQL Server-only implementations**

Move existing SQL bodies to matching SQL Server project classes.

Keep method signatures exactly matching current host `Data/*Queries.cs` methods.

- [ ] **Step 3: Replace host dispatchers**

Host dispatchers route to corresponding SQL Server-only capability.

For known non-SQL Server engine:

```text
UNSUPPORTED: Tool 'ToolName' is not available for engine 'Postgres'. Ask the maintainer to add support if you need this.
```

- [ ] **Step 4: Wire DI and aggregator**

Extend `SqlServerEngine` to implement all SQL Server-only capabilities.

- [ ] **Step 5: Verify and commit**

Run phase gates and commit:

```powershell
git add SqlSchemaMcp.Abstractions SqlSchemaMcp.SqlServer Data Program.cs SqlSchemaMcp.Tests
git commit -m "Route SQL Server operational tools through capabilities"
```

---

## Phase 2F: Schema Snapshot Capability and Shared Analysis/Compare

**Files:**
- Create: `SqlSchemaMcp.SqlServer/Data/SqlServerSchemaSnapshot.cs`
- Create: `Data/SharedAnalysis/NamingAnalyzer.cs`
- Create: `Data/SharedAnalysis/MissingForeignKeyAnalyzer.cs`
- Create: `Data/SharedAnalysis/MissingIndexAnalyzer.cs`
- Rewrite parts of `Data/AnalysisQueries.cs`
- Rewrite or adapt `Data/CompareQueries.cs`
- Tests:
  - shared analyzer golden output tests
  - compare dispatcher tests
  - SQL Server snapshot model tests where feasible without live DB

**Interfaces:**
- Consumes: `ISchemaSnapshotCapability`, `SchemaSnapshot`
- Produces shared analyzer functions that are engine-agnostic.

- [ ] **Step 1: Add golden output tests before rewrite**

Create focused tests for current output formatting of:

- `AnalyzeNamingConventions`
- `AnalyzeMissingForeignKeys`
- `AnalyzeMissingIndexes`

Use in-memory `SchemaSnapshot` test data for the new analyzers, not live SQL Server.

Expected: tests describe the intended output contract before replacing dispatcher paths.

- [ ] **Step 2: Add SQL Server snapshot provider**

Create `SqlServerSchemaSnapshot : SqlQueryBase, ISchemaSnapshotCapability`.

It queries SQL Server catalog metadata and returns:

- objects
- columns
- FK column keys
- PK column keys
- indexed column keys

Key format:

```csharp
$"{schema}.{table}.{column}"
```

Use `StringComparer.OrdinalIgnoreCase` for all key sets.

Add `TODO(multi-engine): verify PostgreSQL/MariaDB type-category mapping matches this semantic contract before implementing their engines.`

- [ ] **Step 3: Add shared analyzers**

Create pure classes:

- `NamingAnalyzer.Build(string database, SchemaSnapshot snapshot)`
- `MissingForeignKeyAnalyzer.Build(string database, SchemaSnapshot snapshot)`
- `MissingIndexAnalyzer.Build(string database, SchemaSnapshot snapshot)`

They must not reference SQL Server types, SqlClient, or host config.

- [ ] **Step 4: Rewrite shared analysis dispatcher paths**

`AnalysisQueries` routes:

- shared methods through `ISchemaSnapshotCapability` + shared analyzers.
- SQL Server-only methods through `ISqlServerAnalysisCapability`.

- [ ] **Step 5: Compare migration**

Refactor `CompareQueries` to use capability-based retrieval.

Minimum acceptable first pass:

- Use `ISchemaSnapshotCapability` for table/view/proc set comparisons.
- Keep table column/proc/view stats via SQL Server capability only if shared model is not rich enough yet.
- Add `TODO(multi-engine): promote routine stats into shared snapshot model if PostgreSQL/MariaDB compare requires it.`

This TODO is explicitly allowed as a migration marker because routine stats normalization is a future engine design decision.

- [ ] **Step 6: Verify and commit**

Run phase gates and commit:

```powershell
git add Data SqlSchemaMcp.SqlServer Program.cs SqlSchemaMcp.Tests
git commit -m "Add schema snapshot capability and shared analysis routing"
```

---

## Phase 2G: Documentation and Runtime Config Finish

**Files:**
- Modify: `README.md`
- Modify: `docs/security-posture.md`
- Modify: `appsettings.example.json`
- Modify: `.env.example`
- Modify: `Program.cs` if needed

**Interfaces:**
- Consumes all previous phases.
- Produces documented multi-engine config semantics and SQL Server project split notes.

- [ ] **Step 1: Document current runtime support**

Document:

- Bare string config means SQL Server.
- Object config can name future engines.
- At the end of this phase, only SQL Server implementation exists.
- Non-SQL Server engines return `UNSUPPORTED:` until their engine projects are implemented.

- [ ] **Step 2: Document security posture**

Add a section:

```text
SQL Server today; PostgreSQL/MariaDB later
```

Explain:

- SQL Server uses login permission probe and ScriptDom query validator.
- Future engines must implement their own read-only permission probe and statement guard.

- [ ] **Step 3: Verify and commit**

Run phase gates and commit:

```powershell
git add README.md docs/security-posture.md appsettings.example.json .env.example Program.cs
git commit -m "Document SQL Server engine split and multi-engine runtime semantics"
```

---

## Phase 2H: Final Branch Verification

- [ ] **Step 1: Run full verification**

```powershell
dotnet restore SqlSchemaMcp.sln
dotnet build SqlSchemaMcp.sln --no-restore
dotnet test SqlSchemaMcp.sln --no-restore
git diff -- Tools/
git diff --check
```

Expected:

- Restore succeeds.
- Build succeeds.
- Tests pass.
- `Tools/*.cs` unchanged.
- No whitespace/line-ending errors.

- [ ] **Step 2: Inspect project dependency direction**

Expected:

```text
SqlSchemaMcp.Abstractions -> no project refs
SqlSchemaMcp.SqlServer -> Abstractions only
SqlSchemaMcp -> Abstractions + SqlServer
SqlSchemaMcp.Tests -> host + Abstractions + SqlServer
```

No SQL Server project references host.

- [ ] **Step 3: Final code review**

Dispatch final review over the whole branch.

Reviewer must explicitly check:

- no god interface introduced.
- `Tools/*.cs` unchanged.
- SQL Server packages moved out of host unless still justified.
- runtime config remains backward compatible.
- `UNSUPPORTED:` semantics are consistent.
- migration TODOs are intentional and actionable.

- [ ] **Step 4: Finish branch**

Use `superpowers:finishing-a-development-branch`.

---

## Expected Commit Sequence

```text
Create SQL Server engine project and move SQL primitives
Route execute_query through SQL Server query capability
Route schema tools through SQL Server capabilities
Route data sampling tools through SQL Server capability
Route SQL Server operational tools through capabilities
Add schema snapshot capability and shared analysis routing
Document SQL Server engine split and multi-engine runtime semantics
```

Extra commits are acceptable only for:

- review fixes.
- build/test infrastructure fixes discovered by systematic debugging.
- line-ending normalization.

---

## Self-Review

**Spec coverage:** This plan implements direct SQL Server project split, capability dispatchers, SQL Server-only routing, shared snapshot analysis, documentation, and final verification. It keeps `Tools/*.cs` unchanged and avoids a god interface.

**Placeholder scan:** The plan contains intentional `TODO(multi-engine):` migration markers only where future PostgreSQL/MariaDB engine semantics need design input. These are allowed by the user for this large refactor and are not placeholders for required Phase 2 functionality.

**Type consistency:** Uses existing Phase 1 contracts: `DatabaseEngine`, `DatabaseConfig`, `Sentinels`, `ICapabilityResolver`, `ISchemaCapability`, `ISqlServerSchemaExtrasCapability`, `IReadOnlyQueryCapability`, `IDataSamplingCapability`, `ISchemaSnapshotCapability`, and `ISqlServerDiagnosticsCapability`. New SQL Server-only capability names are introduced in Phase 2E and consumed only after that phase.
