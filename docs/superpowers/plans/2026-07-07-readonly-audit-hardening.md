# Read-Only Enforcement, Audit Logging & Error Sanitisation — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the SQL Schema MCP defensibly read-only and auditable so a security-conscious lead developer will install it, while keeping the `execute_query` SELECT tool that is used daily for debugging.

**Architecture:** Three defence layers plus honesty. (1) A startup permission gate that verifies each configured login cannot write and refuses to start otherwise. (2) A ScriptDom-based allowlist validator that permits only `SELECT`/`WITH` statements and blocks `SELECT INTO`, `OPENQUERY`/`OPENROWSET`/`OPENDATASOURCE`, `WAITFOR`, and multi-statement batches — replacing the bypassable keyword denylist. (3) A structured, file-backed audit log recording every tool invocation. Plus sanitised error messages that never leak connection/server details to the model, and a rewritten product promise that admits controlled data access.

**Tech Stack:** .NET 10, C#, `Microsoft.Data.SqlClient`, `Microsoft.SqlServer.TransactSql.ScriptDom` (new), `ModelContextProtocol`. Tests: xUnit + FluentAssertions + NSubstitute (house style). Optional integration tests via Testcontainers for SQL Server.

## Global Constraints

- Target framework: `net10.0`. Nullable + ImplicitUsings enabled (already set in `SqlSchemaMcp.csproj`).
- House C# style: file-scoped namespaces, `using` outside namespace (System first), primary constructors where possible, `var` only when type is obvious, `CancellationToken` on every async method, structured logging only (no interpolation in log templates), `CultureInfo.InvariantCulture` on all formatting/parsing.
- No emojis anywhere (code, comments, commits, docs).
- No commented-out code survives a commit — delete or restore.
- Never delete existing behaviour without it being explicitly part of a task; the `execute_query` SELECT tool and all DataTools STAY.
- Tests: AAA structure with blank lines between sections; method names `MethodName_Scenario_ExpectedResult`; no magic strings.
- Plain-text tool output (ASCII), no JSON/markdown in tool responses — this is unchanged.
- The audit log is the only new file the server writes besides `constraints.json`. Its default location follows the same "project root" resolution as `ConstraintRepository`.

---

## File Structure

**New files:**
- `SqlSchemaMcp.Tests/SqlSchemaMcp.Tests.csproj` — unit test project (no DB).
- `SqlSchemaMcp.Tests/Data/SqlStatementValidatorTests.cs`
- `SqlSchemaMcp.Tests/Security/ReadOnlyStartupGateTests.cs`
- `SqlSchemaMcp.Tests/Auditing/FileAuditLogTests.cs`
- `Data/SqlStatementValidator.cs` — ScriptDom allowlist validator (replaces `SqlCommandGuard`).
- `Security/IPermissionProbe.cs` — abstraction: given a connection string, report whether the login can write.
- `Security/SqlServerPermissionProbe.cs` — real implementation querying `IS_ROLEMEMBER`/`fn_my_permissions`.
- `Security/ReadOnlyStartupGate.cs` — pure decision logic: given probe results + config, decide start/refuse/warn.
- `Auditing/IAuditLog.cs` — records tool invocations, wraps a tool call.
- `Auditing/FileAuditLog.cs` — thread-safe JSON-lines file appender.
- `Auditing/AuditEntry.cs` — the record written per invocation.
- `Configuration/AuditOptions.cs` — `Enabled`, `Path`.
- `Configuration/SecurityOptions.cs` — `AllowWritableLogin` (default false), `VerifyLoginsAtStartup` (default true).
- `docs/security-posture.md` — read-only model, required login, threat model, PII-in-data-tools warning.

**Modified files:**
- `Data/SqlCommandGuard.cs` — deleted (superseded by `SqlStatementValidator`).
- `Data/QueryQueries.cs` — call `SqlStatementValidator` instead of `SqlCommandGuard`.
- `Data/SchemaQueries.cs` — remove the two pointless `SqlCommandGuard.AssertReadOnly` calls on constant SQL.
- `Data/SqlQueryBase.cs` — add `SafeError` helper + `ILogger` dependency.
- All `Data/*Queries.cs` — replace `catch (Exception ex) { return $"ERROR: {ex.Message}"; }` with `catch (Exception ex) { return SafeError(ex); }`; pass `ILogger<T>` to base.
- All `Tools/*.cs` — inject `IAuditLog`, wrap each tool body in `audit.Invoke(...)`.
- `Program.cs` — register new services, run the startup gate, wire `AuditOptions`/`SecurityOptions`.
- `Configuration/SqlServerOptions.cs` — unchanged (kept for reference).
- `SqlSchemaMcp.csproj` — add ScriptDom package reference.
- `SqlSchemaMcp.sln` — add the test project.
- `appsettings.example.json` — add `Audit` + `Security` sections, read-only login comment.
- `README.md` and `CLAUDE.md` — rewrite the product promise; add security posture + audit sections.
- `.gitignore` — ignore `audit-log*.jsonl`.

---

## Phase 0 — Test project scaffolding

### Task 0: Create the unit test project

**Files:**
- Create: `SqlSchemaMcp.Tests/SqlSchemaMcp.Tests.csproj`
- Modify: `SqlSchemaMcp.sln`

**Interfaces:**
- Produces: a runnable `dotnet test` target that references the main project and has xUnit + FluentAssertions + NSubstitute.

- [ ] **Step 1: Create the test project file**

Create `SqlSchemaMcp.Tests/SqlSchemaMcp.Tests.csproj`:

```xml
<Project Sdk="Microsoft.NET.Sdk">

  <PropertyGroup>
    <TargetFramework>net10.0</TargetFramework>
    <Nullable>enable</Nullable>
    <ImplicitUsings>enable</ImplicitUsings>
    <IsPackable>false</IsPackable>
  </PropertyGroup>

  <ItemGroup>
    <PackageReference Include="Microsoft.NET.Test.Sdk" Version="17.11.1" />
    <PackageReference Include="xunit" Version="2.9.2" />
    <PackageReference Include="xunit.runner.visualstudio" Version="2.8.2" />
    <PackageReference Include="FluentAssertions" Version="6.12.0" />
    <PackageReference Include="NSubstitute" Version="5.1.0" />
  </ItemGroup>

  <ItemGroup>
    <ProjectReference Include="..\SqlSchemaMcp.csproj" />
  </ItemGroup>

</Project>
```

Note: confirm the latest patch versions during implementation (`dotnet add package`). The main project is an executable (`Sdk.Web`); a `ProjectReference` to it from a test project is valid — its public types are directly testable, so no `InternalsVisibleTo` is needed as long as new classes are `public`.

- [ ] **Step 2: Add the project to the solution**

Run: `dotnet sln SqlSchemaMcp.sln add SqlSchemaMcp.Tests/SqlSchemaMcp.Tests.csproj`
Expected: `Project ... added to the solution.`

- [ ] **Step 3: Add a smoke test**

Create `SqlSchemaMcp.Tests/SmokeTest.cs`:

```csharp
using FluentAssertions;
using Xunit;

namespace SqlSchemaMcp.Tests;

public sealed class SmokeTest
{
    [Fact]
    public void TestHarness_Runs_Passes()
    {
        var actual = 1 + 1;

        actual.Should().Be(2);
    }
}
```

- [ ] **Step 4: Run it**

Run: `dotnet test SqlSchemaMcp.Tests/SqlSchemaMcp.Tests.csproj`
Expected: `Passed! - Failed: 0, Passed: 1`

- [ ] **Step 5: Commit**

```bash
git add SqlSchemaMcp.Tests SqlSchemaMcp.sln
git commit -m "test: add unit test project scaffolding"
```

---

## Phase 1 — Read-only allowlist validator (ScriptDom)

Replaces the bypassable keyword denylist (`SqlCommandGuard`) with a parser-based allowlist. This is the code-level half of "read-only more enforced".

### Task 1: Add ScriptDom and write the validator with failing tests

**Files:**
- Modify: `SqlSchemaMcp.csproj`
- Create: `Data/SqlStatementValidator.cs`
- Test: `SqlSchemaMcp.Tests/Data/SqlStatementValidatorTests.cs`

**Interfaces:**
- Produces: `public static class SqlStatementValidator` with
  `public static SqlValidationResult Validate(string sql)` where
  `public readonly record struct SqlValidationResult(bool IsAllowed, string? Reason)`.
  Consumers call `Validate` and, on `IsAllowed == false`, return `ERROR: <Reason>` (Reason is safe, no server details).

- [ ] **Step 1: Add the ScriptDom package**

Run: `dotnet add SqlSchemaMcp.csproj package Microsoft.SqlServer.TransactSql.ScriptDom`
Expected: package added (confirm latest `161.*` — TSql160 parser covers SQL Server 2022 syntax and is a superset safe for older servers).

- [ ] **Step 2: Write the failing tests**

Create `SqlSchemaMcp.Tests/Data/SqlStatementValidatorTests.cs`:

```csharp
using FluentAssertions;
using SqlSchemaMcp.Data;
using Xunit;

namespace SqlSchemaMcp.Tests.Data;

public sealed class SqlStatementValidatorTests
{
    [Theory]
    [InlineData("SELECT * FROM dbo.Orders")]
    [InlineData("SELECT TOP 10 Id, Name FROM dbo.Customers WHERE IsActive = 1")]
    [InlineData("WITH cte AS (SELECT Id FROM dbo.Orders) SELECT * FROM cte")]
    [InlineData("select o.Id from dbo.Orders o join dbo.Lines l on l.OrderId = o.Id")]
    public void Validate_PlainSelect_IsAllowed(string sql)
    {
        var result = SqlStatementValidator.Validate(sql);

        result.IsAllowed.Should().BeTrue(because: sql);
        result.Reason.Should().BeNull();
    }

    [Theory]
    [InlineData("SELECT * INTO dbo.Copy FROM dbo.Orders")]
    [InlineData("INSERT INTO dbo.Orders (Id) VALUES (1)")]
    [InlineData("UPDATE dbo.Orders SET Name = 'x'")]
    [InlineData("DELETE FROM dbo.Orders")]
    [InlineData("DROP TABLE dbo.Orders")]
    [InlineData("TRUNCATE TABLE dbo.Orders")]
    [InlineData("ALTER TABLE dbo.Orders ADD X int")]
    [InlineData("CREATE TABLE dbo.X (Id int)")]
    [InlineData("EXEC sp_who")]
    [InlineData("MERGE dbo.Orders AS t USING dbo.Src AS s ON t.Id = s.Id WHEN MATCHED THEN DELETE;")]
    [InlineData("GRANT SELECT ON dbo.Orders TO someone")]
    [InlineData("WAITFOR DELAY '00:00:10'")]
    public void Validate_WriteOrControlStatement_IsRejected(string sql)
    {
        var result = SqlStatementValidator.Validate(sql);

        result.IsAllowed.Should().BeFalse(because: sql);
        result.Reason.Should().NotBeNullOrEmpty();
    }

    [Theory]
    [InlineData("SELECT * FROM OPENQUERY(link, 'DELETE FROM x')")]
    [InlineData("SELECT * FROM OPENROWSET('SQLNCLI', 'server';'u';'p', 'SELECT 1')")]
    [InlineData("SELECT * FROM OPENDATASOURCE('SQLNCLI', 'x').db.dbo.t")]
    public void Validate_OpenRowsetFamily_IsRejected(string sql)
    {
        var result = SqlStatementValidator.Validate(sql);

        result.IsAllowed.Should().BeFalse(because: sql);
        result.Reason.Should().Contain("OPEN");
    }

    [Fact]
    public void Validate_MultipleStatements_IsRejected()
    {
        var sql = "SELECT 1; DROP TABLE dbo.Orders";

        var result = SqlStatementValidator.Validate(sql);

        result.IsAllowed.Should().BeFalse();
        result.Reason.Should().Contain("single");
    }

    [Fact]
    public void Validate_Unparseable_IsRejected()
    {
        var sql = "SELECT FROM WHERE )(";

        var result = SqlStatementValidator.Validate(sql);

        result.IsAllowed.Should().BeFalse();
        result.Reason.Should().Contain("parse");
    }

    [Fact]
    public void Validate_Empty_IsRejected()
    {
        var result = SqlStatementValidator.Validate("   ");

        result.IsAllowed.Should().BeFalse();
    }
}
```

- [ ] **Step 3: Run to verify they fail**

Run: `dotnet test SqlSchemaMcp.Tests --filter FullyQualifiedName~SqlStatementValidatorTests`
Expected: FAIL — `SqlStatementValidator` does not exist / does not compile.

- [ ] **Step 4: Implement the validator**

Create `Data/SqlStatementValidator.cs`:

```csharp
using System.Collections.Generic;
using System.Linq;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace SqlSchemaMcp.Data;

/// <summary>
/// Allowlist validator for the execute_query tool. Parses the statement with the
/// T-SQL parser and permits ONLY a single read-only SELECT (optionally a CTE).
/// Everything else — writes, DDL, EXEC, OPENQUERY/OPENROWSET/OPENDATASOURCE,
/// WAITFOR, SELECT INTO, or multiple statements — is rejected.
///
/// This is the code-level defence. The primary defence is the read-only database
/// login verified by the startup gate; see docs/security-posture.md.
/// </summary>
public static class SqlStatementValidator
{
    public static SqlValidationResult Validate(string sql)
    {
        if (string.IsNullOrWhiteSpace(sql))
            return SqlValidationResult.Reject("Empty query is not permitted.");

        var parser = new TSql160Parser(initialQuotedIdentifiers: true);
        using var reader = new System.IO.StringReader(sql);
        var fragment = parser.Parse(reader, out IList<ParseError> errors);

        if (errors.Count > 0)
            return SqlValidationResult.Reject($"Query failed to parse: {errors[0].Message}");

        if (fragment is not TSqlScript script)
            return SqlValidationResult.Reject("Query could not be interpreted as a T-SQL script.");

        var statements = script.Batches.SelectMany(b => b.Statements).ToList();
        if (statements.Count == 0)
            return SqlValidationResult.Reject("No statement found.");
        if (statements.Count > 1)
            return SqlValidationResult.Reject("Only a single SELECT statement is permitted.");

        if (statements[0] is not SelectStatement select)
            return SqlValidationResult.Reject("Only SELECT statements are permitted. This server is read-only.");

        // Block SELECT ... INTO (creates a table — a write via DDL).
        if (select.Into is not null)
            return SqlValidationResult.Reject("SELECT ... INTO is not permitted (it creates a table).");

        var visitor = new ForbiddenConstructVisitor();
        select.Accept(visitor);
        if (visitor.Rejection is not null)
            return SqlValidationResult.Reject(visitor.Rejection);

        return SqlValidationResult.Allow();
    }

    private sealed class ForbiddenConstructVisitor : TSqlFragmentVisitor
    {
        public string? Rejection { get; private set; }

        // QuerySpecification.Into covers SELECT INTO inside subqueries/CTEs too.
        public override void Visit(QuerySpecification node)
        {
            if (node.Into is not null && Rejection is null)
                Rejection = "SELECT ... INTO is not permitted (it creates a table).";
            base.Visit(node);
        }

        public override void Visit(OpenQueryTableReference node)
        {
            Rejection ??= "OPENQUERY is not permitted.";
            base.Visit(node);
        }

        public override void Visit(OpenRowsetTableReference node)
        {
            Rejection ??= "OPENROWSET is not permitted.";
            base.Visit(node);
        }

        public override void Visit(InternalOpenRowset node)
        {
            Rejection ??= "OPENROWSET is not permitted.";
            base.Visit(node);
        }

        public override void Visit(OpenXmlTableReference node)
        {
            Rejection ??= "OPENXML is not permitted.";
            base.Visit(node);
        }

        // OPENDATASOURCE appears as a four-part SchemaObjectFunctionTableReference /
        // AdHocTableReference; block ad-hoc data source references explicitly.
        public override void Visit(AdHocTableReference node)
        {
            Rejection ??= "OPENDATASOURCE / ad-hoc remote table references are not permitted.";
            base.Visit(node);
        }
    }
}

public readonly record struct SqlValidationResult(bool IsAllowed, string? Reason)
{
    public static SqlValidationResult Allow() => new(true, null);
    public static SqlValidationResult Reject(string reason) => new(false, reason);
}
```

Note for implementer: ScriptDom node names can differ slightly by version. If `AdHocTableReference` or `InternalOpenRowset` do not exist in the referenced package version, remove that override and instead confirm the `OPENDATASOURCE` case is caught by the parse step or add a targeted check — the tests above are the contract; make them green.

- [ ] **Step 5: Run tests to verify they pass**

Run: `dotnet test SqlSchemaMcp.Tests --filter FullyQualifiedName~SqlStatementValidatorTests`
Expected: PASS — all theory cases green.

- [ ] **Step 6: Commit**

```bash
git add SqlSchemaMcp.csproj Data/SqlStatementValidator.cs SqlSchemaMcp.Tests/Data/SqlStatementValidatorTests.cs
git commit -m "feat: add ScriptDom allowlist validator for read-only queries"
```

### Task 2: Switch QueryQueries to the validator and delete the denylist

**Files:**
- Modify: `Data/QueryQueries.cs:22-29`
- Modify: `Data/SchemaQueries.cs` (remove two `SqlCommandGuard.AssertReadOnly` calls)
- Delete: `Data/SqlCommandGuard.cs`

**Interfaces:**
- Consumes: `SqlStatementValidator.Validate` from Task 1.

- [ ] **Step 1: Replace the guard call in QueryQueries**

In `Data/QueryQueries.cs`, replace the current try/catch guard block (the `SqlCommandGuard.AssertReadOnly(sql)` inside a try/catch) with:

```csharp
var validation = SqlStatementValidator.Validate(sql);
if (!validation.IsAllowed)
    return $"ERROR: {validation.Reason}";
```

Place it immediately after the `if (!_databases.TryGetValue(...)) return UnknownDatabase(database);` check and before the connection is opened. Remove the now-unused `using` for the guard if present.

- [ ] **Step 2: Remove the pointless guard calls in SchemaQueries**

In `Data/SchemaQueries.cs`, delete the two lines `SqlCommandGuard.AssertReadOnly(sql);` in `ListDdlTriggers` (around line 890) and `GetDdlTriggerDefinition` (around line 947). These validate a compile-time constant string and add nothing. Confirm both methods use only constant SQL and parameterised inputs (they do).

- [ ] **Step 3: Delete the old guard**

Run: `git rm Data/SqlCommandGuard.cs`

- [ ] **Step 4: Build and run the full test suite**

Run: `dotnet build SqlSchemaMcp.sln` then `dotnet test SqlSchemaMcp.sln`
Expected: build succeeds, all tests pass. Confirm no remaining references to `SqlCommandGuard`:
Run: `git grep -n SqlCommandGuard` → Expected: no output.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "refactor: use allowlist validator, remove bypassable keyword denylist"
```

---

## Phase 2 — Startup permission gate

The primary defence: verify each configured login cannot write, and refuse to start otherwise. Decision logic is separated from the SQL probe so it is unit-testable without a database.

### Task 3: Config options and the pure gate decision logic

**Files:**
- Create: `Configuration/SecurityOptions.cs`
- Create: `Security/IPermissionProbe.cs`
- Create: `Security/ReadOnlyStartupGate.cs`
- Test: `SqlSchemaMcp.Tests/Security/ReadOnlyStartupGateTests.cs`

**Interfaces:**
- Produces:
  - `public sealed class SecurityOptions { public bool VerifyLoginsAtStartup { get; init; } = true; public bool AllowWritableLogin { get; init; } = false; }`
  - `public sealed record LoginPermissionResult(string Database, bool Reachable, bool CanWrite, IReadOnlyList<string> GrantedWrites);`
  - `public interface IPermissionProbe { Task<LoginPermissionResult> ProbeAsync(string database, string connectionString, CancellationToken ct); }`
  - `public sealed record GateDecision(bool ShouldStart, IReadOnlyList<string> Errors, IReadOnlyList<string> Warnings);`
  - `public static class ReadOnlyStartupGate { public static GateDecision Evaluate(IReadOnlyList<LoginPermissionResult> probes, SecurityOptions options); }`

- [ ] **Step 1: Create the options type**

Create `Configuration/SecurityOptions.cs`:

```csharp
namespace SqlSchemaMcp.Configuration;

public sealed class SecurityOptions
{
    /// <summary>When true (default) the server probes every configured login at startup.</summary>
    public bool VerifyLoginsAtStartup { get; init; } = true;

    /// <summary>Escape hatch. When false (default) the server refuses to start if any reachable login can write.</summary>
    public bool AllowWritableLogin { get; init; } = false;
}
```

- [ ] **Step 2: Create the probe abstraction**

Create `Security/IPermissionProbe.cs`:

```csharp
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace SqlSchemaMcp.Security;

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

- [ ] **Step 3: Write failing tests for the decision logic**

Create `SqlSchemaMcp.Tests/Security/ReadOnlyStartupGateTests.cs`:

```csharp
using System.Collections.Generic;
using FluentAssertions;
using SqlSchemaMcp.Configuration;
using SqlSchemaMcp.Security;
using Xunit;

namespace SqlSchemaMcp.Tests.Security;

public sealed class ReadOnlyStartupGateTests
{
    private static readonly SecurityOptions Strict = new() { VerifyLoginsAtStartup = true, AllowWritableLogin = false };

    [Fact]
    public void Evaluate_AllReadOnly_StartsWithNoErrors()
    {
        var probes = new List<LoginPermissionResult>
        {
            new("poc", Reachable: true, CanWrite: false, GrantedWrites: []),
            new("azure", Reachable: true, CanWrite: false, GrantedWrites: []),
        };

        var decision = ReadOnlyStartupGate.Evaluate(probes, Strict);

        decision.ShouldStart.Should().BeTrue();
        decision.Errors.Should().BeEmpty();
    }

    [Fact]
    public void Evaluate_WritableLogin_Strict_RefusesToStart()
    {
        var probes = new List<LoginPermissionResult>
        {
            new("poc", Reachable: true, CanWrite: true, GrantedWrites: ["db_datawriter"]),
        };

        var decision = ReadOnlyStartupGate.Evaluate(probes, Strict);

        decision.ShouldStart.Should().BeFalse();
        decision.Errors.Should().ContainSingle().Which.Should().Contain("poc").And.Contain("db_datawriter");
    }

    [Fact]
    public void Evaluate_WritableLogin_AllowOverride_StartsWithWarning()
    {
        var probes = new List<LoginPermissionResult>
        {
            new("poc", Reachable: true, CanWrite: true, GrantedWrites: ["db_owner"]),
        };
        var lenient = new SecurityOptions { VerifyLoginsAtStartup = true, AllowWritableLogin = true };

        var decision = ReadOnlyStartupGate.Evaluate(probes, lenient);

        decision.ShouldStart.Should().BeTrue();
        decision.Warnings.Should().ContainSingle().Which.Should().Contain("poc");
    }

    [Fact]
    public void Evaluate_UnreachableLogin_StartsWithWarning()
    {
        var probes = new List<LoginPermissionResult>
        {
            new("poc", Reachable: false, CanWrite: false, GrantedWrites: []),
        };

        var decision = ReadOnlyStartupGate.Evaluate(probes, Strict);

        decision.ShouldStart.Should().BeTrue();
        decision.Warnings.Should().ContainSingle().Which.Should().Contain("could not be verified");
    }

    [Fact]
    public void Evaluate_VerificationDisabled_AlwaysStarts()
    {
        var probes = new List<LoginPermissionResult>
        {
            new("poc", Reachable: true, CanWrite: true, GrantedWrites: ["db_owner"]),
        };
        var off = new SecurityOptions { VerifyLoginsAtStartup = false, AllowWritableLogin = false };

        var decision = ReadOnlyStartupGate.Evaluate(probes, off);

        decision.ShouldStart.Should().BeTrue();
    }
}
```

- [ ] **Step 4: Run to verify they fail**

Run: `dotnet test SqlSchemaMcp.Tests --filter FullyQualifiedName~ReadOnlyStartupGateTests`
Expected: FAIL — `ReadOnlyStartupGate` does not exist.

- [ ] **Step 5: Implement the gate**

Create `Security/ReadOnlyStartupGate.cs`:

```csharp
using System.Collections.Generic;
using System.Linq;
using SqlSchemaMcp.Configuration;

namespace SqlSchemaMcp.Security;

public sealed record GateDecision(
    bool ShouldStart,
    IReadOnlyList<string> Errors,
    IReadOnlyList<string> Warnings);

public static class ReadOnlyStartupGate
{
    public static GateDecision Evaluate(IReadOnlyList<LoginPermissionResult> probes, SecurityOptions options)
    {
        var errors = new List<string>();
        var warnings = new List<string>();

        if (!options.VerifyLoginsAtStartup)
            return new GateDecision(ShouldStart: true, errors, warnings);

        foreach (var probe in probes)
        {
            if (!probe.Reachable)
            {
                warnings.Add($"Read-only status of login for database '{probe.Database}' could not be verified (unreachable at startup).");
                continue;
            }

            if (!probe.CanWrite)
                continue;

            var grants = string.Join(", ", probe.GrantedWrites);
            if (options.AllowWritableLogin)
                warnings.Add($"Login for database '{probe.Database}' can WRITE ({grants}). Continuing because Security:AllowWritableLogin is true.");
            else
                errors.Add($"Login for database '{probe.Database}' can WRITE ({grants}). Refusing to start. Use a read-only login (db_datareader only) or set Security:AllowWritableLogin=true to override.");
        }

        return new GateDecision(ShouldStart: errors.Count == 0, errors, warnings);
    }
}
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `dotnet test SqlSchemaMcp.Tests --filter FullyQualifiedName~ReadOnlyStartupGateTests`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add Configuration/SecurityOptions.cs Security/IPermissionProbe.cs Security/ReadOnlyStartupGate.cs SqlSchemaMcp.Tests/Security/ReadOnlyStartupGateTests.cs
git commit -m "feat: add read-only startup gate decision logic"
```

### Task 4: Real SQL permission probe + wire the gate into startup

**Files:**
- Create: `Security/SqlServerPermissionProbe.cs`
- Modify: `Program.cs`

**Interfaces:**
- Consumes: `IPermissionProbe`, `LoginPermissionResult` (Task 3), `ReadOnlyStartupGate.Evaluate` (Task 3), `SqlServerOptions` (existing).
- Produces: startup behaviour that aborts (`Environment.Exit(1)` / thrown before host run) when the gate says not to start.

- [ ] **Step 1: Implement the probe**

Create `Security/SqlServerPermissionProbe.cs`:

```csharp
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace SqlSchemaMcp.Security;

/// <summary>
/// Probes a login's effective write capability. A login is considered writable if it is a
/// member of any writing/owning/DDL role, is sysadmin, or holds any explicit DB-scoped write
/// permission. Read intent only — this query performs no writes.
/// </summary>
public sealed class SqlServerPermissionProbe : IPermissionProbe
{
    private const string Sql = """
        SELECT
            CAST(ISNULL(IS_SRVROLEMEMBER('sysadmin'), 0) AS int) AS IsSysadmin,
            CAST(ISNULL(IS_ROLEMEMBER('db_owner'), 0) AS int) AS IsDbOwner,
            CAST(ISNULL(IS_ROLEMEMBER('db_datawriter'), 0) AS int) AS IsDataWriter,
            CAST(ISNULL(IS_ROLEMEMBER('db_ddladmin'), 0) AS int) AS IsDdlAdmin,
            (SELECT COUNT(*) FROM sys.fn_my_permissions(NULL, 'DATABASE')
             WHERE permission_name IN ('INSERT','UPDATE','DELETE','ALTER','CONTROL','CREATE TABLE')) AS WriteGrants
        """;

    public async Task<LoginPermissionResult> ProbeAsync(string database, string connectionString, CancellationToken ct)
    {
        try
        {
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(ct);
            await using var cmd = new SqlCommand(Sql, conn);
            await using var reader = await cmd.ExecuteReaderAsync(ct);

            var grants = new List<string>();
            if (await reader.ReadAsync(ct))
            {
                if (reader.GetInt32(0) == 1) grants.Add("sysadmin");
                if (reader.GetInt32(1) == 1) grants.Add("db_owner");
                if (reader.GetInt32(2) == 1) grants.Add("db_datawriter");
                if (reader.GetInt32(3) == 1) grants.Add("db_ddladmin");
                if (reader.GetInt32(4) > 0) grants.Add("explicit write grants");
            }

            return new LoginPermissionResult(database, Reachable: true, CanWrite: grants.Count > 0, grants);
        }
        catch (Exception)
        {
            // Unreachable / auth failure at startup — the gate treats this as "unverified", not "writable".
            return new LoginPermissionResult(database, Reachable: false, CanWrite: false, []);
        }
    }
}
```

- [ ] **Step 2: Add a startup-gate runner in Program.cs**

In `Program.cs`, add a local async function (near the existing `RegisterServices`) and register `SecurityOptions` + `IPermissionProbe`. In `RegisterServices`, add:

```csharp
services.Configure<SecurityOptions>(configuration.GetSection("Security"));
services.AddSingleton<IPermissionProbe, SqlServerPermissionProbe>();
```

Add the gate runner:

```csharp
static async Task<bool> RunStartupGateAsync(IServiceProvider services, CancellationToken ct)
{
    var options = services.GetRequiredService<IOptions<SqlServerOptions>>().Value;
    var security = services.GetRequiredService<IOptions<SecurityOptions>>().Value;
    var probe = services.GetRequiredService<IPermissionProbe>();

    var results = new List<LoginPermissionResult>();
    foreach (var (name, connectionString) in options.Databases)
        results.Add(await probe.ProbeAsync(name, connectionString, ct));

    var decision = ReadOnlyStartupGate.Evaluate(results, security);

    foreach (var warning in decision.Warnings)
        Console.Error.WriteLine($"[SqlSchemaMcp] WARN: {warning}");
    foreach (var error in decision.Errors)
        Console.Error.WriteLine($"[SqlSchemaMcp] CRITICAL: {error}");

    return decision.ShouldStart;
}
```

Add the required usings at the top of `Program.cs` (outside any namespace): `using Microsoft.Extensions.DependencyInjection;`, `using Microsoft.Extensions.Options;`, `using SqlSchemaMcp.Security;`, `using System.Collections.Generic;`.

- [ ] **Step 3: Invoke the gate before the host serves, in both transport branches**

In the stdio branch, after `var host = builder.Build();` and before `await host.RunAsync();`:

```csharp
if (!await RunStartupGateAsync(host.Services, CancellationToken.None))
{
    Console.Error.WriteLine("[SqlSchemaMcp] Startup aborted: a configured login is not read-only.");
    Environment.Exit(1);
}
```

In the HTTP branch, after `var app = builder.Build();` and before mapping endpoints / `await app.RunAsync();`:

```csharp
if (!await RunStartupGateAsync(app.Services, CancellationToken.None))
{
    Console.Error.WriteLine("[SqlSchemaMcp] Startup aborted: a configured login is not read-only.");
    Environment.Exit(1);
}
```

- [ ] **Step 4: Manual verification against a real database**

Run: `dotnet run` (stdio) with a read-only login configured.
Expected on stderr: `[SqlSchemaMcp] Stdio mode gestart` with no CRITICAL lines.
Then temporarily point a database at a `db_owner` login and confirm: `[SqlSchemaMcp] CRITICAL: Login for database ... can WRITE` and process exits with code 1.

Note: this task has no automated test (it wires real infra); the decision logic it depends on is already covered by Task 3. Keep the manual check in the PR description.

- [ ] **Step 5: Commit**

```bash
git add Security/SqlServerPermissionProbe.cs Program.cs
git commit -m "feat: verify read-only login at startup, refuse to start on writable login"
```

---

## Phase 3 — Error sanitisation

Stop leaking raw `SqlException` text (server, database, login) into the model context. Full detail goes to the logger; the model gets a generic message.

### Task 5: Add SafeError to the query base and thread ILogger through

**Files:**
- Modify: `Data/SqlQueryBase.cs`
- Modify: every `Data/*Queries.cs` constructor and every generic `catch` block
- Modify: `Program.cs` (DI already provides `ILogger<T>` via `AddLogging`; ensure logging is registered in stdio branch — it is via `builder.Logging`; the HTTP branch uses `WebApplication` which adds logging by default)

**Interfaces:**
- Produces: `protected string SafeError(Exception ex, [CallerMemberName] string? operation = null)` on `SqlQueryBase`. Returns `"ERROR: the query failed (see server log)."` and logs the full exception via `ILogger`.
- Consumes: `ILogger` injected into `SqlQueryBase`.

- [ ] **Step 1: Add ILogger + SafeError to the base**

In `Data/SqlQueryBase.cs`, change the primary constructor and add the helper. New top of class:

```csharp
using System;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.RegularExpressions;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SqlSchemaMcp.Configuration;

namespace SqlSchemaMcp.Data;

public abstract partial class SqlQueryBase(IOptions<SqlServerOptions> options, ILogger logger)
{
    protected readonly Dictionary<string, string> _databases = options.Value.Databases;

    protected string SafeError(Exception ex, [CallerMemberName] string? operation = null)
    {
        logger.LogError(ex, "Query operation {Operation} failed", operation);
        return "ERROR: the query failed. Check the server log for details.";
    }

    // ... rest of existing members unchanged ...
```

Keep all existing static helpers. Do not change `UnknownDatabase` (its output is intentional and safe).

- [ ] **Step 2: Update every Queries subclass constructor**

For each of `SchemaQueries`, `AnalysisQueries`, `PipelineQueries`, `CompareQueries`, `DiagnosticsQueries`, `DataQueries`, `SecurityQueries`, `QueryQueries`, change the primary constructor to accept and forward an `ILogger<T>`. Example for `SchemaQueries`:

```csharp
public sealed class SchemaQueries(IOptions<SqlServerOptions> options, ILogger<SchemaQueries> logger)
    : SqlQueryBase(options, logger)
```

Add `using Microsoft.Extensions.Logging;` to each file that does not already have it. Apply the identical shape to all eight classes (only the generic type argument changes).

- [ ] **Step 3: Replace every leaking catch block**

Across all `Data/*Queries.cs`, replace each occurrence of:

```csharp
catch (Exception ex)
{
    return $"ERROR: {ex.Message}";
}
```

with:

```csharp
catch (Exception ex)
{
    return SafeError(ex);
}
```

Verify none remain:
Run: `git grep -n 'ERROR: {ex.Message}'` → Expected: no output.

- [ ] **Step 4: Confirm DI still resolves (logger auto-injected)**

`AddSingleton<SchemaQueries>()` etc. already work because the host provides `ILogger<T>`. No Program.cs change needed for logging in either branch. Build:
Run: `dotnet build SqlSchemaMcp.sln`
Expected: success.

- [ ] **Step 5: Add a regression test that SafeError does not leak**

Create `SqlSchemaMcp.Tests/Data/SafeErrorTests.cs`:

```csharp
using System;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using SqlSchemaMcp.Configuration;
using SqlSchemaMcp.Data;
using Xunit;

namespace SqlSchemaMcp.Tests.Data;

public sealed class SafeErrorTests
{
    private sealed class TestQueries(IOptions<SqlServerOptions> options)
        : SqlQueryBase(options, NullLogger<TestQueries>.Instance)
    {
        public string CallSafeError(Exception ex) => SafeError(ex);
    }

    [Fact]
    public void SafeError_WithSensitiveException_ReturnsGenericMessage()
    {
        var options = Options.Create(new SqlServerOptions());
        var sut = new TestQueries(options);
        var leaky = new InvalidOperationException("Login failed for user 'sa' on server 'prod-sql-01'.");

        var result = sut.CallSafeError(leaky);

        result.Should().NotContain("prod-sql-01");
        result.Should().NotContain("sa");
        result.Should().StartWith("ERROR:");
    }
}
```

Note: `SafeError` and the base constructor must be accessible to the test — they are `protected`, reached here via the nested `TestQueries` subclass, so no visibility change is required.

- [ ] **Step 6: Run the suite**

Run: `dotnet test SqlSchemaMcp.sln`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "fix: sanitise query error messages, log full detail to server log"
```

---

## Phase 4 — Structured audit log

A dedicated JSON-lines audit trail: who ran which tool against which database, with a parameter summary, duration, and outcome. This is the team koopargument.

### Task 6: Audit entry, options, and the file sink (TDD)

**Files:**
- Create: `Auditing/AuditEntry.cs`
- Create: `Auditing/IAuditLog.cs`
- Create: `Auditing/FileAuditLog.cs`
- Create: `Configuration/AuditOptions.cs`
- Test: `SqlSchemaMcp.Tests/Auditing/FileAuditLogTests.cs`

**Interfaces:**
- Produces:
  - `public sealed record AuditEntry(DateTimeOffset TimestampUtc, string Tool, string Database, string ParametersSummary, long DurationMs, bool Success);`
  - `public sealed class AuditOptions { public bool Enabled { get; init; } = true; public string? Path { get; init; } }`
  - `public interface IAuditLog { Task<string> Invoke(string tool, string database, string parametersSummary, Func<Task<string>> body); }`
  - `public sealed class FileAuditLog : IAuditLog` writing one JSON object per line to the resolved path.

- [ ] **Step 1: Create the entry and options records**

Create `Auditing/AuditEntry.cs`:

```csharp
using System;

namespace SqlSchemaMcp.Auditing;

public sealed record AuditEntry(
    DateTimeOffset TimestampUtc,
    string Tool,
    string Database,
    string ParametersSummary,
    long DurationMs,
    bool Success);
```

Create `Configuration/AuditOptions.cs`:

```csharp
namespace SqlSchemaMcp.Configuration;

public sealed class AuditOptions
{
    public bool Enabled { get; init; } = true;

    /// <summary>Absolute or relative path to the JSON-lines audit file. When null, defaults to audit-log.jsonl in the project root.</summary>
    public string? Path { get; init; }
}
```

Create `Auditing/IAuditLog.cs`:

```csharp
using System;
using System.Threading.Tasks;

namespace SqlSchemaMcp.Auditing;

public interface IAuditLog
{
    /// <summary>
    /// Records the invocation of a tool and returns the tool's result. Timing and outcome
    /// are captured even when the body throws (the exception is re-thrown after recording).
    /// </summary>
    Task<string> Invoke(string tool, string database, string parametersSummary, Func<Task<string>> body);
}
```

- [ ] **Step 2: Write failing tests for the file sink**

Create `SqlSchemaMcp.Tests/Auditing/FileAuditLogTests.cs`:

```csharp
using System;
using System.IO;
using System.Text.Json;
using System.Threading.Tasks;
using FluentAssertions;
using Microsoft.Extensions.Options;
using SqlSchemaMcp.Auditing;
using SqlSchemaMcp.Configuration;
using Xunit;

namespace SqlSchemaMcp.Tests.Auditing;

public sealed class FileAuditLogTests : IDisposable
{
    private readonly string _path = Path.Combine(Path.GetTempPath(), $"audit-{Guid.NewGuid():N}.jsonl");

    [Fact]
    public async Task Invoke_SuccessfulBody_WritesEntryAndReturnsResult()
    {
        var sut = new FileAuditLog(Options.Create(new AuditOptions { Enabled = true, Path = _path }));

        var result = await sut.Invoke("ExecuteQuery", "poc", "sql=SELECT 1", () => Task.FromResult("rows: 1"));

        result.Should().Be("rows: 1");
        var line = (await File.ReadAllLinesAsync(_path))[0];
        var entry = JsonSerializer.Deserialize<AuditEntry>(line, JsonSerializerOptions.Web);
        entry!.Tool.Should().Be("ExecuteQuery");
        entry.Database.Should().Be("poc");
        entry.Success.Should().BeTrue();
    }

    [Fact]
    public async Task Invoke_BodyThrows_RecordsFailureAndRethrows()
    {
        var sut = new FileAuditLog(Options.Create(new AuditOptions { Enabled = true, Path = _path }));

        var act = async () => await sut.Invoke("ExecuteQuery", "poc", "sql=bad", () => throw new InvalidOperationException("boom"));

        await act.Should().ThrowAsync<InvalidOperationException>();
        var line = (await File.ReadAllLinesAsync(_path))[0];
        var entry = JsonSerializer.Deserialize<AuditEntry>(line, JsonSerializerOptions.Web);
        entry!.Success.Should().BeFalse();
    }

    [Fact]
    public async Task Invoke_Disabled_DoesNotWriteFile()
    {
        var sut = new FileAuditLog(Options.Create(new AuditOptions { Enabled = false, Path = _path }));

        await sut.Invoke("ExecuteQuery", "poc", "sql=SELECT 1", () => Task.FromResult("ok"));

        File.Exists(_path).Should().BeFalse();
    }

    public void Dispose()
    {
        if (File.Exists(_path)) File.Delete(_path);
    }
}
```

- [ ] **Step 3: Run to verify they fail**

Run: `dotnet test SqlSchemaMcp.Tests --filter FullyQualifiedName~FileAuditLogTests`
Expected: FAIL — `FileAuditLog` does not exist.

- [ ] **Step 4: Implement the file sink**

Create `Auditing/FileAuditLog.cs`:

```csharp
using System;
using System.Diagnostics;
using System.IO;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using SqlSchemaMcp.Configuration;

namespace SqlSchemaMcp.Auditing;

public sealed class FileAuditLog : IAuditLog
{
    private readonly AuditOptions _options;
    private readonly string _path;
    private readonly SemaphoreSlim _writeLock = new(1, 1);

    public FileAuditLog(IOptions<AuditOptions> options)
    {
        _options = options.Value;
        _path = ResolvePath(_options.Path);
    }

    public async Task<string> Invoke(string tool, string database, string parametersSummary, Func<Task<string>> body)
    {
        if (!_options.Enabled)
            return await body();

        var stopwatch = Stopwatch.StartNew();
        var success = false;
        try
        {
            var result = await body();
            success = true;
            return result;
        }
        finally
        {
            stopwatch.Stop();
            await WriteAsync(new AuditEntry(
                DateTimeOffset.UtcNow, tool, database, parametersSummary, stopwatch.ElapsedMilliseconds, success));
        }
    }

    private async Task WriteAsync(AuditEntry entry)
    {
        var line = JsonSerializer.Serialize(entry, JsonSerializerOptions.Web) + Environment.NewLine;
        await _writeLock.WaitAsync();
        try
        {
            // FileShare.ReadWrite so a concurrent process (stdio-per-session) can also append.
            await using var stream = new FileStream(_path, FileMode.Append, FileAccess.Write, FileShare.ReadWrite);
            await using var writer = new StreamWriter(stream);
            await writer.WriteAsync(line);
        }
        catch
        {
            // Auditing must never take down a query. Swallow write failures silently;
            // the diagnostic logger (Phase 3) still captures operational errors.
        }
        finally
        {
            _writeLock.Release();
        }
    }

    private static string ResolvePath(string? configured)
    {
        if (!string.IsNullOrWhiteSpace(configured))
            return configured;

        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir != null)
        {
            if (dir.GetFiles("*.csproj").Length > 0)
                return Path.Combine(dir.FullName, "audit-log.jsonl");
            dir = dir.Parent;
        }
        return Path.Combine(AppContext.BaseDirectory, "audit-log.jsonl");
    }
}
```

Note: cross-process append to one file can interleave lines under heavy concurrent load — the same accepted caveat as `constraints.json` (documented in README). For v1 this is acceptable; a later task can switch to a per-process dated file if needed.

- [ ] **Step 5: Run tests to verify they pass**

Run: `dotnet test SqlSchemaMcp.Tests --filter FullyQualifiedName~FileAuditLogTests`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add Auditing Configuration/AuditOptions.cs SqlSchemaMcp.Tests/Auditing/FileAuditLogTests.cs
git commit -m "feat: add structured file-backed audit log"
```

### Task 7: Register audit log and wrap every tool invocation

**Files:**
- Modify: `Program.cs` (register `AuditOptions` + `IAuditLog`)
- Modify: all `Tools/*.cs` (inject `IAuditLog`, wrap each tool body)

**Interfaces:**
- Consumes: `IAuditLog.Invoke` (Task 6).

- [ ] **Step 1: Register the audit services**

In `Program.cs` `RegisterServices`, add:

```csharp
services.Configure<AuditOptions>(configuration.GetSection("Audit"));
services.AddSingleton<IAuditLog, FileAuditLog>();
```

Add `using SqlSchemaMcp.Auditing;` at the top of `Program.cs`.

- [ ] **Step 2: Wrap tool bodies — pattern**

Each tool class gains an `IAuditLog audit` constructor parameter, and each tool method wraps its delegation. Apply this exact pattern to every `[McpServerTool]` method in every `Tools/*.cs` class. Example transformation for `QueryTools`:

```csharp
using System.ComponentModel;
using ModelContextProtocol.Server;
using SqlSchemaMcp.Auditing;
using SqlSchemaMcp.Data;

namespace SqlSchemaMcp.Tools;

[McpServerToolType]
public sealed class QueryTools(QueryQueries queries, IAuditLog audit)
{
    [McpServerTool, Description("Execute a read-only SELECT query against a configured database. Returns results as an ASCII table (max 500 rows, 30-second timeout). Only a single SELECT/CTE is permitted; writes, DDL, EXEC, OPENQUERY/OPENROWSET, SELECT INTO, and WAITFOR are blocked.")]
    public Task<string> ExecuteQuery(
        [Description("Name of the configured database")] string database,
        [Description("SELECT statement or CTE (WITH ...) to execute. Only read-only SELECT is permitted.")] string sql,
        CancellationToken cancellationToken = default) =>
        audit.Invoke(nameof(ExecuteQuery), database, $"sql={Summarise(sql)}",
            () => queries.ExecuteQuery(database, sql, cancellationToken));

    private static string Summarise(string value) =>
        value.Length <= 200 ? value : value[..200] + "...";
}
```

Rules for the parameter summary per tool:
- Always include `database` as the audit `database` argument.
- Summarise object identifiers (table/proc/view/column names, filters) as `key=value` pairs joined by `; `. Truncate any single value to 200 chars via a local `Summarise` helper (or a shared static in `Auditing`).
- For `ExecuteQuery`, include the (truncated) SQL — it is the whole point of the trail.
- Do NOT include connection strings or row data in the summary.

Apply to all tool classes: `SchemaTools`, `AnalysisTools`, `PipelineTools`, `CompareTools`, `ConstraintTools`, `DiagnosticsTools`, `DataTools`, `SecurityTools`, `QueryTools`. This is mechanical: change the method from `async Task<string> X(...) => await q.X(...)` to `Task<string> X(...) => audit.Invoke(nameof(X), database, "<summary>", () => q.X(...))`.

For `CompareTools` methods that take two databases (`database1`, `database2`), pass `$"{database1} vs {database2}"` as the audit `database` argument and put both names in the summary.

For `ConstraintTools` (no database / operates on `constraints.json`), pass `"(constraints)"` as the audit database argument.

- [ ] **Step 3: Add a shared summary helper to avoid duplication (DRY)**

Create the helper once so each tool class does not redefine `Summarise`. Add to `Auditing/IAuditLog.cs` file (same namespace):

```csharp
public static class AuditSummary
{
    public static string Truncate(string? value, int max = 200) =>
        string.IsNullOrEmpty(value) ? "" : value.Length <= max ? value : value[..max] + "...";
}
```

Use `AuditSummary.Truncate(sql)` in tool summaries instead of a per-class helper.

- [ ] **Step 4: Build and run the full suite**

Run: `dotnet build SqlSchemaMcp.sln` then `dotnet test SqlSchemaMcp.sln`
Expected: build succeeds, all tests pass.

- [ ] **Step 5: Manual end-to-end check**

Run: `dotnet run` and issue one `ExecuteQuery` and one `GetTableSchema` from a connected Claude session (or via an MCP client). Then:
Run: `cat audit-log.jsonl`
Expected: two JSON lines with correct `tool`, `database`, `durationMs`, `success`, and a truncated `parametersSummary`.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat: record every tool invocation in the audit log"
```

---

## Phase 5 — Product promise, config, and docs

Make the honesty match the code: the server is read-only but does access data through `execute_query` and DataTools, with the risks stated.

### Task 8: Rewrite the promise and document the security posture

**Files:**
- Modify: `README.md` (top promise, add Security + Audit sections)
- Modify: `CLAUDE.md` (Purpose + "What NOT to build")
- Create: `docs/security-posture.md`
- Modify: `appsettings.example.json`
- Modify: `.gitignore`

- [ ] **Step 1: Rewrite the README promise**

Replace the current README opening line ("No query execution, no data modification — schema and metadata only.") with an honest statement:

```markdown
A read-only MCP server for SQL Server. It exposes full schema and metadata, and supports
read-only data access for debugging: a validated single-SELECT `execute_query` tool plus
bounded row sampling and column-distribution tools. It never modifies data or schema.

Read-only is enforced on two levels: a startup gate that refuses to run against a login
with write permissions, and a parser-based allowlist that permits only SELECT/CTE queries.
See docs/security-posture.md before installing.
```

- [ ] **Step 2: Update CLAUDE.md**

In `CLAUDE.md`, change the Purpose section line "No query execution. No data access. Schema and metadata only." to:

```markdown
Read-only. Full schema and metadata, plus deliberately-scoped read-only data access
(single-SELECT execute_query, row sampling, column stats) for debugging. No writes, no DDL,
no schema modification. Read-only is enforced by a startup permission gate and a SELECT-only
allowlist validator — not by convention alone.
```

In the "What NOT to build" list, replace "No query execution or data preview" with "No write or DDL execution of any kind; no schema modification". Keep all other "NOT" items.

- [ ] **Step 3: Write the security posture doc**

Create `docs/security-posture.md` covering:
- **Required login:** a dedicated login in `db_datareader` only. Include a runnable snippet:

```sql
CREATE LOGIN sqlschema_ro WITH PASSWORD = '<strong-secret>';
CREATE USER sqlschema_ro FOR LOGIN sqlschema_ro;
ALTER ROLE db_datareader ADD MEMBER sqlschema_ro;
-- Do NOT add db_datawriter, db_ddladmin, db_owner, or grant CONTROL.
```

- **Two enforcement layers:** the startup gate (`Security:VerifyLoginsAtStartup`, `Security:AllowWritableLogin`) and the `SqlStatementValidator` allowlist. State plainly that the login is the primary defence and the validator is defence-in-depth.
- **Data access is real:** `execute_query`, `SampleTableData`, `AnalyzeColumnDistribution`, `FindDuplicateRows` return actual row values; `AnalyzeColumnDistribution` min/max can surface PII. Point operators at column-level DENY if a column must never be read.
- **Audit trail:** what `audit-log.jsonl` records, where it lives, the cross-process interleave caveat, how to disable (`Audit:Enabled=false`).
- **HTTP mode:** localhost only, unauthenticated, not a team transport.
- **Prompt-injection blast radius:** worst case a compromised agent reads any data the read-only login can read (bounded to 500 rows/call, all logged); it cannot write because of the gate + login.

- [ ] **Step 4: Update appsettings.example.json**

Add the new sections and a read-only reminder:

```json
{
  "Mcp": {
    "Port": 5101,
    "BindAddress": "localhost"
  },
  "Security": {
    "VerifyLoginsAtStartup": true,
    "AllowWritableLogin": false
  },
  "Audit": {
    "Enabled": true,
    "Path": null
  },
  "SqlServer": {
    "Databases": {
      "poc":   "Server=YOUR_SERVER;Database=YOUR_POC_DB;User Id=sqlschema_ro;Password=YOUR_SECRET;TrustServerCertificate=true;",
      "azure": "Server=YOUR_SERVER.database.windows.net;Database=YOUR_AZURE_DB;Authentication=Active Directory Default;"
    }
  }
}
```

Note: `Mcp.BindAddress` already exists in this file (added ahead of schedule by Task 9) — keep it, do not drop it while adding the `Security`/`Audit` sections.

- [ ] **Step 5: Ignore the audit file**

Add to `.gitignore`:

```
audit-log*.jsonl
```

- [ ] **Step 6: Commit**

```bash
git add README.md CLAUDE.md docs/security-posture.md appsettings.example.json .gitignore
git commit -m "docs: rewrite product promise to read-only-with-data-access, document security posture"
```

---

## Phase 6 — Deploy HTTP mode as a shared team instance

Goal: run the streamable-HTTP transport centrally on a server so multiple people can point their Claude Code at one instance, instead of each running stdio locally. Decisions locked in for this phase:

- **Authentication:** network-level only for now (VPN / firewall restricts who can reach the port). No application-level auth is added in this phase — the existing OAuth stub in `Program.cs` stays as-is (it satisfies the MCP client's discovery dance; it does not gate access). This is a deliberate, tracked gap — see the Backlog note at the end of this phase.
- **Deploy mechanism:** Docker container.
- **Secrets on the server:** environment variables injected at container run/deploy time (the existing `SQLMCP_` prefix mechanism, already read by `Program.cs`). Azure Key Vault is deliberately deferred — also tracked in the Backlog note.

Stdio mode is unchanged by this phase — local `appsettings.json` / env var overrides remain the supported path for individual developers, as already documented in Phase 5.

> **Status update (ahead of schedule):** Tasks 9–11 below were implemented directly, before
> Phases 0–5, because an external collaborator asked for a working `docker compose` + `.env`
> setup immediately. What actually shipped differs from the original draft in two ways the
> executor must account for:
>
> 1. **Port handling is simpler and single-sourced.** `docker-compose.yml` reads one `MCP_PORT`
>    value from `.env` (default `5101`) and uses it for *both* the published host port and
>    `SQLMCP_Mcp__Port`, so they cannot drift apart: `ports: ["${MCP_PORT:-5101}:${MCP_PORT:-5101}"]`
>    plus `SQLMCP_Mcp__Port: ${MCP_PORT:-5101}`. The Dockerfile `HEALTHCHECK` was written to match:
>    `curl -f "http://localhost:${SQLMCP_Mcp__Port:-5101}/"` (not a hardcoded `5101`).
> 2. **No `Security__`/`Audit__` environment variables or `audit-data` volume yet** — Phases 2 and
>    4 (the startup gate and the audit log) had not been executed when Tasks 10–11 shipped, so
>    those options don't exist in code yet and were correctly left out of `docker-compose.yml`.
>    **When Phase 2 lands:** add `SQLMCP_Security__VerifyLoginsAtStartup` and
>    `SQLMCP_Security__AllowWritableLogin` to the `environment:` block. **When Phase 4 lands:**
>    add `SQLMCP_Audit__Enabled` and `SQLMCP_Audit__Path: /data/audit-log.jsonl` plus a
>    `volumes: [audit-data:/data]` entry and the corresponding top-level `volumes: {audit-data:}`
>    — do not skip this, an un-persisted audit trail on a shared server defeats its purpose.
>
> Task 9 is done as specified. Task 10 is done as specified except for the healthcheck port fix
> noted above. Task 11's `docker-compose.yml` and `.env.example` exist but need the Security/Audit
> additions above once those phases land — treat Task 11's steps below as already-completed for
> the parts they cover, and as the checklist for the remaining Security/Audit wiring.
> `README.md` already documents the Docker path (a "Running HTTP mode with Docker" subsection
> under "HTTP Mode (powerusers)"); Task 12 should extend that section rather than assume it
> doesn't exist yet.

### Task 9: Make the HTTP bind address configurable

Docker port-mapping (`-p 5101:5101`) only works if the process inside the container listens on `0.0.0.0` (all interfaces), not `localhost` (loopback-only). `Program.cs` currently hardcodes `localhost`. This must be configurable so local `dotnet run -- --sse` keeps defaulting to `localhost` (safest default for a lone developer) while the container overrides it to `0.0.0.0`.

**Files:**
- Modify: `Program.cs`
- Modify: `appsettings.example.json`

**Interfaces:**
- Produces: `Mcp:BindAddress` config key (default `"localhost"`), overridable via `SQLMCP_Mcp__BindAddress`.

- [ ] **Step 1: Read the bind address from config**

In `Program.cs`, in the `useSse` branch, change:

```csharp
var port = builder.Configuration.GetValue<int>("Mcp:Port", 5101);
builder.WebHost.UseUrls($"http://localhost:{port}");
```

to:

```csharp
var port = builder.Configuration.GetValue<int>("Mcp:Port", 5101);
var bindAddress = builder.Configuration.GetValue<string>("Mcp:BindAddress", "localhost");
builder.WebHost.UseUrls($"http://{bindAddress}:{port}");
```

- [ ] **Step 2: Update the startup log line to show the real bind address**

Change:

```csharp
Console.Error.WriteLine($"[SqlSchemaMcp] HTTP mode — http://localhost:{port}/");
```

to:

```csharp
Console.Error.WriteLine($"[SqlSchemaMcp] HTTP mode — http://{bindAddress}:{port}/");
```

- [ ] **Step 3: Document the new key in the example config**

In `appsettings.example.json`, add to the `Mcp` section:

```json
"Mcp": {
  "Port": 5101,
  "BindAddress": "localhost"
}
```

Add a one-line comment in `README.md`'s existing `SQLMCP_Mcp__Port` table row area: a new row `| SQLMCP_Mcp__BindAddress | Override the HTTP bind address (use 0.0.0.0 inside a container) |`.

- [ ] **Step 4: Manual verification**

Run: `dotnet run -- --sse`
Expected stderr: `[SqlSchemaMcp] HTTP mode — http://localhost:5101/` (unchanged default behaviour).

Run: `SQLMCP_Mcp__BindAddress=0.0.0.0 dotnet run -- --sse` (or set the env var per your shell)
Expected stderr: `[SqlSchemaMcp] HTTP mode — http://0.0.0.0:5101/`, and `curl http://localhost:5101/` still succeeds locally.

No automated test — this is host-binding infrastructure, not decidable logic. The manual check above is the acceptance criterion; note it in the PR description.

- [ ] **Step 5: Commit**

```bash
git add Program.cs appsettings.example.json README.md
git commit -m "feat: make HTTP bind address configurable for container deployment"
```

### Task 10: Dockerfile and .dockerignore

**Files:**
- Create: `Dockerfile`
- Create: `.dockerignore`

**Interfaces:**
- Produces: a buildable image that runs the HTTP transport, binds `0.0.0.0`, and exposes a healthcheck against the existing plain-GET `/` endpoint already implemented in `Program.cs`.

- [ ] **Step 1: Write the Dockerfile**

Create `Dockerfile`:

```dockerfile
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY SqlSchemaMcp.csproj ./
RUN dotnet restore SqlSchemaMcp.csproj
COPY . .
RUN dotnet publish SqlSchemaMcp.csproj -c Release -o /app/publish --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime
WORKDIR /app

# curl is required for the HEALTHCHECK below; the base image does not ship it.
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /app/publish .
COPY appsettings.example.json ./appsettings.json

# Non-secret defaults only. Real connection strings, audit path, and security
# options are injected as SQLMCP_/Audit__/Security__ environment variables at
# `docker run` / compose / orchestrator time — never baked into the image.
ENV SQLMCP_Mcp__BindAddress=0.0.0.0

EXPOSE 5101

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:5101/ || exit 1

ENTRYPOINT ["dotnet", "SqlSchemaMcp.dll", "--", "--sse"]
```

- [ ] **Step 2: Write .dockerignore**

Create `.dockerignore`:

```
bin/
obj/
.vs/
appsettings.json
constraints.json
audit-log.jsonl
docs/
SqlSchemaMcp.Tests/
.git/
.gitignore
.env
*.md
```

- [ ] **Step 3: Build the image**

Run: `docker build -t sql-schema-mcp:local .`
Expected: build completes with `Successfully tagged sql-schema-mcp:local` (or BuildKit's equivalent final line).

- [ ] **Step 4: Run it with a throwaway config and confirm the healthcheck goes healthy**

Run (adjust the connection string to any reachable test database, or a deliberately unreachable one just to confirm the process starts and serves `/`):

```bash
docker run -d --name sql-schema-mcp-test -p 5101:5101 \
  -e SQLMCP_SqlServer__Databases__poc="Server=host.docker.internal;Database=Test;Trusted_Connection=true;TrustServerCertificate=true;" \
  -e SQLMCP_Security__VerifyLoginsAtStartup=false \
  sql-schema-mcp:local
```

Run: `docker ps` (repeat after ~15s)
Expected: `STATUS` column shows `healthy` (not `starting` or `unhealthy`).

Run: `curl http://localhost:5101/`
Expected: `{"status":"ok","service":"SqlSchemaMcp"}`

Clean up: `docker rm -f sql-schema-mcp-test`

- [ ] **Step 5: Commit**

```bash
git add Dockerfile .dockerignore
git commit -m "feat: add Dockerfile for HTTP-mode deployment"
```

### Task 11: docker-compose for repeatable local/server runs

**Files:**
- Create: `docker-compose.yml`
- Create: `.env.example`
- Modify: `.gitignore`

**Interfaces:**
- Produces: a `docker compose up` path that injects secrets from a local `.env` file (gitignored) and persists the audit log in a named volume across container restarts.

- [ ] **Step 1: Write docker-compose.yml**

Create `docker-compose.yml`:

```yaml
services:
  sql-schema-mcp:
    build: .
    ports:
      - "5101:5101"
    environment:
      SQLMCP_SqlServer__Databases__poc: ${SQLMCP_POC_CONNECTION_STRING}
      SQLMCP_SqlServer__Databases__azure: ${SQLMCP_AZURE_CONNECTION_STRING}
      SQLMCP_Security__VerifyLoginsAtStartup: "true"
      SQLMCP_Security__AllowWritableLogin: "false"
      SQLMCP_Audit__Enabled: "true"
      SQLMCP_Audit__Path: /data/audit-log.jsonl
    volumes:
      - audit-data:/data
    restart: unless-stopped

volumes:
  audit-data:
```

- [ ] **Step 2: Write the committed env template**

Create `.env.example`:

```
SQLMCP_POC_CONNECTION_STRING=Server=YOUR_SERVER;Database=YOUR_POC_DB;User Id=sqlschema_ro;Password=CHANGE_ME;TrustServerCertificate=true;
SQLMCP_AZURE_CONNECTION_STRING=Server=YOUR_SERVER.database.windows.net;Database=YOUR_AZURE_DB;Authentication=Active Directory Default;
```

- [ ] **Step 3: Ignore the real .env**

Add to `.gitignore` (it already ignores `audit-log*.jsonl` from Phase 5 Task 8):

```
.env
```

- [ ] **Step 4: Verify the compose stack starts and the audit volume survives a restart**

Run: `cp .env.example .env` then fill in a real (or deliberately-unreachable-but-syntactically-valid) connection string.
Run: `docker compose up -d --build`
Expected: container reaches `healthy` (`docker compose ps`).

Exercise a tool call that triggers an audit write (e.g. `ExecuteQuery` via a connected client, or temporarily `docker exec` a curl against the MCP endpoint per the `ModelContextProtocol` transport contract), then:
Run: `docker compose restart sql-schema-mcp` followed by `docker compose exec sql-schema-mcp cat /data/audit-log.jsonl`
Expected: prior audit entries are still present after the restart (the named volume persisted them).

Clean up: `docker compose down` (add `-v` only if you intend to discard the audit volume — do not do this on a real shared instance).

- [ ] **Step 5: Commit**

```bash
git add docker-compose.yml .env.example .gitignore
git commit -m "feat: add docker-compose for repeatable HTTP-mode deployment with persisted audit log"
```

### Task 12: Documentation — deployment guide and updated security posture

**Files:**
- Modify: `README.md` (new "Deploying HTTP mode centrally" section)
- Modify: `docs/security-posture.md` (network-only auth requirement, Backlog section)

**Interfaces:**
- None (documentation only).

- [ ] **Step 1: Add the deployment section to README.md**

Insert a new section after the existing "HTTP Mode (powerusers)" section:

```markdown
## Deploying HTTP Mode Centrally (shared team instance)

Run one instance on a server so a team shares it instead of each developer running stdio
locally. This is a deliberate trust boundary change from local stdio — read
`docs/security-posture.md` before exposing this to anyone but yourself.

### Requirements

- Docker (or Docker-compatible runtime) on the target server.
- The server's port (default 5101) reachable ONLY from inside your VPN or behind a
  firewall rule scoped to your team. **Do not expose this port to the public internet.**
  There is currently no application-level authentication — see the Backlog note in
  `docs/security-posture.md`. Network-level access control is the only access control.
- A dedicated read-only SQL login per database (see `docs/security-posture.md`).

### Deploy

```bash
git clone <this-repo> && cd SqlSchemaMcp
cp .env.example .env          # fill in real connection strings
docker compose up -d --build
```

Confirm health: `curl http://<server>:5101/` → `{"status":"ok","service":"SqlSchemaMcp"}`

### Point Claude Code at the shared instance

```json
{
  "mcpServers": {
    "sql-schema": {
      "type": "http",
      "url": "http://<server-on-your-vpn>:5101/"
    }
  }
}
```

### Operating it

- Audit trail: `docker compose exec sql-schema-mcp cat /data/audit-log.jsonl` (persisted in the
  `audit-data` named volume across restarts and image rebuilds).
- Logs: `docker compose logs -f sql-schema-mcp`.
- Update: `git pull && docker compose up -d --build` (brief downtime during rebuild).
- Secrets rotation: update `.env` on the server, then `docker compose up -d` to recreate the
  container with the new environment.
```

- [ ] **Step 2: Update docs/security-posture.md with the HTTP deployment posture and the backlog**

Add a new section to `docs/security-posture.md` (created in Phase 5 Task 8):

```markdown
## Shared HTTP-mode deployment (Phase 6)

When run centrally via Docker for a team, the trust model changes from "only I can reach
this process" to "everyone on the network segment can reach this process". Two decisions
were made deliberately for the first deployable version, and both are tracked as follow-up
work rather than shipped as final:

1. **No application-level authentication.** The OAuth endpoints in `Program.cs` satisfy the
   MCP client's discovery handshake only — they accept any token and do not gate access.
   Access control for the shared instance is network-level only: VPN or firewall rules that
   restrict which machines can reach the port. Do not expose the port to the public internet
   or to any network segment broader than your team. Because there is no per-user
   authentication, the audit log (Phase 4) cannot attribute a tool call to a specific person —
   only that it came from the shared instance.
2. **Secrets via environment variables, not a centralized secret store.** Connection strings
   and other secrets are injected as `SQLMCP_`-prefixed environment variables at container
   start (via `.env` + docker compose, or your orchestrator's own secret injection). This is
   adequate for a small team on a controlled server, but is not centrally rotatable/auditable
   the way a secret store is.

### Backlog (not yet built)

- Real per-user authentication for HTTP mode (e.g. static API keys per user/team as a first
  step, or full Entra ID / Azure AD OAuth for SSO) so the audit log can attribute calls to a
  person and access can be revoked per-user without redeploying.
- Azure Key Vault + Managed Identity for server secrets, replacing plain environment
  variables — removes secrets from the deploy configuration entirely and centralizes rotation.
```

- [ ] **Step 3: Commit**

```bash
git add README.md docs/security-posture.md
git commit -m "docs: document central HTTP-mode deployment and its tracked security gaps"
```

---

## Self-Review (completed against the four decisions)

**Spec coverage:**
- Decision 1 (change the promise, keep SELECT) → Phase 5 Task 8; `execute_query` retained throughout, only its validator changed.
- Decision 2 (enforce read-only more) → Phase 1 (allowlist validator) + Phase 2 (startup gate).
- Decision 3 (tests + audit-logging + error sanitisation) → Phase 0 (test project), Phase 3 (SafeError), Phase 4 (audit log). Every new component ships with tests.
- Decision 4 (deploy HTTP mode centrally, secrets handling) → Phase 6: configurable bind address (Task 9), Docker image with healthcheck (Task 10), docker-compose with persisted audit volume (Task 11), deployment + security-posture docs including the explicitly tracked auth/secrets backlog (Task 12).

**Placeholder scan:** No "TBD"/"add validation"/"similar to Task N". Three explicit implementer notes flagged (ScriptDom node-name confirmation in Task 1; package version confirmation in Task 0; manual-verification-only for Tasks 4 and 9 because they wire real infrastructure rather than decidable logic) — these are verification steps, not missing content.

**Type consistency:** `SqlValidationResult` (Task 1), `LoginPermissionResult`/`GateDecision`/`IPermissionProbe` (Tasks 3–4), `AuditEntry`/`IAuditLog`/`AuditOptions`/`SecurityOptions` (Tasks 3, 6) are used with identical signatures in their consumers (Program.cs wiring, tool wrappers). `SafeError(Exception, string?)` signature matches its call sites. `Mcp:BindAddress` (Task 9) is consumed identically by `Program.cs`, `appsettings.example.json`, and the Dockerfile's `ENV`.

**Open risks for the executor:**
- ScriptDom visitor node names vary by package version — Task 1 tests pin the required behaviour; adjust the visitor to satisfy them.
- The startup gate uses `Environment.Exit(1)`; if a future task adds graceful shutdown, revisit.
- Audit cross-process interleave is an accepted v1 caveat (same as `constraints.json`); Phase 6 makes this more visible (a shared instance means more concurrent tool calls) but does not change the caveat.
- Phase 6 deliberately ships without application-level HTTP auth and without a secret store — both are called out to the user, written into `docs/security-posture.md` as a Backlog, and must not be silently treated as "done" during review.

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-07-07-readonly-audit-hardening.md`. Two execution options:

1. **Subagent-Driven (recommended)** — a fresh subagent per task, review between tasks, fast iteration.
2. **Inline Execution** — execute tasks in this session with checkpoints for review.

Recommended split when handing to Opus: Phases 0–2 (read-only enforcement) as one review unit, Phase 3 (sanitisation) standalone, Phase 4 (audit) as one unit, Phase 5 (docs) as one unit, Phase 6 (deployment) last since it depends on Phase 4's audit log and Phase 5's security-posture doc existing. Each phase leaves the build green and is independently reviewable.
