# SqlSchemaMcp

## Purpose
A standalone read-only MCP server that exposes SQL Server schema information across two named
databases (poc and azure) to Claude. Used for cross-database analysis, naming convention review,
missing constraint detection, complexity analysis, and migration planning.

No query execution. No data access. Schema and metadata only.

## Architecture
Simple and clean — this is a PoC. No vertical slice overhead, no CQRS, no mediator.
Just clear folder separation: Tools call Repository, Repository talks to SQL Server.
Keep it easy to understand and easy to throw away or refactor later.

## Tech Stack
- .NET 10, C#
- `ModelContextProtocol` NuGet package (latest prerelease)
- `Microsoft.Data.SqlClient`
- `Microsoft.Extensions.Hosting` for DI and config
- Stdio transport (default) — SSE available via `--sse` flag

## Project Structure
```
SqlSchemaMcp/
├── SqlSchemaMcp.csproj
├── Program.cs
├── appsettings.json              # gitignored
├── appsettings.example.json      # committed
├── constraints.json              # gitignored
├── constraints.example.json      # committed
├── .gitignore
├── Configuration/
│   └── SqlServerOptions.cs       # Databases: Dictionary<string, string>
├── Tools/
│   ├── SchemaTools.cs            # ListTables, GetTableSchema, GetProcedureDefinition etc.
│   ├── AnalysisTools.cs          # Naming conventions, missing indexes/FKs, complexity
│   ├── CompareTools.cs           # Cross-database comparison
│   └── ConstraintTools.cs        # Read/write constraints.json
└── Data/
    ├── SqlServerRepository.cs    # All SQL queries — shared across tool classes
    └── ConstraintRepository.cs   # JSON file read/write
```

The `Tools/` classes are thin — they receive DI dependencies, call repository methods,
and return strings. Business logic (SQL, JSON manipulation) lives in `Data/`.

## Configuration

### appsettings.json (gitignored)
```json
{
  "SqlServer": {
    "Databases": {
      "poc":   "Server=localhost;Database=PocDb;Trusted_Connection=true;TrustServerCertificate=true;",
      "azure": "Server=myserver.database.windows.net;Database=AzureDb;Authentication=Active Directory Default;"
    }
  }
}
```

### appsettings.example.json (commit this)
```json
{
  "SqlServer": {
    "Databases": {
      "poc":   "Server=YOUR_SERVER;Database=YOUR_POC_DB;Trusted_Connection=true;TrustServerCertificate=true;",
      "azure": "Server=YOUR_SERVER.database.windows.net;Database=YOUR_AZURE_DB;Authentication=Active Directory Default;"
    }
  }
}
```

Env var override: `SQLMCP_SqlServer__Databases__poc=...` and `SQLMCP_SqlServer__Databases__azure=...`

If a tool receives an unknown database name, return a clear error listing the configured names.

### .gitignore
```
appsettings.json
constraints.json
bin/
obj/
```

---

## Tools

### SchemaTools.cs
Discovery:
- `ListTables(database, schemaFilter?, nameFilter?)` — names + approx row counts + MS_Description
- `ListViews(database, nameFilter?)` — view names
- `ListProcedures(database, nameFilter?)` — proc names + last modified date

Detail:
- `GetTableSchema(database, tableName)` — columns (type, nullable, default, PK/FK), indexes, MS_Description per column
- `GetViewDefinition(database, viewName)` — full body from sys.sql_modules
- `GetProcedureDefinition(database, procName)` — full T-SQL body from sys.sql_modules

Cross-cutting:
- `FindReferences(database, objectName)` — procs/views referencing this object via sys.dm_sql_referencing_entities
- `SearchDefinitions(database, keyword)` — keyword search across all proc and view bodies

### AnalysisTools.cs
- `AnalyzeNamingConventions(database)` — flag: hungarian prefixes (tbl_, sp_, vw_), mixed casing,
  version suffixes (_v2, _OLD, _FINAL, _backup), inconsistent abbreviations. Group by violation type.
- `AnalyzeMissingForeignKeys(database)` — columns matching FK name patterns (OrganisationId, TenantId,
  CustomerId, UserId etc.) with no actual FK constraint. Cross-ref against PKs in the same database.
- `AnalyzeMissingIndexes(database)` — FK columns and common filter columns (IsActive, Status,
  CreatedAt, TenantId, OrganisationId) with no index. Use sys.index_columns to check.
- `AnalyzeProcComplexity(database, nameFilter?)` — per proc: line count, table count, presence of
  cursors / temp tables / dynamic SQL / NOLOCK hints. Flag refactor candidates (>200 lines OR cursor OR dynamic SQL).
- `AnalyzeViewComplexity(database, nameFilter?)` — per view: line count, nested view references
  (views referencing other views). Flag deeply nested ones.

### CompareTools.cs
All take `database1` and `database2` explicitly.

- `CompareTables(database1, database2)` — tables only in db1 / only in db2 / in both
- `CompareProcs(database1, database2)` — procs only in db1 / only in db2 / in both
- `CompareTable(database1, database2, tableName)` — column-level diff: missing columns, type mismatches, nullability differences
- `CompareProc(database1, database2, procName)` — exists in each? if both: line count diff + tables referenced diff (no full text diff)

### ConstraintTools.cs
Backed by `constraints.json` in the project root. Persists context across Claude sessions —
things Claude cannot infer from schema alone (legacy dependencies, migration status, known debt).

- `ListConstraints(filter?)` — all constraints, optionally filtered by database or object name
- `AddConstraint(database, objectName, type, description)` — adds entry, auto-generates id + addedAt
- `RemoveConstraint(id)` — remove by id
- `UpdateConstraint(id, description)` — update description

#### Constraint types (enforce these exact string values):
- `LegacyDependent`     — legacy or 3rd party system depends on this object's current signature
- `SharedObject`        — intentionally exists in both databases, changes must be coordinated
- `MigrationInProgress` — intentionally duplicated during migration
- `DoNotRename`         — renaming breaks something outside this codebase
- `DoNotRemove`         — referenced by something not visible in this database
- `KnownTechnicalDebt`  — intentionally bad, known, tracked
- `Note`                — freeform context

#### constraints.json
```json
{
  "constraints": [
    {
      "id": "c1",
      "database": "poc",
      "objectName": "tbl_OrgData",
      "type": "LegacyDependent",
      "description": "Used directly by Classic ASP pages. Column names and proc signatures cannot change.",
      "addedAt": "2025-01-01T00:00:00Z"
    }
  ]
}
```

#### constraints.example.json
```json
{
  "constraints": []
}
```

---

## Data Layer

### SqlServerRepository.cs
Single shared repository. Injected into all Tool classes via DI.
Receives the configured `Dictionary<string, string>` and opens connections by name.

Pattern for every method:
```csharp
public async Task<string> MethodName(string database, ...)
{
    if (!_databases.TryGetValue(database, out var connectionString))
        return $"ERROR: Unknown database '{database}'. Available: {string.Join(", ", _databases.Keys)}";

    try
    {
        await using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync();
        // ... query
        return result;
    }
    catch (Exception ex)
    {
        return $"ERROR: {ex.Message}";
    }
}
```

### ConstraintRepository.cs
Reads and writes `constraints.json` relative to the executing assembly location.
Use `System.Text.Json` with JsonSerializerOptions for pretty-printed output.
Generate constraint ids as short random strings (e.g. 8-char hex).

---

## Output Format
Plain text with ASCII structure — headers, dashes, aligned columns.
No JSON output, no markdown. Claude reasons better over plain structured text during analysis.

Example table schema output:
```
TABLE: [dbo].[Organisations]
────────────────────────────────────────────────────────────
Column                         Type                   Null  PK    Default
──────────────────────────────────────────────────────────────────────────
Id                             int                    NO    PK
Name                           nvarchar(255)          NO
TenantId                       uniqueidentifier       NO          -- no FK constraint defined
IsActive                       bit                    NO          (1)
CreatedAt                      datetime2              NO          (getutcdate())

FOREIGN KEYS
────────────────────────────────────────────────────────────
  (none)

INDEXES
────────────────────────────────────────────────────────────
  [IX_Organisations_TenantId]  (TenantId)
```

---

## MCP Registration

### Stdio mode (default — recommended for most users)

**Claude Code CLI** — add to `~/.claude.json`:
```json
{
  "mcpServers": {
    "sql-schema": {
      "type": "stdio",
      "command": "dotnet",
      "args": ["run", "--no-launch-profile"],
      "cwd": "C:/path/to/SqlSchemaMcp"
    }
  }
}
```

**Claude Desktop** — add to `%APPDATA%\Claude\claude_desktop_config.json`:
```json
{
  "mcpServers": {
    "sql-schema": {
      "type": "stdio",
      "command": "dotnet",
      "args": ["run", "--project", "C:\\path\\to\\SqlSchemaMcp"]
    }
  }
}
```

### HTTP mode (powerusers — multiple Claude instances)

Start the server: `dotnet run -- --sse`

The `--sse` flag is named for historical reasons; the actual transport is streamable HTTP.

**Claude Code CLI** — add to `~/.claude.json`:
```json
{
  "mcpServers": {
    "sql-schema": {
      "type": "http",
      "url": "http://localhost:5101/"
    }
  }
}
```

---

## What NOT to build
- No query execution or data preview
- No schema modification
- No SSE authentication/security — local use only
- No live schema refresh
- No full proc body text diff
- No VSA / mediator pattern — PoC, keep it simple