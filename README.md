# SqlSchemaMcp

A read-only MCP server that exposes SQL Server schema information to Claude. Supports cross-database
analysis, naming convention review, missing constraint detection, complexity analysis, pipeline health,
and migration planning. No query execution, no data modification — schema and metadata only.

---

## Quick Start (stdio — recommended)

1. Clone the repo
2. Copy `appsettings.example.json` to `appsettings.json` and fill in your connection strings
3. Add the server to `~/.claude.json` (see below)
4. Claude Code starts the process automatically — or run `dotnet run` from the repo root manually

The solution file (`SqlSchemaMcp.sln`) is at the repo root alongside the project, so `dotnet run`
and `dotnet build` work from there without any `--project` flag.

---

## HTTP Mode (powerusers)

Use HTTP mode when you want to run the server once and connect multiple Claude instances to it.
One shared process means no conflict on simultaneous `AddConstraint`/`RemoveConstraint` writes to
`constraints.json` — safer than running stdio per session.

Start the server manually:

```
dotnet run -- --sse
```

The `--sse` flag is named for historical reasons; the actual transport is streamable HTTP.
The server starts on `http://localhost:5101/` (port configurable via `Mcp:Port`).

**Stdio and multiple sessions:** stdio spawns a separate subprocess per Claude session with its
own SQL connection pool — no transport conflict. The only shared state is `constraints.json` on
disk: two sessions writing simultaneously can overwrite each other (no file lock). For read-only
work this is not an issue; for constraint writes prefer HTTP mode.

---

## Configuration

### appsettings.json

```json
{
  "Mcp": {
    "Port": 5101
  },
  "SqlServer": {
    "Databases": {
      "poc":   "Server=localhost;Database=PocDb;Trusted_Connection=true;TrustServerCertificate=true;",
      "azure": "Server=myserver.database.windows.net;Database=AzureDb;Authentication=Active Directory Default;"
    }
  }
}
```

`appsettings.json` is gitignored. Copy from `appsettings.example.json` to get started.

### Environment variable overrides

These env vars are read by `Program.cs` with prefix `SQLMCP_` and override any value in
`appsettings.json`. They work in both stdio and HTTP mode.

| Variable | Description |
|----------|-------------|
| `SQLMCP_SqlServer__Databases__poc` | Override the `poc` connection string |
| `SQLMCP_SqlServer__Databases__azure` | Override the `azure` connection string |
| `SQLMCP_Mcp__Port` | Override the HTTP port (HTTP mode only) |

The `__` separator maps to nested JSON keys. Add any database name you configure in appsettings.

---

## Claude Code MCP Registration

Add entries to `~/.claude.json` (your user-wide Claude Code config).

### Stdio (default)

Claude Code starts the process automatically for you.

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

`cwd` points to the repo root (where `SqlSchemaMcp.sln` lives). No `--project` flag needed.

### HTTP (after starting `dotnet run -- --sse`)

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

## Available Tools

### SchemaTools
Discovery and detail for all schema objects.

| Tool | Description |
|------|-------------|
| `ListTables` | List tables with approximate row counts and descriptions |
| `ListViews` | List all views |
| `ListProcedures` | List stored procedures with last modified date |
| `ListFunctions` | List user-defined functions (scalar, inline TVF, multi-statement TVF) |
| `ListTriggers` | List DML triggers with parent table, events, and enabled status |
| `ListSynonyms` | List synonyms with their target object names |
| `ListCheckConstraints` | List CHECK constraints with their expressions |
| `ListDdlTriggers` | List database-level DDL triggers |
| `GetTableSchema` | Full column schema, foreign keys, and indexes for a table |
| `GetViewDefinition` | Full T-SQL body of a view |
| `GetProcedureDefinition` | Full T-SQL body of a stored procedure |
| `GetFunctionDefinition` | Full T-SQL body of a function |
| `GetTriggerDefinition` | Full T-SQL body of a DML trigger |
| `GetDdlTriggerDefinition` | Full T-SQL body of a DDL trigger |
| `FindReferences` | Find all procs and views that reference a given object |
| `SearchDefinitions` | Search for a keyword across all proc and view bodies |

### AnalysisTools
Schema quality and refactoring signals.

| Tool | Description |
|------|-------------|
| `AnalyzeNamingConventions` | Flag Hungarian prefixes, version suffixes, ALL_CAPS, snake_case |
| `AnalyzeMissingForeignKeys` | Find FK-pattern columns with no actual FK constraint |
| `AnalyzeMissingIndexes` | Find FK and common filter columns with no index |
| `AnalyzeProcComplexity` | Per-proc: line count, cursors, temp tables, dynamic SQL, NOLOCK |
| `AnalyzeViewComplexity` | Per-view: line count and nested view references |

### CompareTools
Cross-database comparison.

| Tool | Description |
|------|-------------|
| `CompareTables` | Tables only in db1 / only in db2 / in both |
| `CompareProcs` | Procs only in db1 / only in db2 / in both |
| `CompareViews` | Views only in db1 / only in db2 / in both |
| `CompareTable` | Column-level diff for a specific table |
| `CompareView` | Existence, line count diff, and referenced tables for a view |
| `CompareProc` | Existence, line count diff, and referenced tables for a proc |

### ConstraintTools
Persistent context annotations stored in `constraints.json`.

| Tool | Description |
|------|-------------|
| `ListConstraints` | List all annotations, optionally filtered by database or object name |
| `AddConstraint` | Add an annotation (LegacyDependent, DoNotRename, KnownTechnicalDebt, etc.) |
| `RemoveConstraint` | Remove an annotation by id |
| `UpdateConstraint` | Update the description of an existing annotation |

### PipelineTools
ETL pipeline and staging table health.

| Tool | Description |
|------|-------------|
| `ListFeeds` | Group staging tables by feed name with latest run date |
| `AnalyzePipelineHealth` | Flag overdue, cleanup-late, and excess staging tables |
| `CompareFeedSchema` | Diff the most recent staging table against the permanent table |

### DiagnosticsTools
Server-level diagnostics (requires appropriate permissions).

| Tool | Description |
|------|-------------|
| `ListSqlAgentJobs` | List all SQL Agent jobs with enabled status and last run outcome |
| `ListFailedJobs` | Jobs that failed in the last 7 days with error messages |
| `GetJobHistory` | Step-by-step run history for a specific job |
| `GetExpensiveQueries` | Top N queries by CPU time from dm_exec_query_stats |
| `GetWaitStats` | Top wait types from dm_os_wait_stats |
| `ListLinkedServers` | All linked servers with provider and data source |
| `FindLinkedServerReferences` | Procs and views that contain linked server (four-part) calls |
| `ListServiceBrokerQueues` | User-defined Service Broker queues and services |
| `ListClrAssemblies` | User-defined CLR assemblies registered in the database |

### DataTools
Row-level sampling and column statistics (read-only).

| Tool | Description |
|------|-------------|
| `SampleTable` | Return a small sample of rows (max 100) |
| `AnalyzeColumn` | Distribution stats: null count, distinct count, min/max, length stats |
| `FindNullableWithNoNulls` | Nullable columns that contain zero NULL values in practice |
| `FindDuplicates` | Rows with duplicate values across specified columns |

### SecurityTools

| Tool | Description |
|------|-------------|
| `ListDatabaseUsers` | List users and their assigned roles |
| `ListObjectPermissions` | List explicit GRANT/DENY permissions on tables, views, and procs |

---

## Troubleshooting

**Stdout corruption in stdio mode**
If Claude shows garbled responses or protocol errors, another process may be writing to stdout.
Check startup scripts, .NET startup banners, or logging configuration. All app output goes to
stderr — stdout is reserved for MCP JSON-RPC.

**Unknown database error**
`ERROR: Unknown database 'x'. Available: poc, azure`
The database name passed to a tool does not match any key in `SqlServer.Databases`. Check
`appsettings.json` or the active env var overrides.

**Port already in use (HTTP mode)**
Change the port via `appsettings.json` (`Mcp:Port`) or env var `SQLMCP_Mcp__Port` and update the
`url` in your `.claude.json` accordingly.
