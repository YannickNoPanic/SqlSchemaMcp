# SqlSchemaMcp

A read-only MCP server for SQL Server. It exposes full schema and metadata, and supports
read-only data access for debugging: a validated single-SELECT `execute_query` tool plus
bounded row sampling and column-distribution tools. It never modifies data or schema.

The runtime has a capability-based engine boundary. SQL Server is the complete engine.
PostgreSQL and MariaDB currently support schema browsing plus shared snapshot-backed
analysis; capabilities outside that slice return `UNSUPPORTED:`.

Read-only is enforced on two levels: a startup gate that refuses to run against a login
with write permissions, and a parser-based allowlist that permits only SELECT/CTE queries.
See docs/security-posture.md before installing.

Also supports cross-database analysis, naming convention review, missing constraint detection,
complexity analysis, pipeline health, and migration planning.

---

## Quick Start (stdio — recommended)

1. Clone the repo
2. Copy `appsettings.example.json` to `appsettings.json` and fill in your connection strings
3. Add the server to `~/.claude.json` (see below)
4. Claude Code starts the process automatically — or run `dotnet run` from the repo root manually

The solution file (`SqlSchemaMcp.sln`) is at the repo root alongside the project, so `dotnet run`
and `dotnet build` work from there without any `--project` flag.

### Local .NET tool install

For a more production-like local install, pack and install the tool from the repo:

```bash
dotnet pack SqlSchemaMcp.csproj -c Release
dotnet tool install --global --add-source ./bin/Release SqlSchemaMcp
```

Then register the global command instead of `dotnet run`:

```json
{
  "mcpServers": {
    "sql-schema": {
      "type": "stdio",
      "command": "sql-schema-mcp"
    }
  }
}
```

Use `dotnet tool update --global --add-source ./bin/Release SqlSchemaMcp` after rebuilding a
new local package.

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

### Running HTTP mode with Docker

```bash
cp .env.example .env    # fill in real connection strings, and MCP_PORT if 5101 is taken
docker compose up -d --build
```

The port is fully configurable via `MCP_PORT` in `.env` — it drives both the published host
port and the server's internal `Mcp:Port`, so the two never drift apart.

`poc` and `azure` in `docker-compose.yml`/`.env.example` are just example database keys, not
fixed names — rename them or add more by adding another `SQLMCP_SqlServer__Databases__<yourkey>`
line plus its own connection-string env var. If your login has write permissions and you
accept that risk, set `SQLMCP_ALLOW_WRITABLE_LOGIN=true` in `.env` instead of editing code.
The connection-string env var value must be the raw SQL Server connection string only, for example
`Server=...;Database=...;Encrypt=true;...`. Do not include another `NAME=` prefix inside the value.

Confirm it's up:

```
curl http://localhost:5101/
{"status":"ok","service":"SqlSchemaMcp"}
```

Point Claude Code at it the same way as any other HTTP-mode server (see below), using
`http://<host>:<MCP_PORT>/`. This binds the container to `0.0.0.0` internally; only publish
the port to a network you control (VPN/firewall) — there is no application-level
authentication on this transport yet.

**Running this as a shared team instance instead of your own local container is a deliberate
trust boundary change** — read **[docs/security-posture.md](docs/security-posture.md)**,
specifically the "Shared HTTP-mode deployment" section, before pointing anyone else's Claude
Code at it.

#### Audit trail (persisted across restarts)

The audit log is written to `/data/audit-log.jsonl` inside the container, which
`docker-compose.yml` maps to the named volume `audit-data` — it survives container restarts
and `docker compose up -d --build` rebuilds. Inspect it without stopping the server:

```bash
docker compose exec sql-schema-mcp cat /data/audit-log.jsonl
```

#### Point a teammate's Claude Code at the shared instance

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

#### Operating it

- **Logs:** `docker compose logs -f sql-schema-mcp`
- **Update:** `git pull && docker compose up -d --build` (brief downtime while the container rebuilds)
- **Secrets rotation:** edit `.env` on the server, then `docker compose up -d` to recreate the
  container with the new environment — no rebuild needed unless the image itself changed.

---

## Configuration

### appsettings.json

```json
{
  "Mcp": {
    "Port": 5101,
    "BindAddress": "localhost"
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

### Multi-engine config semantics

`SqlServer:Databases` is the backward-compatible SQL Server section. A bare string
connection string means SQL Server and remains the recommended config for current use.

The database loader also accepts object-shaped entries inside `SqlServer:Databases`
for PostgreSQL and MariaDB:

```json
{
  "SqlServer": {
    "Databases": {
      "poc": "Server=localhost;Database=PocDb;User Id=sqlschema_ro;Password=YOUR_SECRET;TrustServerCertificate=true;",
      "reporting": {
        "Engine": "Postgres",
        "ConnectionString": "Host=localhost;Database=Reporting;Username=readonly;Password=YOUR_SECRET"
      },
      "legacy": {
        "Engine": "MariaDb",
        "ConnectionString": "Server=localhost;Database=Legacy;User ID=readonly;Password=YOUR_SECRET"
      }
    }
  }
}
```

SQL Server supports the full tool set. PostgreSQL and MariaDB support the shared schema
capability and schema snapshot capability, which covers schema browsing and shared analysis
tools such as naming, missing foreign key, and missing index analysis. SQL Server-only,
query, data sampling, diagnostics, security, and pipeline tools return `UNSUPPORTED:` for
PostgreSQL/MariaDB until those engines implement the specific capability. If you need a
missing capability, ask the developer to add support for that engine.

Unsupported responses include the missing capability contract, for example:

```
UNSUPPORTED: Tool 'ExecuteQuery' is not available for engine 'Postgres'. Ask the developer to add 'IReadOnlyQueryCapability' support for this engine.
```

Startup read-only probing is SQL Server-specific. Object-form PostgreSQL or MariaDB entries
are kept in the resolver and routed to their engine projects, but they are not passed to the
SQL Server permission probe. Use read-only PostgreSQL/MariaDB credentials.

### Environment variable overrides

These env vars are read by `Program.cs` with prefix `SQLMCP_` and override any value in
`appsettings.json`. They work in both stdio and HTTP mode.

| Variable | Description |
|----------|-------------|
| `SQLMCP_SqlServer__Databases__poc` | Override the `poc` connection string |
| `SQLMCP_SqlServer__Databases__azure` | Override the `azure` connection string |
| `SQLMCP_Mcp__Port` | Override the HTTP port (HTTP mode only) |
| `SQLMCP_Mcp__BindAddress` | Override the HTTP bind address (use `0.0.0.0` inside a container) |
| `SQLMCP_Security__VerifyLoginsAtStartup` | Override `Security:VerifyLoginsAtStartup` |
| `SQLMCP_Security__AllowWritableLogin` | Override `Security:AllowWritableLogin` |
| `SQLMCP_Audit__Enabled` | Override `Audit:Enabled` |
| `SQLMCP_Audit__Path` | Override `Audit:Path` |

The `__` separator maps to nested JSON keys. Add any database name you configure in appsettings.
Connection-string values must be raw connection strings. If startup reports
`Keyword not supported: 'sqlmcp_azure_connection_string'`, the configured value contains an
environment-variable assignment instead of just the SQL Server connection string.

---

## Security and Audit

This server is read-only by design, enforced at two levels rather than by convention alone:

- **Startup gate** — on launch, every configured SQL Server database login is probed for write
  permissions. If any login can write, the server refuses to start (`Security:VerifyLoginsAtStartup`,
  default `true`; escape hatch `Security:AllowWritableLogin`, default `false`).
- **Statement allowlist** — `execute_query` parses every statement with the T-SQL parser and
  permits only a single SELECT or CTE. Writes, DDL, `EXEC`, `OPENQUERY`/`OPENROWSET`/`OPENDATASOURCE`,
  `SELECT ... INTO`, and multi-statement batches are all rejected.

Row-level data access is real, not simulated: `execute_query`, `SampleTableData`,
`AnalyzeColumnDistribution`, and `FindDuplicateRows` return actual row values (bounded to
500 rows per call, 30-second timeout). Every tool invocation is recorded to a JSON-lines audit
log (`audit-log.jsonl` by default, configurable via `Audit:Path`; disable with `Audit:Enabled=false`).

See **[docs/security-posture.md](docs/security-posture.md)** for the required database login
setup, what the audit log records, and the prompt-injection blast radius — read it before
pointing this server at a database you care about.

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

### RuntimeTools

Local readiness and capability discovery.

| Tool | Description |
|------|-------------|
| `ListConfiguredDatabases` | List configured database keys with engine and capability groups |
| `ListEngineCapabilities` | Show supported and unsupported capability groups for SQL Server, PostgreSQL, and MariaDB |
| `CheckConfiguration` | Summarize runtime configuration readiness without reading schema data |

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

**Unsupported capability**
`UNSUPPORTED:` means the database is configured and routed correctly, but its engine does not
implement the capability required by that tool yet. Run `ListEngineCapabilities` to see the
current engine matrix, then ask the developer to add the named capability if it is needed.

**Configuration self-test**
Run `CheckConfiguration` from Claude after connecting the MCP server. It reports configured
database keys, engine readiness notes, and whether no databases were loaded.

**Port already in use (HTTP mode)**
Change the port via `appsettings.json` (`Mcp:Port`) or env var `SQLMCP_Mcp__Port` and update the
`url` in your `.claude.json` accordingly.
