# SqlSchemaMcp Agent Instructions

This file is the canonical repo-level memory for Claude, Codex, and other coding agents.
Global user instructions still apply. This file only records project-specific context and
overrides.

If a global `CLAUDE.md` or `AGENTS.md` rule conflicts with this file, this file wins for this
repository. Keep tool-specific permission settings out of this file.

## Project Purpose

SqlSchemaMcp is a .NET MCP server for database schema exploration and read-only analysis.

The product promise is read-only access:
- SQL Server supports the complete current tool surface.
- PostgreSQL and MariaDB currently support schema browsing plus shared snapshot-backed
  analysis.
- Missing engine capabilities must return the `UNSUPPORTED:` sentinel with a clear message
  telling the user to ask the developer to add the specific capability.
- The server never modifies data or schema.

Row-level data access is real and intentionally bounded: `execute_query`, table sampling,
column distribution, and duplicate analysis can return actual row values. Treat this as
sensitive behavior and keep `docs/security-posture.md` accurate whenever this changes.

## Current Architecture

The solution is intentionally pragmatic, not Clean Architecture or vertical slices.
Do not force the global architecture defaults onto this repo.

Current project layout:
- `SqlSchemaMcp.csproj`: host, DI wiring, MCP tool registration, stdio/HTTP startup.
- `SqlSchemaMcp.Abstractions`: engine contracts, shared models, capability resolver.
- `SqlSchemaMcp.SqlServer`: complete SQL Server engine implementation.
- `SqlSchemaMcp.Postgres`: PostgreSQL schema/snapshot capabilities.
- `SqlSchemaMcp.MariaDb`: MariaDB schema/snapshot capabilities.
- `Tools/`: MCP-facing tool classes. Keep these thin.
- `Data/`: host-level dispatchers and shared data/query helpers.
- `Security/`: startup read-only gate and permission evaluation.
- `Auditing/`: JSON-lines audit log.
- `Configuration/`: options and `.env` configuration loading.
- `SqlSchemaMcp.Tests/`: xUnit tests.

Tool classes should call dispatcher/query/capability services and return plain structured text.
Business logic belongs in engine/query/capability classes, not in the tool methods.

## Commands

Use these from the repository root:

```powershell
dotnet restore SqlSchemaMcp.sln
dotnet build SqlSchemaMcp.sln --no-restore
dotnet test SqlSchemaMcp.sln --no-restore
dotnet pack SqlSchemaMcp.csproj -c Release --no-restore
```

For HTTP mode:

```powershell
dotnet run -- --sse
docker compose up -d --build
```

The packaged tool command is `sql-schema-mcp`.

## Configuration Ownership

Use one place per kind of setting:
- `.env`: real database entries, connection strings, secrets, and deployment overrides.
- `appsettings.json`: optional local app defaults when `.env` is not wanted.
- `appsettings.example.json`: committed non-secret default shape.
- `docker-compose.yml`: container wiring only: build, env file, ports, volumes, networks,
  restart policy.

Do not put database keys, connection strings, audit settings, or security settings directly in
`docker-compose.yml`.

Configuration precedence is:
1. appsettings defaults
2. nearest `.env` loaded by `DotEnvConfiguration`
3. real process environment variables with prefix `SQLMCP_`

Environment variables win over `.env`; `.env` wins over appsettings.

## Security Rules

Read-only is enforced by both:
- startup SQL Server permission probing via the read-only startup gate
- parser-based SELECT/CTE allowlisting for `execute_query`

Do not weaken these checks without an explicit design discussion.

Dedicated read-only database credentials are required. SQL Server logins should have
`db_datareader` and no write/admin roles. PostgreSQL and MariaDB credentials should also be
read-only, even though their permission probes are not implemented yet.

HTTP mode has no real application-level authentication today. Treat shared HTTP deployments as
a network-bound trust decision and keep the warning in `docs/security-posture.md`.

Audit behavior matters. Tool calls that return `ERROR:` or `UNSUPPORTED:` are audit failures.
Do not bypass `IAuditLog` when adding new tools.

## Multi-Engine Rules

Avoid a god interface. Add small capability interfaces when an engine can support a behavior.

SQL Server-only features should remain SQL Server-only until another engine has a real,
tested implementation. Do not fake support by translating SQL Server catalog assumptions.

When a capability is unavailable, return a stable `UNSUPPORTED:` message that includes:
- tool name
- engine name
- missing capability contract
- "Ask the developer to add ..." guidance

## Testing Rules

Follow the global xUnit, FluentAssertions, and AAA conventions.

Add or update tests when changing:
- configuration loading and precedence
- capability dispatch
- `UNSUPPORTED:` behavior
- read-only validation or permission probing
- audit success/failure classification
- Docker/config ownership expectations

Prefer focused unit tests for contracts and dispatch behavior. Integration tests that need
real database containers should be explicit and isolated from normal unit-test assumptions.

## Documentation Rules

Keep README, `docs/security-posture.md`, `.env.example`, and `appsettings.example.json`
consistent when changing configuration, security, or deployment behavior.

Plan/spec lifecycle:
- Commit specs and plans that describe active or still-relevant architecture.
- Do not commit raw session prompts, personal scratch notes, or database/sample-data dumps.
- Completed implementation plans should be summarized into current docs or a short decision
  note when their details are no longer useful.
- Large historical plans should not stay active project context unless they explain a current
  architectural boundary.

Do not commit:
- `.claude/settings.local.json`
- `.claude.local.md`
- local `.env` files
- raw audit logs
- raw customer/database dumps
- ad hoc HTML refactoring notes

## Worktree Policy

Create feature worktrees under `.codex/worktrees` or `.claude/worktrees`.
Do not create worktrees at repository root or outside those directories unless the user asks.

## Code Style Overrides

Global C# style defaults apply unless existing local code establishes a more specific pattern.

Project-specific notes:
- Output returned to MCP clients should be plain structured text, not JSON or Markdown, unless a
  tool already has a different established contract.
- Use structured logging. Do not put secrets or raw connection strings in logs or tool output.
- Keep comments sparse and useful.
- Do not leave TODO comments in production code unless the user explicitly asks for tracked
  refactor breadcrumbs; prefer docs or issues for future work.
