# Security posture

This server is read-only by design, but "read-only" is enforced, not assumed. Read this
before pointing it at any database, especially one containing real customer or production data.

---

## Required login: db_datareader only

Create a dedicated SQL login for this server with `db_datareader` and nothing else.
Do not reuse an application login, a migration login, or anything with elevated rights.

```sql
CREATE LOGIN sqlschema_ro WITH PASSWORD = '<strong-secret>';
CREATE USER sqlschema_ro FOR LOGIN sqlschema_ro;
ALTER ROLE db_datareader ADD MEMBER sqlschema_ro;
-- Do NOT add db_datawriter, db_ddladmin, db_owner, or grant CONTROL.
```

Do this for every SQL Server database configured under `SqlServer:Databases` (both `poc`
and `azure`, or whatever SQL Server names you configure). Object-form PostgreSQL or MariaDB
entries are recognized by the multi-engine resolver, but they are not probed by the SQL Server
permission gate until those engines provide their own permission probes.

---

## Two enforcement layers

Read-only is enforced at two independent levels. **The login is the primary defence; the
in-process validator is defence-in-depth, not a substitute for it.**

### SQL Server, PostgreSQL, and MariaDB

SQL Server has both enforcement pieces: `SqlServerPermissionProbe` checks login permissions
at startup, and `SqlStatementValidator` uses ScriptDom to reject non-SELECT statements for
`execute_query`.

PostgreSQL and MariaDB currently expose schema browsing and schema snapshot capabilities
only. They do not expose row-level query execution or data sampling, so no in-process
statement guard exists for those engines yet. Future PostgreSQL/MariaDB row-level tools must
provide their own read-only permission probe and statement guard before being enabled.
Capabilities outside the implemented schema/snapshot slice return `UNSUPPORTED:`.

### 1. Startup permission gate (primary defence)

On every launch (stdio or HTTP), `SqlServerPermissionProbe` (`Security/SqlServerPermissionProbe.cs`)
queries each configured SQL Server login for `sysadmin`, `db_owner`, `db_datawriter`, `db_ddladmin`
membership, and any explicit `INSERT`/`UPDATE`/`DELETE`/`ALTER`/`CONTROL`/`CREATE TABLE`
grant. `ReadOnlyStartupGate.Evaluate` (`Security/ReadOnlyStartupGate.cs`) then decides whether
the process is allowed to start:

- If any reachable login can write, the server **refuses to start** and exits with a
  `CRITICAL` message naming the database and the specific grants found.
- If a login is unreachable at startup (network blip, credential issue), the gate logs a
  `WARN` and continues — an unreachable login is treated as "unverified", not "writable".
  This means the gate cannot catch a write-capable login that happens to be unreachable
  at the moment of the probe; it only catches what it can see.

Config keys (`Configuration/SecurityOptions.cs`):

| Key | Default | Effect |
|---|---|---|
| `Security:VerifyLoginsAtStartup` | `true` | When `false`, the gate is skipped entirely and the server starts unconditionally. |
| `Security:AllowWritableLogin` | `false` | Escape hatch. When `true`, a writable login downgrades from a startup-blocking error to a `WARN` and the server starts anyway. |

Turning either of these on is a deliberate decision to trust something other than the
database permission model to keep this server read-only. Do not flip them without a reason.

### 2. Statement allowlist (defence-in-depth)

`SqlStatementValidator` (`Data/SqlStatementValidator.cs`) parses every string passed to
`execute_query` with the T-SQL parser (`Microsoft.SqlServer.TransactSql.ScriptDom`) and
permits **only** a single `SELECT` statement (a leading `WITH` CTE is fine). It rejects:

- Anything that isn't a `SELECT` — `INSERT`, `UPDATE`, `DELETE`, `EXEC`, DDL, etc.
- `SELECT ... INTO` (creates a table — a write via DDL)
- `OPENQUERY`, `OPENROWSET`, `OPENDATASOURCE` / ad-hoc remote table references, `OPENXML`
- More than one statement in the same batch

This is a parser-based allowlist, not a keyword blocklist — it cannot be bypassed by string
tricks that a naive substring check might miss. But it only governs what `execute_query`
accepts; it does nothing if the underlying login can already write. Layer 1 is what actually
prevents writes; layer 2 narrows what a well-behaved client (or a prompt-injected agent) can
even attempt.

---

## Data access is real

This server is not "schema only." `execute_query`, `SampleTableData`,
`AnalyzeColumnDistribution`, and `FindDuplicateRows` (`Tools/QueryTools.cs`,
`Tools/DataTools.cs`) return **actual row values** from the configured database, subject to
the `db_datareader` login's visibility:

- `execute_query` — any single SELECT/CTE, capped at 500 rows, 30-second command timeout.
- `SampleTableData` — up to 100 raw rows from a named table.
- `AnalyzeColumnDistribution` — null count, distinct count, and **min/max value** for a
  column. Min/max on a column like `DateOfBirth`, `Salary`, or `Email` can surface real,
  potentially sensitive values, not just statistics about them.
- `FindDuplicateRows` — grouped row values for duplicate-detection.

If a column must never be readable by this server — a PII column, a secret, anything you
don't want appearing in a query result or the audit log — do not rely on the tools to avoid
it. Enforce it at the database with a column-level `DENY`:

```sql
DENY SELECT ON dbo.Users (Ssn, PasswordHash) TO sqlschema_ro;
```

A `db_datareader` login can still be scoped down per-column with explicit `DENY`s; the
server has no way to know which columns are sensitive, so this must be done at the database.

---

## Audit trail

Every tool invocation (schema tools, analysis tools, and the data-access tools above) is
wrapped by `FileAuditLog.Invoke` (`Auditing/FileAuditLog.cs`) and recorded as one JSON line
per call in an append-only file:

- **What is recorded per entry:** UTC timestamp, tool name, database, a truncated summary
  of the parameters (including, for `execute_query`, the SQL text itself), elapsed
  milliseconds, and a `Success` flag.
- **What `Success` means:** `true` only if the tool call did not throw **and** its returned
  string does not start with the `ERROR:` or `UNSUPPORTED:` sentinel. A call that completes
  without an exception but returns a handled `ERROR:` result or unsupported-capability result
  is recorded as `Success: false`.
- **Where it lives:** `Audit:Path`, or by default a file named `audit-log.jsonl` in the
  project root (resolved by walking up from the executing assembly to the directory
  containing the `.csproj`).
- **Cross-process interleave caveat:** the file is opened with `FileShare.ReadWrite` so that
  multiple concurrent processes (e.g. several stdio sessions running at once) can append to
  the same file without one blocking the other. Writes within a single process are
  serialized by an internal lock, but there is no cross-process lock — under concurrent
  writers from separate processes, individual JSON lines are still written atomically as
  whole lines are appended, but two processes could interleave their appends in write order.
  Do not assume the file is sorted by wall-clock time across processes.
- **Disabling it:** set `Audit:Enabled=false`. When disabled, tool calls run without any
  auditing overhead — no file I/O happens at all. A write failure to the audit file itself
  (disk full, permissions) is swallowed silently so that auditing can never take down a
  query; it does not fall back to any other sink.

---

## HTTP mode

HTTP (streamable HTTP, started with `--sse`) has **no application-level authentication**.
The OAuth endpoints in `Program.cs` exist only to satisfy the MCP client's discovery
handshake — they do not validate tokens or gate access to any tool. Anyone who can reach the
bound host and port can call every tool this server exposes, including `execute_query`.

Treat HTTP mode as **localhost-only / network-perimeter-only**: bind to `localhost` (the
default `Mcp:BindAddress`) for a single machine, or if running it centrally (see the Docker
setup in the README), restrict reachability with a VPN or firewall. It is not a substitute
for real authentication and is not intended as a multi-tenant or public transport.

---

## Shared HTTP-mode deployment

Running one Docker instance centrally so a team shares it (see the README's "Running HTTP
mode with Docker" section) changes the trust model from "only I can reach this process" to
"everyone on the network segment can reach this process." Two things follow directly from
that, and neither is solved by application code today:

- **There is still no application-level authentication.** The OAuth endpoints in
  `Program.cs` (`/.well-known/oauth-protected-resource`, `/.well-known/oauth-authorization-server`)
  exist only to satisfy the MCP client's discovery handshake — they accept any token and do
  not gate access to any tool. Access control for a shared instance is **network-level
  only**: a VPN or firewall rule restricting which machines can reach the port. Do not expose
  the port to the public internet, and do not expose it to a network segment broader than
  your team.
- **The audit log cannot attribute a call to a person.** Because there is no per-user
  authentication, every call recorded by the audit trail (see above) looks the same
  regardless of which teammate's Claude Code made it — the log can tell you that a call came
  from the shared instance, not who was driving it.

### Backlog (not yet built)

- **Per-user authentication for HTTP mode.** Static API keys per user/team would be a
  reasonable first step; full Entra ID / Azure AD OAuth would additionally give SSO. Either
  would let the audit log attribute calls to a person and let access be revoked per-user
  without redeploying the whole instance.
- **Azure Key Vault + Managed Identity for server secrets.** Connection strings and other
  secrets currently arrive as plain `SQLMCP_`-prefixed environment variables via `.env` and
  docker compose. That's adequate for a small team on a controlled server, but it isn't
  centrally rotatable or auditable the way a secret store is.
- **Fix the hardcoded `localhost` in the OAuth discovery endpoints.** `Program.cs` builds the
  `resource`, `issuer`, `authorization_endpoint`, `token_endpoint`, and `registration_endpoint`
  values in `/.well-known/oauth-protected-resource` and `/.well-known/oauth-authorization-server`
  from a literal `http://localhost:{port}`, regardless of the actual configured
  `Mcp:BindAddress` or the host a remote client actually used to reach the server. A client
  connecting to the shared instance over the VPN receives discovery metadata pointing back at
  "localhost" rather than the real host. This is a real, present gap, not a hypothetical one —
  and worth fixing alongside the per-user-auth work above, since implementing real
  authentication will likely require reworking these same endpoints anyway.

---

## Prompt-injection blast radius

If an agent using this server is manipulated (via prompt injection, a malicious tool result,
or otherwise) into calling tools it shouldn't, the worst case is bounded by the two
enforcement layers above:

- It **can read** any data the configured `db_datareader` login can see — up to 500 rows per
  `execute_query` call, or up to 100 rows per `SampleTableData` call, with every call logged
  to the audit trail (tool, database, parameters, timestamp).
- It **cannot write, drop, alter, or execute** anything, because the startup gate refuses to
  run the process at all against a writable login, and the statement allowlist independently
  rejects anything that isn't a plain SELECT.
- It **cannot exfiltrate via linked servers or ad-hoc sources** through `execute_query` —
  `OPENQUERY`, `OPENROWSET`, and `OPENDATASOURCE` are rejected by the validator. (Read-only
  discovery of existing linked servers via `ListLinkedServers` and
  `FindLinkedServerReferences` in `DiagnosticsTools` is unaffected — those are schema
  metadata queries, not query execution.)

The realistic worst case is unauthorized *reading* of whatever the read-only login can see,
fully logged. It is not unauthorized writing, and it is not schema modification.
