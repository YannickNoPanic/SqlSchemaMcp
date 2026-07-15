# Capability-Based Multi-Engine Design

## Goal

Introduce a future-proof multi-engine foundation without creating a broad god interface.
SQL Server remains the first fully supported engine, while PostgreSQL and MariaDB can be
added incrementally by implementing only the capabilities they actually support.

## Current Context

The current repo has a stable MCP tool layer in `Tools/*.cs`. Those classes are thin and
should remain unchanged. The SQL Server implementation lives mostly in large
`Data/*Queries.cs` classes that combine SQL access, formatting, and engine-specific behavior.

The existing multi-engine plan correctly preserves the tool surface, but its proposed
`IDbEngine` interface is too broad. It would force every engine to know about every tool,
including SQL Server-only concepts such as Agent jobs, wait stats, linked servers, CLR
assemblies, and SQL Server permission details.

## Design Principles

- `Tools/*.cs` stay unchanged.
- Engines implement small capability interfaces, not one broad interface.
- Unsupported functionality returns a clear user-facing `UNSUPPORTED:` result.
- `UNSUPPORTED:` is audited as `Success = false`.
- No remote notification, issue creation, webhook, email, or extra request tracking is built.
- SQL Server behavior must stay unchanged except for allowed namespace/project-reference moves.
- New engines can start with schema browsing and shared analysis before implementing query/data tools.
- SQL Server-only concepts must not leak into PostgreSQL or MariaDB contracts.

## Core Contracts

The abstractions project owns shared engine identity, config, schema models, sentinels, and
capability contracts.

```csharp
namespace SqlSchemaMcp.Abstractions;

public enum DatabaseEngine
{
    SqlServer,
    Postgres,
    MariaDb
}

public sealed record DatabaseConfig(
    string Name,
    DatabaseEngine Engine,
    string ConnectionString);

public interface IDatabaseEngine
{
    DatabaseEngine Kind { get; }
}
```

The resolver maps a configured database name to the engine kind and an optional capability.

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

Unknown database and unsupported capability responses are centralized.

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

## Capability Groups

Capability interfaces are grouped by behavior, not by database engine. Each interface should be
small enough that an engine can reasonably implement the whole contract.

### Schema Browsing

```csharp
namespace SqlSchemaMcp.Abstractions;

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

SQL Server-only schema extensions remain separate.

```csharp
namespace SqlSchemaMcp.Abstractions;

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

### Read-Only Query and Data Access

```csharp
namespace SqlSchemaMcp.Abstractions;

public interface IReadOnlyQueryCapability
{
    Task<string> ExecuteQuery(string database, string sql, CancellationToken ct);
}

public interface IDataSamplingCapability
{
    Task<string> SampleTableData(string database, string tableName, int rows, CancellationToken ct);
    Task<string> AnalyzeColumnDistribution(string database, string tableName, string columnName, CancellationToken ct);
    Task<string> FindNullableColumnsWithNoNulls(string database, string tableName, CancellationToken ct);
    Task<string> FindDuplicateRows(string database, string tableName, string columns, int top, CancellationToken ct);
}
```

Each engine owns its own query validator. SQL Server keeps ScriptDom. PostgreSQL and MariaDB
must use engine-native guards in their later engine plans.

### Shared Snapshot

Shared analysis and compare operations consume normalized schema models instead of raw engine
catalog tables.

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

public interface ISchemaSnapshotCapability
{
    Task<SchemaSnapshot> GetSchemaSnapshot(string database, CancellationToken ct);
}
```

The key format for the `*ColumnKeys` sets is `"{schema}.{table}.{column}"`, using
`StringComparer.OrdinalIgnoreCase`.

### SQL Server-Specific Capabilities

SQL Server operational tools stay in SQL Server-specific contracts.

```csharp
namespace SqlSchemaMcp.Abstractions;

public interface ISqlServerDiagnosticsCapability
{
    Task<string> ListAgentJobs(string database, CancellationToken ct);
    Task<string> GetFailingJobs(string database, CancellationToken ct);
    Task<string> GetJobHistory(string database, string jobName, int maxRuns, CancellationToken ct);
    Task<string> AnalyzeTopExpensiveQueries(string database, int top, CancellationToken ct);
    Task<string> AnalyzeWaitStats(string database, CancellationToken ct);
    Task<string> ListLinkedServers(string database, CancellationToken ct);
    Task<string> FindLinkedServerUsage(string database, CancellationToken ct);
    Task<string> ListServiceBroker(string database, CancellationToken ct);
    Task<string> ListClrAssemblies(string database, CancellationToken ct);
}
```

Pipeline and security tools follow the same pattern with dedicated SQL Server capability
interfaces if they cannot be expressed as cross-engine behavior.

## Dispatcher Behavior

The existing `Data/*Queries.cs` class names stay in place because `Tools/*.cs` already depend
on them. Their implementation changes from direct SQL Server queries to capability routing.

Example dispatcher shape:

```csharp
public sealed class QueryQueries(ICapabilityResolver resolver)
{
    public Task<string> ExecuteQuery(string database, string sql, CancellationToken ct = default)
    {
        if (!resolver.TryResolve<IReadOnlyQueryCapability>(database, out var engine, out var capability))
        {
            return Task.FromResult(
                resolver.TryGetEngine(database, out engine)
                    ? Sentinels.Unsupported(nameof(ExecuteQuery), engine)
                    : Sentinels.UnknownDatabase(resolver.DatabaseNames, database));
        }

        return capability.ExecuteQuery(database, sql, ct);
    }
}
```

Each dispatcher uses the smallest capability needed for the tool it is serving.

## Unsupported User Experience

When a capability is missing, the user receives a direct message:

```text
UNSUPPORTED: Tool 'AnalyzeWaitStats' is not available for engine 'Postgres'. Ask the maintainer to add support if you need this.
```

This is enough notification for the current product. The response tells the user what failed,
which engine caused it, and what to do next. No local backlog or remote notification channel is
part of this design.

## Audit Behavior

`FileAuditLog` treats both `ERROR:` and `UNSUPPORTED:` as unsuccessful tool results.

```csharp
success = !result.StartsWith("ERROR:", StringComparison.Ordinal)
    && !result.StartsWith("UNSUPPORTED:", StringComparison.Ordinal);
```

No new audit outcome enum is introduced. The existing boolean continues to answer whether the
requested tool operation was actually performed.

## Configuration

The first implementation keeps backward compatibility with the current config shape.

Bare strings remain SQL Server:

```json
{
  "SqlServer": {
    "Databases": {
      "poc": "Server=YOUR_SERVER;Database=YOUR_DB;User Id=sqlschema_ro;Password=YOUR_SECRET;"
    }
  }
}
```

Object form enables non-SQL Server engines:

```json
{
  "SqlServer": {
    "Databases": {
      "reporting": {
        "Engine": "Postgres",
        "ConnectionString": "Host=YOUR_HOST;Database=YOUR_DB;Username=sqlschema_ro;Password=YOUR_SECRET;"
      }
    }
  }
}
```

The section name `SqlServer` is retained only for compatibility. Documentation must call this
out explicitly.

## Project Structure

Target structure:

```text
SqlSchemaMcp.Abstractions/
  DatabaseEngine.cs
  DatabaseConfig.cs
  Sentinels.cs
  Capabilities/
    ISchemaCapability.cs
    IReadOnlyQueryCapability.cs
    IDataSamplingCapability.cs
    ISchemaSnapshotCapability.cs
    ISqlServerDiagnosticsCapability.cs
  SchemaModels.cs
  ICapabilityResolver.cs

SqlSchemaMcp.SqlServer/
  SqlServerEngine.cs
  SqlServerSchema.cs
  SqlServerSchemaExtras.cs
  SqlServerQuery.cs
  SqlServerDataSampling.cs
  SqlServerSchemaSnapshot.cs
  SqlServerDiagnostics.cs
  SqlStatementValidator.cs
  SqlServerPermissionProbe.cs

SqlSchemaMcp/
  Data/
    SchemaQueries.cs
    QueryQueries.cs
    DataQueries.cs
    AnalysisQueries.cs
    CompareQueries.cs
    DiagnosticsQueries.cs
    PipelineQueries.cs
    SecurityQueries.cs
  Engines/
    CapabilityResolver.cs
  Configuration/
    DatabaseConfigLoader.cs
```

The host contains dispatchers and composition. The SQL Server project contains SQL Server
implementation details. The abstractions project contains only pure contracts and shared models.

## Migration Strategy

1. Add abstractions, sentinel helper, config loader, and capability resolver.
2. Move SQL Server primitives into `SqlSchemaMcp.SqlServer`.
3. Move SQL Server query implementations behind focused capability classes.
4. Replace host `Data/*Queries.cs` implementations with dispatchers.
5. Add shared snapshot analyzers and compare logic after SQL Server behavior is preserved.
6. Update audit and documentation for `UNSUPPORTED:`.

The first migration phase must keep SQL Server behavior stable. Shared analysis rewrites should
have focused output tests before replacing current SQL Server output paths.

## Testing Requirements

- Existing 35 tests remain green.
- `Tools/*.cs` must be unchanged.
- Unit tests cover `Sentinels.Unsupported`.
- Unit tests cover unknown database vs unsupported capability routing.
- Unit tests cover `FileAuditLog` marking `UNSUPPORTED:` as failure.
- Unit tests cover config loader support for bare string and object forms.
- Shared analyzer output tests are required before replacing current SQL Server analysis output.

## Non-Goals

- No remote notify mechanism.
- No automatic GitHub issue creation.
- No user identity or per-user unsupported tracking.
- No full authentication redesign for HTTP mode.
- No query execution beyond read-only guarded statements.
- No schema modification.
