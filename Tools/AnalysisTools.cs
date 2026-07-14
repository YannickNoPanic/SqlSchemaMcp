using System.ComponentModel;
using ModelContextProtocol.Server;
using SqlSchemaMcp.Auditing;
using SqlSchemaMcp.Data;

namespace SqlSchemaMcp.Tools;

[McpServerToolType]
public sealed class AnalysisTools(AnalysisQueries queries, IAuditLog audit)
{
    [McpServerTool, Description("Scan all tables, columns, views, and procedures for naming convention violations: Hungarian prefixes (tbl_, sp_, vw_), version suffixes (_v2, _OLD, _FINAL), ALL_CAPS, snake_case. Results grouped by violation type.")]
    public Task<string> AnalyzeNamingConventions(
        [Description("Name of the configured database")] string database,
        CancellationToken cancellationToken = default) =>
        audit.Invoke(nameof(AnalyzeNamingConventions), database, "",
            () => queries.AnalyzeNamingConventions(database, cancellationToken));

    [McpServerTool, Description("Find columns that match FK name patterns (e.g. OrganisationId, TenantId, UserId) but have no FK constraint defined. Cross-references against actual primary keys in the same database.")]
    public Task<string> AnalyzeMissingForeignKeys(
        [Description("Name of the configured database")] string database,
        CancellationToken cancellationToken = default) =>
        audit.Invoke(nameof(AnalyzeMissingForeignKeys), database, "",
            () => queries.AnalyzeMissingForeignKeys(database, cancellationToken));

    [McpServerTool, Description("Find FK-pattern columns and common filter columns (IsActive, Status, CreatedAt, TenantId, OrganisationId) that have no index defined.")]
    public Task<string> AnalyzeMissingIndexes(
        [Description("Name of the configured database")] string database,
        CancellationToken cancellationToken = default) =>
        audit.Invoke(nameof(AnalyzeMissingIndexes), database, "",
            () => queries.AnalyzeMissingIndexes(database, cancellationToken));

    [McpServerTool, Description("Analyse stored procedure complexity: line count, cursors, temp tables, dynamic SQL, NOLOCK hints. Flags refactor candidates (>200 lines, cursors, or dynamic SQL).")]
    public Task<string> AnalyzeProcComplexity(
        [Description("Name of the configured database")] string database,
        [Description("Optional filter by procedure name (partial match)")] string? nameFilter = null,
        CancellationToken cancellationToken = default) =>
        audit.Invoke(nameof(AnalyzeProcComplexity), database, $"nameFilter={AuditSummary.Truncate(nameFilter)}",
            () => queries.AnalyzeProcComplexity(database, nameFilter, cancellationToken));

    [McpServerTool, Description("Analyse view complexity: line count and nested view references (views that reference other views). Flags complex views.")]
    public Task<string> AnalyzeViewComplexity(
        [Description("Name of the configured database")] string database,
        [Description("Optional filter by view name (partial match)")] string? nameFilter = null,
        CancellationToken cancellationToken = default) =>
        audit.Invoke(nameof(AnalyzeViewComplexity), database, $"nameFilter={AuditSummary.Truncate(nameFilter)}",
            () => queries.AnalyzeViewComplexity(database, nameFilter, cancellationToken));

    [McpServerTool, Description("Find indexes that are made redundant by a broader index on the same table (duplicate leading key columns). Reports the redundant index and the index that covers it.")]
    public Task<string> AnalyzeDuplicateIndexes(
        [Description("Name of the configured database")] string database,
        CancellationToken cancellationToken = default) =>
        audit.Invoke(nameof(AnalyzeDuplicateIndexes), database, "",
            () => queries.AnalyzeDuplicateIndexes(database, cancellationToken));

    [McpServerTool, Description("Find tables not referenced by any stored procedure or view in this database. NOTE: does not account for application-level references — treat results as candidates for investigation, not definitive unused tables.")]
    public Task<string> FindUnusedTables(
        [Description("Name of the configured database")] string database,
        CancellationToken cancellationToken = default) =>
        audit.Invoke(nameof(FindUnusedTables), database, "",
            () => queries.FindUnusedTables(database, cancellationToken));

    [McpServerTool, Description("Find stored procedures not referenced by any other SQL object in this database. NOTE: does not account for application-level calls — treat results as candidates for investigation only.")]
    public Task<string> FindUnusedProcedures(
        [Description("Name of the configured database")] string database,
        CancellationToken cancellationToken = default) =>
        audit.Invoke(nameof(FindUnusedProcedures), database, "",
            () => queries.FindUnusedProcedures(database, cancellationToken));

    [McpServerTool, Description("Report index fragmentation using sys.dm_db_index_physical_stats (LIMITED scan). Only returns indexes with >10% fragmentation and >100 pages. WARNING: may be slow on large databases.")]
    public Task<string> AnalyzeIndexFragmentation(
        [Description("Name of the configured database")] string database,
        [Description("Optional filter by table name (partial match)")] string? nameFilter = null,
        CancellationToken cancellationToken = default) =>
        audit.Invoke(nameof(AnalyzeIndexFragmentation), database, $"nameFilter={AuditSummary.Truncate(nameFilter)}",
            () => queries.AnalyzeIndexFragmentation(database, nameFilter, cancellationToken));

    [McpServerTool, Description("Analyse all triggers: flags disabled triggers, INSTEAD OF triggers, tables with multiple triggers, and AFTER triggers that fire on multiple events.")]
    public Task<string> AnalyzeTriggers(
        [Description("Name of the configured database")] string database,
        CancellationToken cancellationToken = default) =>
        audit.Invoke(nameof(AnalyzeTriggers), database, "",
            () => queries.AnalyzeTriggers(database, cancellationToken));

    [McpServerTool, Description("Find identity columns (int, bigint, smallint, tinyint) and show how close they are to their maximum value. Flags columns above 70% capacity.")]
    public Task<string> AnalyzeIdentityColumns(
        [Description("Name of the configured database")] string database,
        CancellationToken cancellationToken = default) =>
        audit.Invoke(nameof(AnalyzeIdentityColumns), database, "",
            () => queries.AnalyzeIdentityColumns(database, cancellationToken));

    [McpServerTool, Description("Show physical table sizes (data + index pages) ordered by total size. Uses sys.dm_db_partition_stats. Requires VIEW DATABASE STATE permission.")]
    public Task<string> AnalyzeTableSizes(
        [Description("Name of the configured database")] string database,
        CancellationToken cancellationToken = default) =>
        audit.Invoke(nameof(AnalyzeTableSizes), database, "",
            () => queries.AnalyzeTableSizes(database, cancellationToken));

    [McpServerTool, Description("Return SQL Server's own missing index recommendations from sys.dm_db_missing_index_details, ranked by estimated impact score. Requires VIEW DATABASE STATE permission.")]
    public Task<string> AnalyzeMissingIndexSuggestions(
        [Description("Name of the configured database")] string database,
        CancellationToken cancellationToken = default) =>
        audit.Invoke(nameof(AnalyzeMissingIndexSuggestions), database, "",
            () => queries.AnalyzeMissingIndexSuggestions(database, cancellationToken));

    [McpServerTool, Description("List all objects (tables, views, procedures, functions, triggers) modified in the last N days, ordered by most recently modified.")]
    public Task<string> GetRecentObjectChanges(
        [Description("Name of the configured database")] string database,
        [Description("Number of days to look back (default 30)")] int days = 30,
        CancellationToken cancellationToken = default) =>
        audit.Invoke(nameof(GetRecentObjectChanges), database, $"days={days}",
            () => queries.GetRecentObjectChanges(database, days, cancellationToken));

    [McpServerTool, Description("Show total read/write operation counts per table using sys.dm_db_index_usage_stats, with a per-day average based on server uptime. Useful for identifying the most and least queried tables. Only tables with recorded activity are shown; staging snapshots excluded. NOTE: counts reset on SQL Server restart.")]
    public Task<string> AnalyzeTableQueryStats(
        [Description("Name of the configured database")] string database,
        CancellationToken cancellationToken = default) =>
        audit.Invoke(nameof(AnalyzeTableQueryStats), database, "",
            () => queries.AnalyzeTableQueryStats(database, cancellationToken));

    [McpServerTool, Description("Show last read and write timestamps per table using sys.dm_db_index_usage_stats. Useful for identifying tables not accessed since the last server restart. NOTE: stats reset on SQL Server restart — NULL means no access recorded, not necessarily never used. Excludes staging snapshot tables.")]
    public Task<string> AnalyzeTableAccessStats(
        [Description("Name of the configured database")] string database,
        CancellationToken cancellationToken = default) =>
        audit.Invoke(nameof(AnalyzeTableAccessStats), database, "",
            () => queries.AnalyzeTableAccessStats(database, cancellationToken));

    [McpServerTool, Description("High-level database summary: object counts, top 10 tables by row count, database size, and health flags (heaps, tables without PKs, disabled triggers/indexes).")]
    public Task<string> GenerateDatabaseSummary(
        [Description("Name of the configured database")] string database,
        CancellationToken cancellationToken = default) =>
        audit.Invoke(nameof(GenerateDatabaseSummary), database, "",
            () => queries.GenerateDatabaseSummary(database, cancellationToken));
}
