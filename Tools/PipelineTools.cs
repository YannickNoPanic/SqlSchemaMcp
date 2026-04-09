using System.ComponentModel;
using ModelContextProtocol.Server;
using SqlSchemaMcp.Data;

namespace SqlSchemaMcp.Tools;

[McpServerToolType]
public sealed class PipelineTools(PipelineQueries queries)
{
    [McpServerTool, Description("List all ETL data feeds by grouping staging tables (BaseName_YYYYMMDD_HHMMSS pattern) by base name. Shows staging count, latest and earliest run dates, and whether a matching current table exists. Excludes staging tables from all counts.")]
    public async Task<string> ListDataFeeds(
        [Description("Name of the configured database")] string database,
        CancellationToken cancellationToken = default) =>
        await queries.ListDataFeeds(database, cancellationToken);

    [McpServerTool, Description("Analyse ETL pipeline health per feed. Flags OVERDUE feeds (latest staging run >24h ago), CLEANUP LATE feeds (oldest staging table >5 days old), and EXCESS feeds (more than 6 staging tables). OK feeds are shown as a summary count only.")]
    public async Task<string> AnalyzeStagingHealth(
        [Description("Name of the configured database")] string database,
        CancellationToken cancellationToken = default) =>
        await queries.AnalyzeStagingHealth(database, cancellationToken);

    [McpServerTool, Description("Compare the column schema of the most recent staging table for a feed against its current (permanent) table. Shows columns dropped by the mk_* proc, columns added, and type mismatches. Reveals what transformation each proc applies.")]
    public async Task<string> CompareStagingToCurrentSchema(
        [Description("Name of the configured database")] string database,
        [Description("Feed base name without date suffix (e.g. MgUsers, BackupObjectsVeeam12)")] string feedBaseName,
        [Description("Name of the current permanent table to compare against (e.g. Microsoft_Users)")] string currentTableName,
        CancellationToken cancellationToken = default) =>
        await queries.CompareStagingToCurrentSchema(database, feedBaseName, currentTableName, cancellationToken);
}
