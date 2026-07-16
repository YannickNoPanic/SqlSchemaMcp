using System.Globalization;
using System.Text;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SqlSchemaMcp.Abstractions;
using SqlSchemaMcp.Abstractions.Capabilities;
using SqlSchemaMcp.Data.SharedAnalysis;
using SqlSchemaMcp.SqlServer.Configuration;
using SqlSchemaMcp.SqlServer.Data;

namespace SqlSchemaMcp.Data;

public sealed class AnalysisQueries(
    IOptions<SqlServerEngineOptions> options,
    ILogger<AnalysisQueries> logger,
    ICapabilityResolver resolver)
    : SqlQueryBase(options, logger)
{
    public async Task<string> AnalyzeNamingConventions(
        string database,
        CancellationToken cancellationToken = default)
    {
        return await ResolveSchemaSnapshot(database, nameof(AnalyzeNamingConventions), async capability =>
            NamingAnalyzer.Build(database, await capability.GetSchemaSnapshot(database, cancellationToken)));
    }

    public async Task<string> AnalyzeMissingForeignKeys(
        string database,
        CancellationToken cancellationToken = default)
    {
        return await ResolveSchemaSnapshot(database, nameof(AnalyzeMissingForeignKeys), async capability =>
            MissingForeignKeyAnalyzer.Build(database, await capability.GetSchemaSnapshot(database, cancellationToken)));
    }

    public async Task<string> AnalyzeMissingIndexes(
        string database,
        CancellationToken cancellationToken = default)
    {
        return await ResolveSchemaSnapshot(database, nameof(AnalyzeMissingIndexes), async capability =>
            MissingIndexAnalyzer.Build(database, await capability.GetSchemaSnapshot(database, cancellationToken)));
    }

    private async Task<string> ResolveSchemaSnapshot(
        string database,
        string toolName,
        Func<ISchemaSnapshotCapability, Task<string>> execute)
    {
        if (resolver.TryResolve<ISchemaSnapshotCapability>(database, out _, out var capability) && capability is not null)
        {
            try
            {
                return await execute(capability);
            }
            catch (Exception ex)
            {
                return SafeError(ex, toolName);
            }
        }

        return
            resolver.TryGetEngine(database, out var engine)
                ? Sentinels.Unsupported(toolName, engine)
                : Sentinels.UnknownDatabase(resolver.DatabaseNames, database);
    }
    public Task<string> AnalyzeDuplicateIndexes(
        string database,
        CancellationToken cancellationToken = default) =>
        ResolveSqlServerAnalysis(database, nameof(AnalyzeDuplicateIndexes), capability => capability.AnalyzeDuplicateIndexes(database, cancellationToken));

    public Task<string> FindUnusedTables(
        string database,
        CancellationToken cancellationToken = default) =>
        ResolveSqlServerAnalysis(database, nameof(FindUnusedTables), capability => capability.FindUnusedTables(database, cancellationToken));

    public Task<string> FindUnusedProcedures(
        string database,
        CancellationToken cancellationToken = default) =>
        ResolveSqlServerAnalysis(database, nameof(FindUnusedProcedures), capability => capability.FindUnusedProcedures(database, cancellationToken));

    public Task<string> AnalyzeProcComplexity(
        string database,
        string? nameFilter = null,
        CancellationToken cancellationToken = default) =>
        ResolveSqlServerAnalysis(database, nameof(AnalyzeProcComplexity), capability => capability.AnalyzeProcComplexity(database, nameFilter, cancellationToken));

    public Task<string> AnalyzeViewComplexity(
        string database,
        string? nameFilter = null,
        CancellationToken cancellationToken = default) =>
        ResolveSqlServerAnalysis(database, nameof(AnalyzeViewComplexity), capability => capability.AnalyzeViewComplexity(database, nameFilter, cancellationToken));

    public Task<string> AnalyzeIndexFragmentation(
        string database,
        string? nameFilter = null,
        CancellationToken cancellationToken = default) =>
        ResolveSqlServerAnalysis(database, nameof(AnalyzeIndexFragmentation), capability => capability.AnalyzeIndexFragmentation(database, nameFilter, cancellationToken));

    public Task<string> AnalyzeTriggers(
        string database,
        CancellationToken cancellationToken = default) =>
        ResolveSqlServerAnalysis(database, nameof(AnalyzeTriggers), capability => capability.AnalyzeTriggers(database, cancellationToken));

    public Task<string> AnalyzeIdentityColumns(
        string database,
        CancellationToken cancellationToken = default) =>
        ResolveSqlServerAnalysis(database, nameof(AnalyzeIdentityColumns), capability => capability.AnalyzeIdentityColumns(database, cancellationToken));

    public Task<string> AnalyzeTableSizes(
        string database,
        CancellationToken cancellationToken = default) =>
        ResolveSqlServerAnalysis(database, nameof(AnalyzeTableSizes), capability => capability.AnalyzeTableSizes(database, cancellationToken));

    public Task<string> AnalyzeMissingIndexSuggestions(
        string database,
        CancellationToken cancellationToken = default) =>
        ResolveSqlServerAnalysis(database, nameof(AnalyzeMissingIndexSuggestions), capability => capability.AnalyzeMissingIndexSuggestions(database, cancellationToken));

    public Task<string> GetRecentObjectChanges(
        string database,
        int days,
        CancellationToken cancellationToken = default) =>
        ResolveSqlServerAnalysis(database, nameof(GetRecentObjectChanges), capability => capability.GetRecentObjectChanges(database, days, cancellationToken));

    public Task<string> AnalyzeTableQueryStats(
        string database,
        CancellationToken cancellationToken = default) =>
        ResolveSqlServerAnalysis(database, nameof(AnalyzeTableQueryStats), capability => capability.AnalyzeTableQueryStats(database, cancellationToken));

    public Task<string> AnalyzeTableAccessStats(
        string database,
        CancellationToken cancellationToken = default) =>
        ResolveSqlServerAnalysis(database, nameof(AnalyzeTableAccessStats), capability => capability.AnalyzeTableAccessStats(database, cancellationToken));

    public Task<string> GenerateDatabaseSummary(
        string database,
        CancellationToken cancellationToken = default) =>
        ResolveSqlServerAnalysis(database, nameof(GenerateDatabaseSummary), capability => capability.GenerateDatabaseSummary(database, cancellationToken));

    private Task<string> ResolveSqlServerAnalysis(
        string database,
        string toolName,
        Func<ISqlServerAnalysisCapability, Task<string>> execute)
    {
        if (resolver.TryResolve<ISqlServerAnalysisCapability>(database, out _, out var capability) && capability is not null)
            return execute(capability);

        return Task.FromResult(
            resolver.TryGetEngine(database, out var engine)
                ? Sentinels.Unsupported(toolName, engine)
                : Sentinels.UnknownDatabase(resolver.DatabaseNames, database));
    }
}
