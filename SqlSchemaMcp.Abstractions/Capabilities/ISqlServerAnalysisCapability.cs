namespace SqlSchemaMcp.Abstractions.Capabilities;

public interface ISqlServerAnalysisCapability
{
    Task<string> AnalyzeProcComplexity(string database, string? nameFilter, CancellationToken ct);
    Task<string> AnalyzeViewComplexity(string database, string? nameFilter, CancellationToken ct);
    Task<string> AnalyzeDuplicateIndexes(string database, CancellationToken ct);
    Task<string> FindUnusedTables(string database, CancellationToken ct);
    Task<string> FindUnusedProcedures(string database, CancellationToken ct);
    Task<string> AnalyzeIndexFragmentation(string database, string? nameFilter, CancellationToken ct);
    Task<string> AnalyzeTriggers(string database, CancellationToken ct);
    Task<string> AnalyzeIdentityColumns(string database, CancellationToken ct);
    Task<string> AnalyzeTableSizes(string database, CancellationToken ct);
    Task<string> AnalyzeMissingIndexSuggestions(string database, CancellationToken ct);
    Task<string> GetRecentObjectChanges(string database, int days, CancellationToken ct);
    Task<string> AnalyzeTableQueryStats(string database, CancellationToken ct);
    Task<string> AnalyzeTableAccessStats(string database, CancellationToken ct);
    Task<string> GenerateDatabaseSummary(string database, CancellationToken ct);
}
