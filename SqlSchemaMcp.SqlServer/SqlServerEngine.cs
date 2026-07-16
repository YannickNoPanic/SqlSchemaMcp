using SqlSchemaMcp.Abstractions;
using SqlSchemaMcp.Abstractions.Capabilities;
using SqlSchemaMcp.SqlServer.Data;

namespace SqlSchemaMcp.SqlServer;

public sealed class SqlServerEngine(
    SqlServerQuery query,
    SqlServerSchema schema,
    SqlServerSchemaExtras schemaExtras,
    SqlServerDataSampling dataSampling,
    SqlServerDiagnostics diagnostics,
    SqlServerPipeline pipeline,
    SqlServerSecurity security,
    SqlServerAnalysis analysis)
    : IDatabaseEngine,
      IReadOnlyQueryCapability,
      ISchemaCapability,
      ISqlServerSchemaExtrasCapability,
      IDataSamplingCapability,
      ISqlServerDiagnosticsCapability,
      ISqlServerPipelineCapability,
      ISqlServerSecurityCapability,
      ISqlServerAnalysisCapability
{
    public DatabaseEngine Kind => DatabaseEngine.SqlServer;

    public Task<string> ExecuteQuery(string database, string sql, CancellationToken ct) =>
        query.ExecuteQuery(database, sql, ct);

    public Task<string> ListTables(string database, string? schemaFilter, string? nameFilter, CancellationToken ct) =>
        schema.ListTables(database, schemaFilter, nameFilter, ct);

    public Task<string> ListViews(string database, string? nameFilter, CancellationToken ct) =>
        schema.ListViews(database, nameFilter, ct);

    public Task<string> ListProcedures(string database, string? nameFilter, CancellationToken ct) =>
        schema.ListProcedures(database, nameFilter, ct);

    public Task<string> ListFunctions(string database, string? nameFilter, CancellationToken ct) =>
        schema.ListFunctions(database, nameFilter, ct);

    public Task<string> GetTableSchema(string database, string tableName, CancellationToken ct) =>
        schema.GetTableSchema(database, tableName, ct);

    public Task<string> GetViewDefinition(string database, string viewName, CancellationToken ct) =>
        schema.GetViewDefinition(database, viewName, ct);

    public Task<string> GetProcedureDefinition(string database, string procName, CancellationToken ct) =>
        schema.GetProcedureDefinition(database, procName, ct);

    public Task<string> GetFunctionDefinition(string database, string functionName, CancellationToken ct) =>
        schema.GetFunctionDefinition(database, functionName, ct);

    public Task<string> FindReferences(string database, string objectName, CancellationToken ct) =>
        schema.FindReferences(database, objectName, ct);

    public Task<string> SearchDefinitions(string database, string keyword, CancellationToken ct) =>
        schema.SearchDefinitions(database, keyword, ct);

    public Task<string> ListTriggers(string database, string? nameFilter, CancellationToken ct) =>
        schemaExtras.ListTriggers(database, nameFilter, ct);

    public Task<string> GetTriggerDefinition(string database, string triggerName, CancellationToken ct) =>
        schemaExtras.GetTriggerDefinition(database, triggerName, ct);

    public Task<string> ListSynonyms(string database, string? nameFilter, CancellationToken ct) =>
        schemaExtras.ListSynonyms(database, nameFilter, ct);

    public Task<string> ListCheckConstraints(string database, string? nameFilter, CancellationToken ct) =>
        schemaExtras.ListCheckConstraints(database, nameFilter, ct);

    public Task<string> ListDdlTriggers(string database, CancellationToken ct) =>
        schemaExtras.ListDdlTriggers(database, ct);

    public Task<string> GetDdlTriggerDefinition(string database, string triggerName, CancellationToken ct) =>
        schemaExtras.GetDdlTriggerDefinition(database, triggerName, ct);

    public Task<string> SampleTableData(string database, string tableName, int rows, CancellationToken ct) =>
        dataSampling.SampleTableData(database, tableName, rows, ct);

    public Task<string> AnalyzeColumnDistribution(string database, string tableName, string columnName, CancellationToken ct) =>
        dataSampling.AnalyzeColumnDistribution(database, tableName, columnName, ct);

    public Task<string> FindNullableColumnsWithNoNulls(string database, string tableName, CancellationToken ct) =>
        dataSampling.FindNullableColumnsWithNoNulls(database, tableName, ct);

    public Task<string> FindDuplicateRows(string database, string tableName, string columns, int top, CancellationToken ct) =>
        dataSampling.FindDuplicateRows(database, tableName, columns, top, ct);

    public Task<string> ListAgentJobs(string database, CancellationToken ct) =>
        diagnostics.ListAgentJobs(database, ct);

    public Task<string> GetFailingJobs(string database, CancellationToken ct) =>
        diagnostics.GetFailingJobs(database, ct);

    public Task<string> GetJobHistory(string database, string jobName, int maxRuns, CancellationToken ct) =>
        diagnostics.GetJobHistory(database, jobName, maxRuns, ct);

    public Task<string> AnalyzeTopExpensiveQueries(string database, int top, CancellationToken ct) =>
        diagnostics.AnalyzeTopExpensiveQueries(database, top, ct);

    public Task<string> AnalyzeWaitStats(string database, CancellationToken ct) =>
        diagnostics.AnalyzeWaitStats(database, ct);

    public Task<string> ListLinkedServers(string database, CancellationToken ct) =>
        diagnostics.ListLinkedServers(database, ct);

    public Task<string> FindLinkedServerUsage(string database, string? linkedServerName, CancellationToken ct) =>
        diagnostics.FindLinkedServerUsage(database, linkedServerName, ct);

    public Task<string> ListServiceBroker(string database, CancellationToken ct) =>
        diagnostics.ListServiceBroker(database, ct);

    public Task<string> ListClrAssemblies(string database, CancellationToken ct) =>
        diagnostics.ListClrAssemblies(database, ct);

    public Task<string> ListDataFeeds(string database, CancellationToken ct) =>
        pipeline.ListDataFeeds(database, ct);

    public Task<string> AnalyzeStagingHealth(string database, CancellationToken ct) =>
        pipeline.AnalyzeStagingHealth(database, ct);

    public Task<string> CompareStagingToCurrentSchema(string database, string feedBaseName, string currentTableName, CancellationToken ct) =>
        pipeline.CompareStagingToCurrentSchema(database, feedBaseName, currentTableName, ct);

    public Task<string> ListDatabaseUsers(string database, CancellationToken ct) =>
        security.ListDatabaseUsers(database, ct);

    public Task<string> ListObjectPermissions(string database, CancellationToken ct) =>
        security.ListObjectPermissions(database, ct);

    public Task<string> AnalyzeProcComplexity(string database, string? nameFilter, CancellationToken ct) =>
        analysis.AnalyzeProcComplexity(database, nameFilter, ct);

    public Task<string> AnalyzeViewComplexity(string database, string? nameFilter, CancellationToken ct) =>
        analysis.AnalyzeViewComplexity(database, nameFilter, ct);

    public Task<string> AnalyzeDuplicateIndexes(string database, CancellationToken ct) =>
        analysis.AnalyzeDuplicateIndexes(database, ct);

    public Task<string> FindUnusedTables(string database, CancellationToken ct) =>
        analysis.FindUnusedTables(database, ct);

    public Task<string> FindUnusedProcedures(string database, CancellationToken ct) =>
        analysis.FindUnusedProcedures(database, ct);

    public Task<string> AnalyzeIndexFragmentation(string database, string? nameFilter, CancellationToken ct) =>
        analysis.AnalyzeIndexFragmentation(database, nameFilter, ct);

    public Task<string> AnalyzeTriggers(string database, CancellationToken ct) =>
        analysis.AnalyzeTriggers(database, ct);

    public Task<string> AnalyzeIdentityColumns(string database, CancellationToken ct) =>
        analysis.AnalyzeIdentityColumns(database, ct);

    public Task<string> AnalyzeTableSizes(string database, CancellationToken ct) =>
        analysis.AnalyzeTableSizes(database, ct);

    public Task<string> AnalyzeMissingIndexSuggestions(string database, CancellationToken ct) =>
        analysis.AnalyzeMissingIndexSuggestions(database, ct);

    public Task<string> GetRecentObjectChanges(string database, int days, CancellationToken ct) =>
        analysis.GetRecentObjectChanges(database, days, ct);

    public Task<string> AnalyzeTableQueryStats(string database, CancellationToken ct) =>
        analysis.AnalyzeTableQueryStats(database, ct);

    public Task<string> AnalyzeTableAccessStats(string database, CancellationToken ct) =>
        analysis.AnalyzeTableAccessStats(database, ct);

    public Task<string> GenerateDatabaseSummary(string database, CancellationToken ct) =>
        analysis.GenerateDatabaseSummary(database, ct);
}
