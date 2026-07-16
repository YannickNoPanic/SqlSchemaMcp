using SqlSchemaMcp.Abstractions;
using SqlSchemaMcp.Abstractions.Capabilities;
using SqlSchemaMcp.SqlServer.Data;

namespace SqlSchemaMcp.SqlServer;

public sealed class SqlServerEngine(
    SqlServerQuery query,
    SqlServerSchema schema,
    SqlServerSchemaExtras schemaExtras)
    : IDatabaseEngine,
      IReadOnlyQueryCapability,
      ISchemaCapability,
      ISqlServerSchemaExtrasCapability
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
}
