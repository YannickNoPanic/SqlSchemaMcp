namespace SqlSchemaMcp.Abstractions.Capabilities;

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
