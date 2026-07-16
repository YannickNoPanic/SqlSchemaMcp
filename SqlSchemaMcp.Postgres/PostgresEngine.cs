using SqlSchemaMcp.Abstractions;
using SqlSchemaMcp.Abstractions.Capabilities;
using SqlSchemaMcp.Postgres.Data;

namespace SqlSchemaMcp.Postgres;

public sealed class PostgresEngine(PostgresSchema schema, PostgresSchemaSnapshot schemaSnapshot)
    : IDatabaseEngine, ISchemaCapability, ISchemaSnapshotCapability
{
    public DatabaseEngine Kind => DatabaseEngine.Postgres;

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

    public Task<SchemaSnapshot> GetSchemaSnapshot(string database, CancellationToken ct) =>
        schemaSnapshot.GetSchemaSnapshot(database, ct);
}
