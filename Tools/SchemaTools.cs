using System.ComponentModel;
using ModelContextProtocol.Server;
using SqlSchemaMcp.Data;

namespace SqlSchemaMcp.Tools;

[McpServerToolType]
public sealed class SchemaTools(SchemaQueries queries)
{
    [McpServerTool, Description("List all tables in the specified database with approximate row counts and descriptions.")]
    public async Task<string> ListTables(
        [Description("Name of the configured database (e.g. 'poc' or 'azure')")] string database,
        [Description("Filter by schema name (exact match, e.g. 'dbo')")] string? schemaFilter = null,
        [Description("Filter by table name (partial match)")] string? nameFilter = null,
        CancellationToken cancellationToken = default) =>
        await queries.ListTables(database, schemaFilter, nameFilter, cancellationToken);

    [McpServerTool, Description("List all views in the specified database.")]
    public async Task<string> ListViews(
        [Description("Name of the configured database")] string database,
        [Description("Filter by view name (partial match)")] string? nameFilter = null,
        CancellationToken cancellationToken = default) =>
        await queries.ListViews(database, nameFilter, cancellationToken);

    [McpServerTool, Description("List all stored procedures in the specified database with last modified date.")]
    public async Task<string> ListProcedures(
        [Description("Name of the configured database")] string database,
        [Description("Filter by procedure name (partial match)")] string? nameFilter = null,
        CancellationToken cancellationToken = default) =>
        await queries.ListProcedures(database, nameFilter, cancellationToken);

    [McpServerTool, Description("Get full column schema, foreign keys, and indexes for a table.")]
    public async Task<string> GetTableSchema(
        [Description("Name of the configured database")] string database,
        [Description("Table name, optionally schema-qualified (e.g. 'Organisations' or 'dbo.Organisations')")] string tableName,
        CancellationToken cancellationToken = default) =>
        await queries.GetTableSchema(database, tableName, cancellationToken);

    [McpServerTool, Description("Get the full T-SQL definition of a view.")]
    public async Task<string> GetViewDefinition(
        [Description("Name of the configured database")] string database,
        [Description("View name")] string viewName,
        CancellationToken cancellationToken = default) =>
        await queries.GetViewDefinition(database, viewName, cancellationToken);

    [McpServerTool, Description("Get the full T-SQL body of a stored procedure.")]
    public async Task<string> GetProcedureDefinition(
        [Description("Name of the configured database")] string database,
        [Description("Stored procedure name")] string procName,
        CancellationToken cancellationToken = default) =>
        await queries.GetProcedureDefinition(database, procName, cancellationToken);

    [McpServerTool, Description("Find all stored procedures and views that reference the given object.")]
    public async Task<string> FindReferences(
        [Description("Name of the configured database")] string database,
        [Description("Object name to search for references to (e.g. 'Organisations' or 'dbo.Organisations')")] string objectName,
        CancellationToken cancellationToken = default) =>
        await queries.FindReferences(database, objectName, cancellationToken);

    [McpServerTool, Description("Search for a keyword across all stored procedure and view definitions.")]
    public async Task<string> SearchDefinitions(
        [Description("Name of the configured database")] string database,
        [Description("Keyword or fragment to search for in procedure and view bodies")] string keyword,
        CancellationToken cancellationToken = default) =>
        await queries.SearchDefinitions(database, keyword, cancellationToken);

    [McpServerTool, Description("List all user-defined functions (scalar, inline table-valued, multi-statement table-valued) in the specified database.")]
    public async Task<string> ListFunctions(
        [Description("Name of the configured database")] string database,
        [Description("Filter by function name (partial match)")] string? nameFilter = null,
        CancellationToken cancellationToken = default) =>
        await queries.ListFunctions(database, nameFilter, cancellationToken);

    [McpServerTool, Description("Get the full T-SQL body of a user-defined function.")]
    public async Task<string> GetFunctionDefinition(
        [Description("Name of the configured database")] string database,
        [Description("Function name")] string functionName,
        CancellationToken cancellationToken = default) =>
        await queries.GetFunctionDefinition(database, functionName, cancellationToken);

    [McpServerTool, Description("List all DML triggers in the specified database with their parent table, events (INSERT/UPDATE/DELETE), and enabled status.")]
    public async Task<string> ListTriggers(
        [Description("Name of the configured database")] string database,
        [Description("Filter by trigger name (partial match)")] string? nameFilter = null,
        CancellationToken cancellationToken = default) =>
        await queries.ListTriggers(database, nameFilter, cancellationToken);

    [McpServerTool, Description("Get the full T-SQL body of a trigger.")]
    public async Task<string> GetTriggerDefinition(
        [Description("Name of the configured database")] string database,
        [Description("Trigger name")] string triggerName,
        CancellationToken cancellationToken = default) =>
        await queries.GetTriggerDefinition(database, triggerName, cancellationToken);

    [McpServerTool, Description("List all synonyms in the specified database with their target object names.")]
    public async Task<string> ListSynonyms(
        [Description("Name of the configured database")] string database,
        [Description("Filter by synonym name (partial match)")] string? nameFilter = null,
        CancellationToken cancellationToken = default) =>
        await queries.ListSynonyms(database, nameFilter, cancellationToken);

    [McpServerTool, Description("List all CHECK constraints defined on tables, showing the table, column (if column-level), and constraint expression.")]
    public async Task<string> ListCheckConstraints(
        [Description("Name of the configured database")] string database,
        [Description("Filter by table name (partial match)")] string? nameFilter = null,
        CancellationToken cancellationToken = default) =>
        await queries.ListCheckConstraints(database, nameFilter, cancellationToken);

    [McpServerTool, Description("List all database-level DDL triggers (fire on CREATE/ALTER/DROP statements) with their enabled status and events.")]
    public async Task<string> ListDdlTriggers(
        [Description("Name of the configured database")] string database,
        CancellationToken cancellationToken = default) =>
        await queries.ListDdlTriggers(database, cancellationToken);

    [McpServerTool, Description("Get the full T-SQL body of a database-level DDL trigger.")]
    public async Task<string> GetDdlTriggerDefinition(
        [Description("Name of the configured database")] string database,
        [Description("DDL trigger name")] string triggerName,
        CancellationToken cancellationToken = default) =>
        await queries.GetDdlTriggerDefinition(database, triggerName, cancellationToken);
}
