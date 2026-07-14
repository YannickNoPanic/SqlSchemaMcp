using System.ComponentModel;
using ModelContextProtocol.Server;
using SqlSchemaMcp.Auditing;
using SqlSchemaMcp.Data;

namespace SqlSchemaMcp.Tools;

[McpServerToolType]
public sealed class SchemaTools(SchemaQueries queries, IAuditLog audit)
{
    [McpServerTool, Description("List all tables in the specified database with approximate row counts and descriptions.")]
    public Task<string> ListTables(
        [Description("Name of the configured database (e.g. 'poc' or 'azure')")] string database,
        [Description("Filter by schema name (exact match, e.g. 'dbo')")] string? schemaFilter = null,
        [Description("Filter by table name (partial match)")] string? nameFilter = null,
        CancellationToken cancellationToken = default) =>
        audit.Invoke(nameof(ListTables), database, $"schemaFilter={AuditSummary.Truncate(schemaFilter)}; nameFilter={AuditSummary.Truncate(nameFilter)}",
            () => queries.ListTables(database, schemaFilter, nameFilter, cancellationToken));

    [McpServerTool, Description("List all views in the specified database.")]
    public Task<string> ListViews(
        [Description("Name of the configured database")] string database,
        [Description("Filter by view name (partial match)")] string? nameFilter = null,
        CancellationToken cancellationToken = default) =>
        audit.Invoke(nameof(ListViews), database, $"nameFilter={AuditSummary.Truncate(nameFilter)}",
            () => queries.ListViews(database, nameFilter, cancellationToken));

    [McpServerTool, Description("List all stored procedures in the specified database with last modified date.")]
    public Task<string> ListProcedures(
        [Description("Name of the configured database")] string database,
        [Description("Filter by procedure name (partial match)")] string? nameFilter = null,
        CancellationToken cancellationToken = default) =>
        audit.Invoke(nameof(ListProcedures), database, $"nameFilter={AuditSummary.Truncate(nameFilter)}",
            () => queries.ListProcedures(database, nameFilter, cancellationToken));

    [McpServerTool, Description("Get full column schema, foreign keys, and indexes for a table.")]
    public Task<string> GetTableSchema(
        [Description("Name of the configured database")] string database,
        [Description("Table name, optionally schema-qualified (e.g. 'Organisations' or 'dbo.Organisations')")] string tableName,
        CancellationToken cancellationToken = default) =>
        audit.Invoke(nameof(GetTableSchema), database, $"tableName={AuditSummary.Truncate(tableName)}",
            () => queries.GetTableSchema(database, tableName, cancellationToken));

    [McpServerTool, Description("Get the full T-SQL definition of a view.")]
    public Task<string> GetViewDefinition(
        [Description("Name of the configured database")] string database,
        [Description("View name")] string viewName,
        CancellationToken cancellationToken = default) =>
        audit.Invoke(nameof(GetViewDefinition), database, $"viewName={AuditSummary.Truncate(viewName)}",
            () => queries.GetViewDefinition(database, viewName, cancellationToken));

    [McpServerTool, Description("Get the full T-SQL body of a stored procedure.")]
    public Task<string> GetProcedureDefinition(
        [Description("Name of the configured database")] string database,
        [Description("Stored procedure name")] string procName,
        CancellationToken cancellationToken = default) =>
        audit.Invoke(nameof(GetProcedureDefinition), database, $"procName={AuditSummary.Truncate(procName)}",
            () => queries.GetProcedureDefinition(database, procName, cancellationToken));

    [McpServerTool, Description("Find all stored procedures and views that reference the given object.")]
    public Task<string> FindReferences(
        [Description("Name of the configured database")] string database,
        [Description("Object name to search for references to (e.g. 'Organisations' or 'dbo.Organisations')")] string objectName,
        CancellationToken cancellationToken = default) =>
        audit.Invoke(nameof(FindReferences), database, $"objectName={AuditSummary.Truncate(objectName)}",
            () => queries.FindReferences(database, objectName, cancellationToken));

    [McpServerTool, Description("Search for a keyword across all stored procedure and view definitions.")]
    public Task<string> SearchDefinitions(
        [Description("Name of the configured database")] string database,
        [Description("Keyword or fragment to search for in procedure and view bodies")] string keyword,
        CancellationToken cancellationToken = default) =>
        audit.Invoke(nameof(SearchDefinitions), database, $"keyword={AuditSummary.Truncate(keyword)}",
            () => queries.SearchDefinitions(database, keyword, cancellationToken));

    [McpServerTool, Description("List all user-defined functions (scalar, inline table-valued, multi-statement table-valued) in the specified database.")]
    public Task<string> ListFunctions(
        [Description("Name of the configured database")] string database,
        [Description("Filter by function name (partial match)")] string? nameFilter = null,
        CancellationToken cancellationToken = default) =>
        audit.Invoke(nameof(ListFunctions), database, $"nameFilter={AuditSummary.Truncate(nameFilter)}",
            () => queries.ListFunctions(database, nameFilter, cancellationToken));

    [McpServerTool, Description("Get the full T-SQL body of a user-defined function.")]
    public Task<string> GetFunctionDefinition(
        [Description("Name of the configured database")] string database,
        [Description("Function name")] string functionName,
        CancellationToken cancellationToken = default) =>
        audit.Invoke(nameof(GetFunctionDefinition), database, $"functionName={AuditSummary.Truncate(functionName)}",
            () => queries.GetFunctionDefinition(database, functionName, cancellationToken));

    [McpServerTool, Description("List all DML triggers in the specified database with their parent table, events (INSERT/UPDATE/DELETE), and enabled status.")]
    public Task<string> ListTriggers(
        [Description("Name of the configured database")] string database,
        [Description("Filter by trigger name (partial match)")] string? nameFilter = null,
        CancellationToken cancellationToken = default) =>
        audit.Invoke(nameof(ListTriggers), database, $"nameFilter={AuditSummary.Truncate(nameFilter)}",
            () => queries.ListTriggers(database, nameFilter, cancellationToken));

    [McpServerTool, Description("Get the full T-SQL body of a trigger.")]
    public Task<string> GetTriggerDefinition(
        [Description("Name of the configured database")] string database,
        [Description("Trigger name")] string triggerName,
        CancellationToken cancellationToken = default) =>
        audit.Invoke(nameof(GetTriggerDefinition), database, $"triggerName={AuditSummary.Truncate(triggerName)}",
            () => queries.GetTriggerDefinition(database, triggerName, cancellationToken));

    [McpServerTool, Description("List all synonyms in the specified database with their target object names.")]
    public Task<string> ListSynonyms(
        [Description("Name of the configured database")] string database,
        [Description("Filter by synonym name (partial match)")] string? nameFilter = null,
        CancellationToken cancellationToken = default) =>
        audit.Invoke(nameof(ListSynonyms), database, $"nameFilter={AuditSummary.Truncate(nameFilter)}",
            () => queries.ListSynonyms(database, nameFilter, cancellationToken));

    [McpServerTool, Description("List all CHECK constraints defined on tables, showing the table, column (if column-level), and constraint expression.")]
    public Task<string> ListCheckConstraints(
        [Description("Name of the configured database")] string database,
        [Description("Filter by table name (partial match)")] string? nameFilter = null,
        CancellationToken cancellationToken = default) =>
        audit.Invoke(nameof(ListCheckConstraints), database, $"nameFilter={AuditSummary.Truncate(nameFilter)}",
            () => queries.ListCheckConstraints(database, nameFilter, cancellationToken));

    [McpServerTool, Description("List all database-level DDL triggers (fire on CREATE/ALTER/DROP statements) with their enabled status and events.")]
    public Task<string> ListDdlTriggers(
        [Description("Name of the configured database")] string database,
        CancellationToken cancellationToken = default) =>
        audit.Invoke(nameof(ListDdlTriggers), database, "",
            () => queries.ListDdlTriggers(database, cancellationToken));

    [McpServerTool, Description("Get the full T-SQL body of a database-level DDL trigger.")]
    public Task<string> GetDdlTriggerDefinition(
        [Description("Name of the configured database")] string database,
        [Description("DDL trigger name")] string triggerName,
        CancellationToken cancellationToken = default) =>
        audit.Invoke(nameof(GetDdlTriggerDefinition), database, $"triggerName={AuditSummary.Truncate(triggerName)}",
            () => queries.GetDdlTriggerDefinition(database, triggerName, cancellationToken));
}
