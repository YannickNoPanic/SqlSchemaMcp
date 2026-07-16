using SqlSchemaMcp.Abstractions;
using SqlSchemaMcp.Abstractions.Capabilities;

namespace SqlSchemaMcp.Data;

public sealed class SchemaQueries(ICapabilityResolver resolver)
{
    public Task<string> ListTables(
        string database,
        string? schemaFilter,
        string? nameFilter,
        CancellationToken cancellationToken = default) =>
        ResolveSchema(database, nameof(ListTables), capability => capability.ListTables(database, schemaFilter, nameFilter, cancellationToken));

    public Task<string> ListViews(
        string database,
        string? nameFilter,
        CancellationToken cancellationToken = default) =>
        ResolveSchema(database, nameof(ListViews), capability => capability.ListViews(database, nameFilter, cancellationToken));

    public Task<string> ListProcedures(
        string database,
        string? nameFilter,
        CancellationToken cancellationToken = default) =>
        ResolveSchema(database, nameof(ListProcedures), capability => capability.ListProcedures(database, nameFilter, cancellationToken));

    public Task<string> ListFunctions(
        string database,
        string? nameFilter,
        CancellationToken cancellationToken = default) =>
        ResolveSchema(database, nameof(ListFunctions), capability => capability.ListFunctions(database, nameFilter, cancellationToken));

    public Task<string> ListTriggers(
        string database,
        string? nameFilter,
        CancellationToken cancellationToken = default) =>
        ResolveExtras(database, nameof(ListTriggers), capability => capability.ListTriggers(database, nameFilter, cancellationToken));

    public Task<string> ListSynonyms(
        string database,
        string? nameFilter,
        CancellationToken cancellationToken = default) =>
        ResolveExtras(database, nameof(ListSynonyms), capability => capability.ListSynonyms(database, nameFilter, cancellationToken));

    public Task<string> ListCheckConstraints(
        string database,
        string? nameFilter,
        CancellationToken cancellationToken = default) =>
        ResolveExtras(database, nameof(ListCheckConstraints), capability => capability.ListCheckConstraints(database, nameFilter, cancellationToken));

    public Task<string> GetTableSchema(
        string database,
        string tableName,
        CancellationToken cancellationToken = default) =>
        ResolveSchema(database, nameof(GetTableSchema), capability => capability.GetTableSchema(database, tableName, cancellationToken));

    public Task<string> GetViewDefinition(
        string database,
        string viewName,
        CancellationToken cancellationToken = default) =>
        ResolveSchema(database, nameof(GetViewDefinition), capability => capability.GetViewDefinition(database, viewName, cancellationToken));

    public Task<string> GetProcedureDefinition(
        string database,
        string procName,
        CancellationToken cancellationToken = default) =>
        ResolveSchema(database, nameof(GetProcedureDefinition), capability => capability.GetProcedureDefinition(database, procName, cancellationToken));

    public Task<string> GetFunctionDefinition(
        string database,
        string functionName,
        CancellationToken cancellationToken = default) =>
        ResolveSchema(database, nameof(GetFunctionDefinition), capability => capability.GetFunctionDefinition(database, functionName, cancellationToken));

    public Task<string> GetTriggerDefinition(
        string database,
        string triggerName,
        CancellationToken cancellationToken = default) =>
        ResolveExtras(database, nameof(GetTriggerDefinition), capability => capability.GetTriggerDefinition(database, triggerName, cancellationToken));

    public Task<string> FindReferences(
        string database,
        string objectName,
        CancellationToken cancellationToken = default) =>
        ResolveSchema(database, nameof(FindReferences), capability => capability.FindReferences(database, objectName, cancellationToken));

    public Task<string> SearchDefinitions(
        string database,
        string keyword,
        CancellationToken cancellationToken = default) =>
        ResolveSchema(database, nameof(SearchDefinitions), capability => capability.SearchDefinitions(database, keyword, cancellationToken));

    public Task<string> ListDdlTriggers(
        string database,
        CancellationToken cancellationToken = default) =>
        ResolveExtras(database, nameof(ListDdlTriggers), capability => capability.ListDdlTriggers(database, cancellationToken));

    public Task<string> GetDdlTriggerDefinition(
        string database,
        string triggerName,
        CancellationToken cancellationToken = default) =>
        ResolveExtras(database, nameof(GetDdlTriggerDefinition), capability => capability.GetDdlTriggerDefinition(database, triggerName, cancellationToken));

    private Task<string> ResolveSchema(
        string database,
        string toolName,
        Func<ISchemaCapability, Task<string>> execute)
    {
        if (resolver.TryResolve<ISchemaCapability>(database, out _, out var capability) && capability is not null)
            return execute(capability);

        return UnsupportedOrUnknown(database, toolName);
    }

    private Task<string> ResolveExtras(
        string database,
        string toolName,
        Func<ISqlServerSchemaExtrasCapability, Task<string>> execute)
    {
        if (resolver.TryResolve<ISqlServerSchemaExtrasCapability>(database, out _, out var capability) && capability is not null)
            return execute(capability);

        return UnsupportedOrUnknown(database, toolName);
    }

    private Task<string> UnsupportedOrUnknown(string database, string toolName) =>
        Task.FromResult(
            resolver.TryGetEngine(database, out var engine)
                ? Sentinels.Unsupported(toolName, engine)
                : Sentinels.UnknownDatabase(resolver.DatabaseNames, database));
}
