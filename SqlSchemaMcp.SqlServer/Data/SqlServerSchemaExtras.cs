using SqlSchemaMcp.Abstractions.Capabilities;

namespace SqlSchemaMcp.SqlServer.Data;

public sealed class SqlServerSchemaExtras(SqlServerSchema schema) : ISqlServerSchemaExtrasCapability
{
    public Task<string> ListTriggers(string database, string? nameFilter, CancellationToken ct) =>
        schema.ListTriggers(database, nameFilter, ct);

    public Task<string> GetTriggerDefinition(string database, string triggerName, CancellationToken ct) =>
        schema.GetTriggerDefinition(database, triggerName, ct);

    public Task<string> ListSynonyms(string database, string? nameFilter, CancellationToken ct) =>
        schema.ListSynonyms(database, nameFilter, ct);

    public Task<string> ListCheckConstraints(string database, string? nameFilter, CancellationToken ct) =>
        schema.ListCheckConstraints(database, nameFilter, ct);

    public Task<string> ListDdlTriggers(string database, CancellationToken ct) =>
        schema.ListDdlTriggers(database, ct);

    public Task<string> GetDdlTriggerDefinition(string database, string triggerName, CancellationToken ct) =>
        schema.GetDdlTriggerDefinition(database, triggerName, ct);
}
