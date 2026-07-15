namespace SqlSchemaMcp.Abstractions.Capabilities;

public interface ISqlServerSchemaExtrasCapability
{
    Task<string> ListTriggers(string database, string? nameFilter, CancellationToken ct);
    Task<string> GetTriggerDefinition(string database, string triggerName, CancellationToken ct);
    Task<string> ListSynonyms(string database, string? nameFilter, CancellationToken ct);
    Task<string> ListCheckConstraints(string database, string? nameFilter, CancellationToken ct);
    Task<string> ListDdlTriggers(string database, CancellationToken ct);
    Task<string> GetDdlTriggerDefinition(string database, string triggerName, CancellationToken ct);
}
