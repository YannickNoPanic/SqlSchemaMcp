namespace SqlSchemaMcp.Configuration;

public sealed class AuditOptions
{
    public bool Enabled { get; init; } = true;

    /// <summary>Absolute or relative path to the JSON-lines audit file. When null, defaults to audit-log.jsonl in the project root.</summary>
    public string? Path { get; init; }
}
