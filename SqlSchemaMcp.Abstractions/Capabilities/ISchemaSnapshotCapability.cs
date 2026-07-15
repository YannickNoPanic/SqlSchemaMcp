namespace SqlSchemaMcp.Abstractions.Capabilities;

public interface ISchemaSnapshotCapability
{
    Task<SchemaSnapshot> GetSchemaSnapshot(string database, CancellationToken ct);
}
