namespace SqlSchemaMcp.Abstractions;

public enum ColumnTypeCategory
{
    Integer,
    Guid,
    Text,
    Boolean,
    Temporal,
    Decimal,
    Other
}

public sealed record SchemaObject(string Type, string Schema, string Name);

public sealed record SchemaColumn(
    string Schema,
    string Table,
    string Column,
    ColumnTypeCategory TypeCategory,
    string FormattedType,
    string Nullable);

public sealed record SchemaSnapshot(
    IReadOnlyList<SchemaObject> Objects,
    IReadOnlyList<SchemaColumn> Columns,
    IReadOnlySet<string> ForeignKeyColumnKeys,
    IReadOnlySet<string> PrimaryKeyColumnKeys,
    IReadOnlySet<string> IndexedColumnKeys);
