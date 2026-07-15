namespace SqlSchemaMcp.Abstractions.Capabilities;

public interface IDatabaseEngine
{
    DatabaseEngine Kind { get; }
}
