using SqlSchemaMcp.Abstractions;
using SqlSchemaMcp.Abstractions.Capabilities;

namespace SqlSchemaMcp.Data;

public sealed class DataQueries(ICapabilityResolver resolver)
{
    public Task<string> SampleTableData(
        string database,
        string tableName,
        int rows,
        CancellationToken cancellationToken = default) =>
        Resolve(database, nameof(SampleTableData), capability => capability.SampleTableData(database, tableName, rows, cancellationToken));

    public Task<string> AnalyzeColumnDistribution(
        string database,
        string tableName,
        string columnName,
        CancellationToken cancellationToken = default) =>
        Resolve(database, nameof(AnalyzeColumnDistribution), capability => capability.AnalyzeColumnDistribution(database, tableName, columnName, cancellationToken));

    public Task<string> FindNullableColumnsWithNoNulls(
        string database,
        string tableName,
        CancellationToken cancellationToken = default) =>
        Resolve(database, nameof(FindNullableColumnsWithNoNulls), capability => capability.FindNullableColumnsWithNoNulls(database, tableName, cancellationToken));

    public Task<string> FindDuplicateRows(
        string database,
        string tableName,
        string columns,
        int top,
        CancellationToken cancellationToken = default) =>
        Resolve(database, nameof(FindDuplicateRows), capability => capability.FindDuplicateRows(database, tableName, columns, top, cancellationToken));

    private Task<string> Resolve(
        string database,
        string toolName,
        Func<IDataSamplingCapability, Task<string>> execute)
    {
        if (resolver.TryResolve<IDataSamplingCapability>(database, out _, out var capability) && capability is not null)
            return execute(capability);

        return Task.FromResult(
            resolver.TryGetEngine(database, out var engine)
                ? Sentinels.Unsupported(toolName, engine, nameof(IDataSamplingCapability))
                : Sentinels.UnknownDatabase(resolver.DatabaseNames, database));
    }
}
