namespace SqlSchemaMcp.Abstractions.Capabilities;

public interface IDataSamplingCapability
{
    Task<string> SampleTableData(string database, string tableName, int rows, CancellationToken ct);
    Task<string> AnalyzeColumnDistribution(string database, string tableName, string columnName, CancellationToken ct);
    Task<string> FindNullableColumnsWithNoNulls(string database, string tableName, CancellationToken ct);
    Task<string> FindDuplicateRows(string database, string tableName, string columns, int top, CancellationToken ct);
}
