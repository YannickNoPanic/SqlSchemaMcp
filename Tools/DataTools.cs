using System.ComponentModel;
using ModelContextProtocol.Server;
using SqlSchemaMcp.Data;

namespace SqlSchemaMcp.Tools;

[McpServerToolType]
public sealed class DataTools(DataQueries queries)
{
    [McpServerTool, Description("Return a small sample of rows from a table (max 100 rows). Useful for validating that declared column types match actual data content.")]
    public async Task<string> SampleTableData(
        [Description("Name of the configured database")] string database,
        [Description("Table name, optionally schema-qualified (e.g. 'Organisations' or 'dbo.Organisations')")] string tableName,
        [Description("Number of rows to return (default 5, max 100)")] int rows = 5,
        CancellationToken cancellationToken = default) =>
        await queries.SampleTableData(database, tableName, rows, cancellationToken);

    [McpServerTool, Description("Show distribution statistics for a single column: total rows, null count, distinct count, min/max values. For text columns also shows max and average actual length vs declared length.")]
    public async Task<string> AnalyzeColumnDistribution(
        [Description("Name of the configured database")] string database,
        [Description("Table name, optionally schema-qualified")] string tableName,
        [Description("Column name to analyse")] string columnName,
        CancellationToken cancellationToken = default) =>
        await queries.AnalyzeColumnDistribution(database, tableName, columnName, cancellationToken);

    [McpServerTool, Description("Find nullable columns in a table that contain zero NULL values in practice — candidates for NOT NULL constraints.")]
    public async Task<string> FindNullableColumnsWithNoNulls(
        [Description("Name of the configured database")] string database,
        [Description("Table name, optionally schema-qualified")] string tableName,
        CancellationToken cancellationToken = default) =>
        await queries.FindNullableColumnsWithNoNulls(database, tableName, cancellationToken);

    [McpServerTool, Description("Find rows with duplicate values across the specified columns. Useful for validating whether a UNIQUE constraint is safe to add.")]
    public async Task<string> FindDuplicateRows(
        [Description("Name of the configured database")] string database,
        [Description("Table name, optionally schema-qualified")] string tableName,
        [Description("Comma-separated list of column names to group by (e.g. 'Email' or 'TenantId,Code')")] string columns,
        [Description("Maximum number of duplicate groups to return (default 20)")] int top = 20,
        CancellationToken cancellationToken = default) =>
        await queries.FindDuplicateRows(database, tableName, columns, top, cancellationToken);
}
