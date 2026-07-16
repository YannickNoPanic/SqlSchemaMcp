using System.Text.RegularExpressions;

namespace SqlSchemaMcp.Data.SharedAnalysis;

internal static partial class AnalysisFilters
{
    public static bool IsStagingTable(string tableName) => StagingRegex().IsMatch(tableName);

    [GeneratedRegex(@"_\d{8}_\d{6}$")]
    private static partial Regex StagingRegex();
}
