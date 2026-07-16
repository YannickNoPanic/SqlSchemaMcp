using System.Text;
using SqlSchemaMcp.Abstractions;

namespace SqlSchemaMcp.Data.SharedAnalysis;

public static class MissingIndexAnalyzer
{
    private static readonly HashSet<string> CommonFilterColumns = new(StringComparer.OrdinalIgnoreCase)
    {
        "IsActive",
        "IsDeleted",
        "Status",
        "CreatedAt",
        "DeletedAt",
        "TenantId",
        "OrganisationId",
        "OrganizationId",
        "AccountId"
    };

    public static string Build(string database, SchemaSnapshot snapshot)
    {
        var sb = new StringBuilder();
        sb.AppendLine($"MISSING INDEX ANALYSIS: [{database}]");
        sb.AppendLine(new string('─', 80));
        sb.AppendLine("FK-pattern and common filter columns with no index:");
        sb.AppendLine();

        int count = 0;
        foreach (var column in snapshot.Columns.Where(IsCandidate))
        {
            string key = $"{column.Schema}.{column.Table}.{column.Column}";
            if (snapshot.IndexedColumnKeys.Contains(key))
                continue;

            count++;
            sb.AppendLine($"  [{column.Schema}].[{column.Table}].{column.Column}");
        }

        if (count == 0)
            sb.AppendLine("  (none found — all candidate columns are indexed)");

        sb.AppendLine();
        sb.AppendLine($"  {count} potentially unindexed column(s)");

        return sb.ToString();
    }

    private static bool IsCandidate(SchemaColumn column) =>
        column.Column.EndsWith("Id", StringComparison.Ordinal)
        || column.Column.EndsWith("ID", StringComparison.Ordinal)
        || column.Column.EndsWith("_id", StringComparison.OrdinalIgnoreCase)
        || CommonFilterColumns.Contains(column.Column);
}
