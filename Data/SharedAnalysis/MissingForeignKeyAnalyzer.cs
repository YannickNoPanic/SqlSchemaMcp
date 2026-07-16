using System.Text;
using SqlSchemaMcp.Abstractions;

namespace SqlSchemaMcp.Data.SharedAnalysis;

public static class MissingForeignKeyAnalyzer
{
    public static string Build(string database, SchemaSnapshot snapshot)
    {
        var sb = new StringBuilder();
        sb.AppendLine($"MISSING FOREIGN KEY ANALYSIS: [{database}]");
        sb.AppendLine(new string('─', 80));
        sb.AppendLine("Columns matching FK name patterns with no FK constraint defined:");
        sb.AppendLine();

        int count = 0;
        foreach (var column in snapshot.Columns.Where(IsForeignKeyCandidate))
        {
            string key = $"{column.Schema}.{column.Table}.{column.Column}";
            if (snapshot.ForeignKeyColumnKeys.Contains(key))
                continue;

            if (snapshot.PrimaryKeyColumnKeys.Contains(key))
                continue;

            count++;
            sb.AppendLine($"  [{column.Schema}].[{column.Table}].{column.Column} ({column.FormattedType})");
        }

        if (count == 0)
            sb.AppendLine("  (none found — all FK-pattern columns have constraints)");

        sb.AppendLine();
        sb.AppendLine($"  {count} potential missing FK(s)");

        return sb.ToString();
    }

    private static bool IsForeignKeyCandidate(SchemaColumn column)
    {
        if (column.TypeCategory is not (ColumnTypeCategory.Integer or ColumnTypeCategory.Guid))
            return false;

        return column.Column.EndsWith("Id", StringComparison.Ordinal)
            || column.Column.EndsWith("ID", StringComparison.Ordinal)
            || column.Column.EndsWith("_id", StringComparison.OrdinalIgnoreCase);
    }
}
