using System.Text;
using SqlSchemaMcp.Abstractions;

namespace SqlSchemaMcp.Data.SharedAnalysis;

public static class NamingAnalyzer
{
    public static string Build(string database, SchemaSnapshot snapshot)
    {
        var hungarian = new List<string>();
        var versionSuffix = new List<string>();
        var allCaps = new List<string>();
        var snakeCase = new List<string>();

        string[] hungarianPrefixes = ["tbl_", "sp_", "vw_", "col_", "f_", "fn_", "usp_"];
        string[] versionSuffixes = ["_v2", "_v3", "_v4", "_v5", "_final", "_old", "_backup", "_copy", "_new", "_temp", "_bak"];

        foreach (var item in snapshot.Objects)
        {
            string lower = item.Name.ToLowerInvariant();
            string label = $"  [{item.Schema}].[{item.Name}] ({item.Type})";

            if (hungarianPrefixes.Any(p => lower.StartsWith(p, StringComparison.OrdinalIgnoreCase)))
                hungarian.Add(label);

            if (versionSuffixes.Any(s => lower.EndsWith(s, StringComparison.OrdinalIgnoreCase)))
                versionSuffix.Add(label);

            if (string.Equals(item.Name, item.Name.ToUpperInvariant(), StringComparison.Ordinal) && item.Name.Length > 1)
                allCaps.Add(label);

            if (item.Name.Contains('_') && !hungarianPrefixes.Any(p => lower.StartsWith(p, StringComparison.OrdinalIgnoreCase)))
                snakeCase.Add(label);
        }

        var colHungarian = new List<string>();
        var colAllCaps = new List<string>();
        var colSnakeCase = new List<string>();

        foreach (var item in snapshot.Columns)
        {
            string lower = item.Column.ToLowerInvariant();
            string label = $"  [{item.Schema}].[{item.Table}].{item.Column}";

            if (hungarianPrefixes.Any(p => lower.StartsWith(p, StringComparison.OrdinalIgnoreCase)))
                colHungarian.Add(label);

            if (string.Equals(item.Column, item.Column.ToUpperInvariant(), StringComparison.Ordinal) && item.Column.Length > 1)
                colAllCaps.Add(label);

            if (item.Column.Contains('_'))
                colSnakeCase.Add(label);
        }

        var sb = new StringBuilder();
        sb.AppendLine($"NAMING CONVENTION ANALYSIS: [{database}]");
        sb.AppendLine(new string('─', 70));

        AppendViolationSection(sb, "HUNGARIAN PREFIXES (objects)", hungarian);
        AppendViolationSection(sb, "HUNGARIAN PREFIXES (columns)", colHungarian);
        AppendViolationSection(sb, "VERSION SUFFIXES (_v2, _OLD, _FINAL, etc.)", versionSuffix);
        AppendViolationSection(sb, "ALL_CAPS OBJECTS", allCaps);
        AppendViolationSection(sb, "ALL_CAPS COLUMNS", colAllCaps);
        AppendViolationSection(sb, "snake_case OBJECTS", snakeCase);
        AppendViolationSection(sb, "snake_case COLUMNS", colSnakeCase);

        int total = hungarian.Count + colHungarian.Count + versionSuffix.Count
            + allCaps.Count + colAllCaps.Count + snakeCase.Count + colSnakeCase.Count;
        sb.AppendLine($"Total violations: {total}");

        return sb.ToString();
    }

    private static void AppendViolationSection(StringBuilder sb, string header, List<string> items)
    {
        sb.AppendLine();
        sb.AppendLine($"{header} ({items.Count})");
        sb.AppendLine(new string('-', 60));
        if (items.Count == 0)
            sb.AppendLine("  (none)");
        else
            foreach (var item in items)
                sb.AppendLine(item);
    }
}
