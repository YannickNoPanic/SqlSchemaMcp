using System.ComponentModel;
using System.Text;
using ModelContextProtocol.Server;
using SqlSchemaMcp.Data;

namespace SqlSchemaMcp.Tools;

[McpServerToolType]
public sealed class CompareTools(CompareQueries queries)
{
    [McpServerTool, Description("Compare tables across two databases: tables only in db1, only in db2, and in both.")]
    public async Task<string> CompareTables(
        [Description("First database name")] string database1,
        [Description("Second database name")] string database2,
        CancellationToken cancellationToken = default)
    {
        var (tables1, tables2, error) = await LoadBothSets(
            () => queries.GetTableNames(database1, cancellationToken),
            () => queries.GetTableNames(database2, cancellationToken));

        if (error != null) return error;

        return BuildSetComparisonReport("TABLE COMPARISON", database1, database2, tables1!, tables2!);
    }

    [McpServerTool, Description("Compare stored procedures across two databases: procs only in db1, only in db2, and in both.")]
    public async Task<string> CompareProcs(
        [Description("First database name")] string database1,
        [Description("Second database name")] string database2,
        CancellationToken cancellationToken = default)
    {
        var (procs1, procs2, error) = await LoadBothSets(
            () => queries.GetProcNames(database1, cancellationToken),
            () => queries.GetProcNames(database2, cancellationToken));

        if (error != null) return error;

        return BuildSetComparisonReport("PROCEDURE COMPARISON", database1, database2, procs1!, procs2!);
    }

    [McpServerTool, Description("Column-level diff of a table between two databases: columns missing in each, and columns present in both with type or nullability differences.")]
    public async Task<string> CompareTable(
        [Description("First database name")] string database1,
        [Description("Second database name")] string database2,
        [Description("Table name to compare (e.g. 'Organisations' or 'dbo.Organisations')")] string tableName,
        CancellationToken cancellationToken = default)
    {
        var cols1Task = queries.GetTableColumns(database1, tableName, cancellationToken);
        var cols2Task = queries.GetTableColumns(database2, tableName, cancellationToken);
        await Task.WhenAll(cols1Task, cols2Task);

        var cols1 = cols1Task.Result;
        var cols2 = cols2Task.Result;

        var sb = new StringBuilder();
        sb.AppendLine($"TABLE DIFF: [{tableName}]  ({database1} vs {database2})");
        sb.AppendLine(new string('─', 90));

        var map1 = cols1.ToDictionary(c => c.Name, StringComparer.OrdinalIgnoreCase);
        var map2 = cols2.ToDictionary(c => c.Name, StringComparer.OrdinalIgnoreCase);

        var onlyIn1 = cols1.Where(c => !map2.ContainsKey(c.Name)).ToList();
        var onlyIn2 = cols2.Where(c => !map1.ContainsKey(c.Name)).ToList();
        var inBoth = cols1.Where(c => map2.ContainsKey(c.Name)).ToList();

        sb.AppendLine($"\nONLY IN [{database1}] ({onlyIn1.Count})");
        sb.AppendLine(new string('-', 60));
        if (onlyIn1.Count == 0)
            sb.AppendLine("  (none)");
        else
            foreach (var c in onlyIn1)
                sb.AppendLine($"  {c.Name,-35} {c.Type,-25} NULL={c.Nullable}");

        sb.AppendLine($"\nONLY IN [{database2}] ({onlyIn2.Count})");
        sb.AppendLine(new string('-', 60));
        if (onlyIn2.Count == 0)
            sb.AppendLine("  (none)");
        else
            foreach (var c in onlyIn2)
                sb.AppendLine($"  {c.Name,-35} {c.Type,-25} NULL={c.Nullable}");

        sb.AppendLine($"\nDIFFERENCES IN BOTH ({inBoth.Count} shared columns checked)");
        sb.AppendLine(new string('-', 60));

        int diffCount = 0;
        foreach (var c1 in inBoth)
        {
            var c2 = map2[c1.Name];
            if (!string.Equals(c1.Type, c2.Type, StringComparison.OrdinalIgnoreCase) ||
                !string.Equals(c1.Nullable, c2.Nullable, StringComparison.OrdinalIgnoreCase))
            {
                diffCount++;
                sb.AppendLine($"  {c1.Name}");
                sb.AppendLine($"    {database1}: {c1.Type} NULL={c1.Nullable}");
                sb.AppendLine($"    {database2}: {c2.Type} NULL={c2.Nullable}");
            }
        }

        if (diffCount == 0)
            sb.AppendLine("  (no type or nullability differences)");

        return sb.ToString();
    }

    [McpServerTool, Description("Compare views across two databases: views only in db1, only in db2, and in both.")]
    public async Task<string> CompareViews(
        [Description("First database name")] string database1,
        [Description("Second database name")] string database2,
        CancellationToken cancellationToken = default)
    {
        var (views1, views2, error) = await LoadBothSets(
            () => queries.GetViewNames(database1, cancellationToken),
            () => queries.GetViewNames(database2, cancellationToken));

        if (error != null) return error;

        return BuildSetComparisonReport("VIEW COMPARISON", database1, database2, views1!, views2!);
    }

    [McpServerTool, Description("Compare a view between two databases: existence, line count difference, and tables referenced in each.")]
    public async Task<string> CompareView(
        [Description("First database name")] string database1,
        [Description("Second database name")] string database2,
        [Description("View name to compare")] string viewName,
        CancellationToken cancellationToken = default)
    {
        var stats1Task = queries.GetViewStats(database1, viewName, cancellationToken);
        var stats2Task = queries.GetViewStats(database2, viewName, cancellationToken);
        await Task.WhenAll(stats1Task, stats2Task);

        var (lines1, tables1) = stats1Task.Result;
        var (lines2, tables2) = stats2Task.Result;

        bool existsIn1 = lines1 > 0;
        bool existsIn2 = lines2 > 0;

        var sb = new StringBuilder();
        sb.AppendLine($"VIEW DIFF: [{viewName}]  ({database1} vs {database2})");
        sb.AppendLine(new string('─', 70));
        sb.AppendLine($"  Exists in [{database1}]: {(existsIn1 ? "YES" : "NO")}");
        sb.AppendLine($"  Exists in [{database2}]: {(existsIn2 ? "YES" : "NO")}");

        if (!existsIn1 || !existsIn2)
            return sb.ToString();

        sb.AppendLine();
        sb.AppendLine($"  Line count [{database1}]: {lines1}");
        sb.AppendLine($"  Line count [{database2}]: {lines2}");
        sb.AppendLine($"  Difference:              {Math.Abs(lines1 - lines2)} line(s) {(lines1 > lines2 ? $"(more in {database1})" : lines2 > lines1 ? $"(more in {database2})" : "(identical)")}");

        var onlyIn1 = tables1.Except(tables2, StringComparer.OrdinalIgnoreCase).ToList();
        var onlyIn2 = tables2.Except(tables1, StringComparer.OrdinalIgnoreCase).ToList();
        var inBoth = tables1.Intersect(tables2, StringComparer.OrdinalIgnoreCase).ToList();

        sb.AppendLine();
        sb.AppendLine("TABLES REFERENCED");
        sb.AppendLine(new string('-', 50));
        sb.AppendLine($"  Only in [{database1}]: {(onlyIn1.Count == 0 ? "(none)" : string.Join(", ", onlyIn1))}");
        sb.AppendLine($"  Only in [{database2}]: {(onlyIn2.Count == 0 ? "(none)" : string.Join(", ", onlyIn2))}");
        sb.AppendLine($"  In both: {(inBoth.Count == 0 ? "(none)" : string.Join(", ", inBoth))}");

        return sb.ToString();
    }

    [McpServerTool, Description("Compare a stored procedure between two databases: existence, line count difference, and tables referenced in each.")]
    public async Task<string> CompareProc(
        [Description("First database name")] string database1,
        [Description("Second database name")] string database2,
        [Description("Stored procedure name to compare")] string procName,
        CancellationToken cancellationToken = default)
    {
        var stats1Task = queries.GetProcStats(database1, procName, cancellationToken);
        var stats2Task = queries.GetProcStats(database2, procName, cancellationToken);
        await Task.WhenAll(stats1Task, stats2Task);

        var (lines1, tables1) = stats1Task.Result;
        var (lines2, tables2) = stats2Task.Result;

        bool existsIn1 = lines1 > 0;
        bool existsIn2 = lines2 > 0;

        var sb = new StringBuilder();
        sb.AppendLine($"PROCEDURE DIFF: [{procName}]  ({database1} vs {database2})");
        sb.AppendLine(new string('─', 70));
        sb.AppendLine($"  Exists in [{database1}]: {(existsIn1 ? "YES" : "NO")}");
        sb.AppendLine($"  Exists in [{database2}]: {(existsIn2 ? "YES" : "NO")}");

        if (!existsIn1 || !existsIn2)
            return sb.ToString();

        sb.AppendLine();
        sb.AppendLine($"  Line count [{database1}]: {lines1}");
        sb.AppendLine($"  Line count [{database2}]: {lines2}");
        sb.AppendLine($"  Difference:              {Math.Abs(lines1 - lines2)} line(s) {(lines1 > lines2 ? $"(more in {database1})" : lines2 > lines1 ? $"(more in {database2})" : "(identical)")}");

        var onlyIn1 = tables1.Except(tables2, StringComparer.OrdinalIgnoreCase).ToList();
        var onlyIn2 = tables2.Except(tables1, StringComparer.OrdinalIgnoreCase).ToList();
        var inBoth = tables1.Intersect(tables2, StringComparer.OrdinalIgnoreCase).ToList();

        sb.AppendLine();
        sb.AppendLine($"TABLES REFERENCED");
        sb.AppendLine(new string('-', 50));
        sb.AppendLine($"  Only in [{database1}]: {(onlyIn1.Count == 0 ? "(none)" : string.Join(", ", onlyIn1))}");
        sb.AppendLine($"  Only in [{database2}]: {(onlyIn2.Count == 0 ? "(none)" : string.Join(", ", onlyIn2))}");
        sb.AppendLine($"  In both: {(inBoth.Count == 0 ? "(none)" : string.Join(", ", inBoth))}");

        return sb.ToString();
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static async Task<(HashSet<string>? Set1, HashSet<string>? Set2, string? Error)> LoadBothSets(
        Func<Task<HashSet<string>>> load1,
        Func<Task<HashSet<string>>> load2)
    {
        var t1 = load1();
        var t2 = load2();
        await Task.WhenAll(t1, t2);
        return (t1.Result, t2.Result, null);
    }

    private static string BuildSetComparisonReport(
        string title,
        string database1,
        string database2,
        HashSet<string> set1,
        HashSet<string> set2)
    {
        var onlyIn1 = set1.Where(x => !set2.Contains(x)).OrderBy(x => x).ToList();
        var onlyIn2 = set2.Where(x => !set1.Contains(x)).OrderBy(x => x).ToList();
        var inBoth = set1.Where(x => set2.Contains(x)).OrderBy(x => x).ToList();

        var sb = new StringBuilder();
        sb.AppendLine($"{title}: [{database1}] vs [{database2}]");
        sb.AppendLine(new string('─', 70));

        sb.AppendLine($"\nONLY IN [{database1}] ({onlyIn1.Count})");
        sb.AppendLine(new string('-', 50));
        if (onlyIn1.Count == 0) sb.AppendLine("  (none)");
        else foreach (var x in onlyIn1) sb.AppendLine($"  {x}");

        sb.AppendLine($"\nONLY IN [{database2}] ({onlyIn2.Count})");
        sb.AppendLine(new string('-', 50));
        if (onlyIn2.Count == 0) sb.AppendLine("  (none)");
        else foreach (var x in onlyIn2) sb.AppendLine($"  {x}");

        sb.AppendLine($"\nIN BOTH ({inBoth.Count})");
        sb.AppendLine(new string('-', 50));
        if (inBoth.Count == 0) sb.AppendLine("  (none)");
        else foreach (var x in inBoth) sb.AppendLine($"  {x}");

        return sb.ToString();
    }
}
