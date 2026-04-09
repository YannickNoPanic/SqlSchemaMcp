using System.ComponentModel;
using System.Text;
using ModelContextProtocol.Server;
using SqlSchemaMcp.Data;

namespace SqlSchemaMcp.Tools;

[McpServerToolType]
public sealed class ConstraintTools
{
    [McpServerTool, Description("List all constraint/context entries. Optionally filter by database name or object name (partial match).")]
    public static string ListConstraints(
        [Description("Optional filter string — matches against database name or object name")] string? filter = null)
    {
        var entries = ConstraintRepository.List(filter);

        var sb = new StringBuilder();
        sb.AppendLine("CONSTRAINTS" + (filter != null ? $" (filter: '{filter}')" : ""));
        sb.AppendLine(new string('─', 80));

        if (entries.Count == 0)
        {
            sb.AppendLine("  (none)");
            return sb.ToString();
        }

        foreach (var e in entries)
        {
            sb.AppendLine($"  [{e.Id}]  {e.Database} / {e.ObjectName}");
            sb.AppendLine($"    Type:    {e.Type}");
            sb.AppendLine($"    Added:   {e.AddedAt:yyyy-MM-dd}");
            sb.AppendLine($"    {e.Description}");
            sb.AppendLine();
        }

        sb.AppendLine($"  {entries.Count} entry/entries");
        return sb.ToString();
    }
}
