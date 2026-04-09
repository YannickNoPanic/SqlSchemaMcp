using System.Text.Json;
using System.Text.Json.Serialization;

namespace SqlSchemaMcp.Data;

public sealed class ConstraintRepository
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = true
    };

    private static readonly string FilePath = Path.Combine(FindProjectRoot(), "constraints.json");

    public static List<ConstraintEntry> List(string? filter = null)
    {
        var file = Load();
        if (filter is null)
            return file.Constraints;

        return [.. file.Constraints
            .Where(c =>
                c.Database.Contains(filter, StringComparison.OrdinalIgnoreCase) ||
                c.ObjectName.Contains(filter, StringComparison.OrdinalIgnoreCase))];
    }

    private static ConstraintsFile Load()
    {
        if (!File.Exists(FilePath))
            return new ConstraintsFile([]);

        var json = File.ReadAllText(FilePath);
        return JsonSerializer.Deserialize<ConstraintsFile>(json, JsonOptions)
            ?? new ConstraintsFile([]);
    }

    private static string FindProjectRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir != null)
        {
            if (dir.GetFiles("*.csproj").Length > 0)
                return dir.FullName;
            dir = dir.Parent;
        }
        return AppContext.BaseDirectory;
    }
}

public sealed class ConstraintsFile(List<ConstraintEntry> constraints)
{
    public List<ConstraintEntry> Constraints { get; init; } = constraints;
}

public sealed record ConstraintEntry(
    string Id,
    string Database,
    string ObjectName,
    string Type,
    string Description,
    [property: JsonPropertyName("addedAt")] DateTime AddedAt);
