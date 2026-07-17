namespace SqlSchemaMcp.Configuration;

public static class DotEnvConfiguration
{
    private const string Prefix = "SQLMCP_";

    public static IReadOnlyDictionary<string, string?> Load(string path)
    {
        var values = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase);

        if (!File.Exists(path))
            return values;

        foreach (var rawLine in File.ReadLines(path))
        {
            var line = rawLine.Trim();
            if (line.Length == 0 || line.StartsWith('#'))
                continue;

            if (line.StartsWith("export ", StringComparison.Ordinal))
                line = line["export ".Length..].TrimStart();

            int separator = line.IndexOf('=');
            if (separator <= 0)
                continue;

            var key = line[..separator].Trim();
            if (!key.StartsWith(Prefix, StringComparison.Ordinal))
                continue;

            var value = Unquote(line[(separator + 1)..].Trim());
            var configurationKey = key[Prefix.Length..].Replace("__", ":", StringComparison.Ordinal);
            values[configurationKey] = value;
        }

        return values;
    }

    public static string? FindNearest(string startDirectory)
    {
        var directory = new DirectoryInfo(startDirectory);

        while (directory is not null)
        {
            var candidate = Path.Combine(directory.FullName, ".env");
            if (File.Exists(candidate))
                return candidate;

            directory = directory.Parent;
        }

        return null;
    }

    public static IReadOnlyDictionary<string, string?> LoadNearest(params string[] startDirectories)
    {
        foreach (var startDirectory in startDirectories.Where(Directory.Exists).Distinct(StringComparer.OrdinalIgnoreCase))
        {
            var path = FindNearest(startDirectory);
            if (path is not null)
                return Load(path);
        }

        return new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase);
    }

    private static string Unquote(string value)
    {
        if (value.Length >= 2
            && ((value[0] == '"' && value[^1] == '"') || (value[0] == '\'' && value[^1] == '\'')))
        {
            return value[1..^1];
        }

        return value;
    }
}
