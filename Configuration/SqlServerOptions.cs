namespace SqlSchemaMcp.Configuration;

public sealed class SqlServerOptions
{
    public Dictionary<string, string> Databases { get; init; } = new(StringComparer.OrdinalIgnoreCase);

    public IReadOnlyList<string> GetConfigurationErrors()
    {
        var errors = new List<string>();

        foreach (var (name, connectionString) in Databases)
        {
            if (string.IsNullOrWhiteSpace(connectionString))
            {
                errors.Add($"SQLMCP_SqlServer__Databases__{name} is empty.");
                continue;
            }

            var trimmed = connectionString.Trim();
            int firstEquals = trimmed.IndexOf('=');
            int firstSeparator = trimmed.IndexOf(';');

            if (firstEquals > 0
                && firstSeparator > firstEquals
                && trimmed[..firstEquals].Contains("connection_string", StringComparison.OrdinalIgnoreCase))
            {
                errors.Add(
                    $"SQLMCP_SqlServer__Databases__{name} must contain the SQL Server connection string value directly. "
                    + "Do not prefix it with another environment variable assignment.");
            }

            if (trimmed.Contains(",Encrypt=", StringComparison.OrdinalIgnoreCase))
            {
                errors.Add(
                    $"SQLMCP_SqlServer__Databases__{name} uses a comma before 'Encrypt='. "
                    + "Use a semicolon-separated SQL Server connection string.");
            }
        }

        return errors;
    }
}
