using System.Text.RegularExpressions;

namespace SqlSchemaMcp.Data;

/// <summary>
/// Validates SQL command text before execution. Rejects any statement that contains
/// write or DDL keywords so that a misconfigured or accidentally elevated account
/// cannot modify data. The primary protection is the database account permissions;
/// this is defence-in-depth.
/// </summary>
public static partial class SqlCommandGuard
{
    [GeneratedRegex(@"\b(INSERT|UPDATE|DELETE|DROP|TRUNCATE|ALTER|CREATE|EXEC|EXECUTE|MERGE|GRANT|REVOKE|DENY)\b", RegexOptions.IgnoreCase)]
    private static partial Regex WriteRegex();

    /// <summary>
    /// Throws <see cref="InvalidOperationException"/> if the SQL contains any write or DDL keyword.
    /// </summary>
    public static void AssertReadOnly(string sql)
    {
        var match = WriteRegex().Match(sql);
        if (match.Success)
            throw new InvalidOperationException(
                $"Write operation blocked by SqlCommandGuard: keyword '{match.Value}' is not permitted. " +
                "This MCP server is read-only.");
    }
}
