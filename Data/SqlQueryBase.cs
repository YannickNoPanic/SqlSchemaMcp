using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;
using SqlSchemaMcp.Configuration;

namespace SqlSchemaMcp.Data;

public abstract partial class SqlQueryBase(IOptions<SqlServerOptions> options)
{
    protected readonly Dictionary<string, string> _databases = options.Value.Databases;

    protected string UnknownDatabase(string database) =>
        $"ERROR: Unknown database '{database}'. Available: {string.Join(", ", _databases.Keys)}";

    protected static (string Schema, string Table) ParseSchemaTable(string tableName)
    {
        var trimmed = tableName.Trim();

        // Bracketed name like [WR.Conf.Devices] — dots are part of the table name, not a schema separator
        if (trimmed.StartsWith('['))
        {
            int closingBracket = trimmed.IndexOf(']');
            if (closingBracket > 0)
            {
                string firstPart = trimmed[1..closingBracket];
                string remainder = trimmed[(closingBracket + 1)..].Trim();

                // [schema].[table] or [schema].table
                if (remainder.StartsWith('.'))
                {
                    string rest = remainder[1..].Trim().Trim('[', ']');
                    return (firstPart, rest);
                }

                // [tableName] only — no schema
                return ("dbo", firstPart);
            }
        }

        // Unbracketed with dot: schema.table (only split if no further dots, otherwise treat as table name with dots)
        if (trimmed.Contains('.'))
        {
            var parts = trimmed.Split('.', 2);
            return (parts[0].Trim('[', ']'), parts[1].Trim('[', ']'));
        }

        return ("dbo", trimmed.Trim('[', ']'));
    }

    protected static async Task<bool> TableExists(SqlConnection conn, string schema, string table, CancellationToken cancellationToken)
    {
        const string sql = """
            SELECT TOP 1 1
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = @schema AND TABLE_NAME = @table
            """;
        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@schema", schema);
        cmd.Parameters.AddWithValue("@table", table);
        var result = await cmd.ExecuteScalarAsync(cancellationToken);
        return result is not null and not DBNull;
    }

    protected static string FormatColumnType(string dataType, int? maxLength, int? precision, int? scale) =>
        dataType.ToLowerInvariant() switch
        {
            "nvarchar" or "varchar" or "nchar" or "char" =>
                maxLength == -1 ? $"{dataType}(max)" : $"{dataType}({maxLength})",
            "decimal" or "numeric" =>
                $"{dataType}({precision},{scale})",
            _ => dataType
        };

    protected static string BoolFlag(bool value) => value ? "YES" : "NO";

    protected static string FormatKb(long kb) =>
        kb >= 1024 * 1024 ? $"{kb / 1024 / 1024:N1} GB"
        : kb >= 1024 ? $"{kb / 1024:N1} MB"
        : $"{kb:N0} KB";

    // Matches the BaseName_YYYYMMDD_HHMMSS pattern used by the ETL pipeline.
    protected const string StagingExcludeLike =
        "%[_][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][_][0-9][0-9][0-9][0-9][0-9][0-9]";

    [GeneratedRegex(@"_\d{8}_\d{6}$")]
    protected static partial Regex StagingRegex();

    protected static bool IsStaging(string tableName) => StagingRegex().IsMatch(tableName);
}
