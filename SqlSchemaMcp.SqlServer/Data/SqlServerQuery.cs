using System.Globalization;
using System.Text;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SqlSchemaMcp.Abstractions.Capabilities;
using SqlSchemaMcp.SqlServer.Configuration;

namespace SqlSchemaMcp.SqlServer.Data;

public sealed class SqlServerQuery(IOptions<SqlServerEngineOptions> options, ILogger<SqlServerQuery> logger)
    : SqlQueryBase(options, logger), IReadOnlyQueryCapability
{
    private const int MaxRows = 500;
    private const int CommandTimeoutSeconds = 30;

    public async Task<string> ExecuteQuery(
        string database,
        string sql,
        CancellationToken ct = default)
    {
        if (!_databases.TryGetValue(database, out var connectionString))
            return UnknownDatabase(database);

        var validation = SqlStatementValidator.Validate(sql);
        if (!validation.IsAllowed)
            return $"ERROR: {validation.Reason}";

        try
        {
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(ct);

            await using var cmd = new SqlCommand(sql, conn);
            cmd.CommandTimeout = CommandTimeoutSeconds;
            await using var reader = await cmd.ExecuteReaderAsync(ct);

            int fieldCount = reader.FieldCount;
            var colNames = new string[fieldCount];
            for (int i = 0; i < fieldCount; i++)
                colNames[i] = reader.GetName(i);

            // Buffer up to MaxRows + 1 so we can detect whether the result was truncated.
            var rowBuffer = new List<string[]>();
            while (await reader.ReadAsync(ct) && rowBuffer.Count <= MaxRows)
            {
                var vals = new string[fieldCount];
                for (int i = 0; i < fieldCount; i++)
                {
                    string v = reader.IsDBNull(i) ? "NULL" : Convert.ToString(reader.GetValue(i), CultureInfo.InvariantCulture) ?? "";
                    vals[i] = v.Length > 50 ? v[..47] + "..." : v;
                }
                rowBuffer.Add(vals);
            }

            bool truncated = rowBuffer.Count > MaxRows;
            if (truncated)
                rowBuffer.RemoveAt(rowBuffer.Count - 1);

            // Column widths: max of header length and widest value, capped at 50.
            var widths = new int[fieldCount];
            for (int i = 0; i < fieldCount; i++)
            {
                widths[i] = Math.Min(50, colNames[i].Length);
                foreach (var row in rowBuffer)
                    widths[i] = Math.Max(widths[i], row[i].Length);
            }

            var sb = new StringBuilder();
            sb.AppendLine(new string('─', 100));
            sb.AppendLine(string.Join("  ", colNames.Select((c, i) =>
            {
                var display = c.Length > widths[i] ? c[..(widths[i] - 3)] + "..." : c;
                return display.PadRight(widths[i]);
            })));
            sb.AppendLine(new string('-', 100));

            if (rowBuffer.Count == 0)
            {
                sb.AppendLine("  (no rows)");
            }
            else
            {
                foreach (var row in rowBuffer)
                    sb.AppendLine(string.Join("  ", row.Select((v, i) => v.PadRight(widths[i]))));
            }

            sb.AppendLine(new string('─', 100));
            sb.Append(truncated
                ? $"  {MaxRows} row(s) shown — result truncated (result set exceeds {MaxRows} rows)"
                : $"  {rowBuffer.Count} row(s)");

            return sb.ToString();
        }
        catch (Exception ex)
        {
            return SafeError(ex);
        }
    }
}
