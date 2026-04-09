using System.Globalization;
using System.Text;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;
using SqlSchemaMcp.Configuration;

namespace SqlSchemaMcp.Data;

public sealed class PipelineQueries(IOptions<SqlServerOptions> options) : SqlQueryBase(options)
{
    public async Task<string> ListDataFeeds(
        string database,
        CancellationToken cancellationToken = default)
    {
        if (!_databases.TryGetValue(database, out var connectionString))
            return UnknownDatabase(database);

        try
        {
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(cancellationToken);

            var (feeds, currentNames) = await FetchFeedGroups(conn, cancellationToken);

            var sb = new StringBuilder();
            sb.AppendLine($"DATA FEEDS: [{database}]");
            sb.AppendLine(new string('─', 100));
            sb.AppendLine($"{"Feed",-40} {"Staging",7}  {"Latest",-21} {"Earliest",-21} {"Current",7}");
            sb.AppendLine(new string('─', 100));

            foreach (var f in feeds)
            {
                string latest = f.Latest == DateTime.MinValue ? "(none)" : f.Latest.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
                string earliest = f.Earliest == DateTime.MinValue ? "(none)" : f.Earliest.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
                sb.AppendLine($"{f.BaseName,-40} {f.StagingCount,7}  {latest,-21} {earliest,-21} {BoolFlag(f.CurrentTableExists),7}");
            }

            sb.AppendLine(new string('─', 100));
            sb.AppendLine($"  {feeds.Count} feed(s)   {feeds.Sum(f => f.StagingCount)} staging table(s)");

            var feedBaseNames = new HashSet<string>(feeds.Select(f => f.BaseName), StringComparer.OrdinalIgnoreCase);
            int orphanedCurrentCount = currentNames.Count(name => !feedBaseNames.Contains(name));
            if (orphanedCurrentCount > 0)
                sb.AppendLine($"  {orphanedCurrentCount} current table(s) with no matching staging feed (manual or retired)");

            return sb.ToString();
        }
        catch (Exception ex)
        {
            return $"ERROR: {ex.Message}";
        }
    }

    public async Task<string> AnalyzeStagingHealth(
        string database,
        CancellationToken cancellationToken = default)
    {
        if (!_databases.TryGetValue(database, out var connectionString))
            return UnknownDatabase(database);

        try
        {
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(cancellationToken);

            var (feeds, _) = await FetchFeedGroups(conn, cancellationToken);
            var now = DateTime.UtcNow;

            var flagged = feeds
                .Select(f =>
                {
                    var flags = new List<string>();
                    if (f.Latest != DateTime.MinValue && now - f.Latest > TimeSpan.FromHours(24))
                        flags.Add("OVERDUE");
                    if (f.Earliest != DateTime.MinValue && now - f.Earliest > TimeSpan.FromDays(5))
                        flags.Add("CLEANUP LATE");
                    if (f.StagingCount > 6)
                        flags.Add("EXCESS");
                    return (Feed: f, Flags: flags);
                })
                .ToList();

            int okCount = flagged.Count(x => x.Flags.Count == 0);
            var problems = flagged
                .Where(x => x.Flags.Count > 0)
                .OrderByDescending(x => x.Flags.Count)
                .ThenBy(x => x.Feed.BaseName, StringComparer.OrdinalIgnoreCase)
                .ToList();

            var sb = new StringBuilder();
            sb.AppendLine($"STAGING HEALTH: [{database}]");
            sb.AppendLine($"As of: {now:yyyy-MM-dd HH:mm:ss} UTC");
            sb.AppendLine(new string('─', 100));
            sb.AppendLine();
            sb.AppendLine($"OK feeds: {okCount} (not shown)");
            sb.AppendLine();

            if (problems.Count == 0)
            {
                sb.AppendLine("  All feeds are healthy.");
            }
            else
            {
                sb.AppendLine($"FLAGGED FEEDS ({problems.Count}):");
                sb.AppendLine(new string('─', 100));
                sb.AppendLine($"{"Feed",-40} {"Staging",7}  {"Latest",-21} {"Flag(s)"}");
                sb.AppendLine(new string('─', 100));
                foreach (var (f, flags) in problems)
                {
                    string latest = f.Latest == DateTime.MinValue ? "(none)" : f.Latest.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
                    sb.AppendLine($"{f.BaseName,-40} {f.StagingCount,7}  {latest,-21} {string.Join(", ", flags)}");
                }
                sb.AppendLine(new string('─', 100));
                sb.AppendLine($"  {problems.Count} flagged feed(s)   {okCount} ok");
            }

            return sb.ToString();
        }
        catch (Exception ex)
        {
            return $"ERROR: {ex.Message}";
        }
    }

    public async Task<string> CompareStagingToCurrentSchema(
        string database,
        string feedBaseName,
        string currentTableName,
        CancellationToken cancellationToken = default)
    {
        if (!_databases.TryGetValue(database, out var connectionString))
            return UnknownDatabase(database);

        feedBaseName = feedBaseName.Trim('[', ']');
        currentTableName = currentTableName.Trim('[', ']');

        const string findLatestSql = """
            SELECT TOP 1 name
            FROM sys.tables
            WHERE name LIKE @prefix + '[_][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][_][0-9][0-9][0-9][0-9][0-9][0-9]'
                AND type = 'U'
            ORDER BY name DESC
            """;

        try
        {
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(cancellationToken);

            // Find most recent staging table
            string? stagingTableName = null;
            await using (var cmd = new SqlCommand(findLatestSql, conn))
            {
                cmd.Parameters.AddWithValue("@prefix", feedBaseName);
                var result = await cmd.ExecuteScalarAsync(cancellationToken);
                stagingTableName = result as string;
            }

            if (stagingTableName == null)
                return $"No staging tables found for feed '{feedBaseName}'.";

            static async Task<Dictionary<string, string>> FetchColumns(SqlConnection c, string tableName, CancellationToken ct)
            {
                const string sql = """
                    SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = @tableName AND TABLE_SCHEMA = 'dbo'
                    ORDER BY ORDINAL_POSITION
                    """;
                var dict = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                await using var cmd = new SqlCommand(sql, c);
                cmd.Parameters.AddWithValue("@tableName", tableName);
                await using var reader = await cmd.ExecuteReaderAsync(ct);
                while (await reader.ReadAsync(ct))
                {
                    string name = reader.GetString(0);
                    string dataType = reader.GetString(1);
                    int? maxLen = reader.IsDBNull(2) ? null : reader.GetInt32(2);
                    int? prec = reader.IsDBNull(3) ? null : Convert.ToInt32(reader.GetValue(3));
                    int? scale = reader.IsDBNull(4) ? null : Convert.ToInt32(reader.GetValue(4));
                    dict[name] = FormatColumnType(dataType, maxLen, prec, scale);
                }
                return dict;
            }

            var stagingCols = await FetchColumns(conn, stagingTableName, cancellationToken);
            var currentCols = await FetchColumns(conn, currentTableName, cancellationToken);

            if (currentCols.Count == 0)
                return $"No columns found for current table '{currentTableName}'. Verify the table name.";

            var onlyInStaging = stagingCols.Keys.Except(currentCols.Keys, StringComparer.OrdinalIgnoreCase).OrderBy(x => x).ToList();
            var onlyInCurrent = currentCols.Keys.Except(stagingCols.Keys, StringComparer.OrdinalIgnoreCase).OrderBy(x => x).ToList();
            var inBoth = stagingCols.Keys.Intersect(currentCols.Keys, StringComparer.OrdinalIgnoreCase).ToList();
            var typeMismatches = inBoth
                .Where(col => !stagingCols[col].Equals(currentCols[col], StringComparison.OrdinalIgnoreCase))
                .OrderBy(x => x)
                .ToList();

            var sb = new StringBuilder();
            sb.AppendLine($"STAGING VS CURRENT SCHEMA COMPARISON: [{database}]");
            sb.AppendLine($"Feed:           {feedBaseName}");
            sb.AppendLine($"Staging table:  [dbo].[{stagingTableName}]");
            sb.AppendLine($"Current table:  [dbo].[{currentTableName}]");
            sb.AppendLine(new string('─', 80));

            sb.AppendLine();
            sb.AppendLine($"COLUMNS DROPPED BY mk_* PROC (in staging only) ({onlyInStaging.Count}):");
            if (onlyInStaging.Count == 0)
                sb.AppendLine("  (none)");
            else
                foreach (var col in onlyInStaging)
                    sb.AppendLine($"  {col,-35} {stagingCols[col]}");

            sb.AppendLine();
            sb.AppendLine($"COLUMNS ADDED BY mk_* PROC (in current only) ({onlyInCurrent.Count}):");
            if (onlyInCurrent.Count == 0)
                sb.AppendLine("  (none)");
            else
                foreach (var col in onlyInCurrent)
                    sb.AppendLine($"  {col,-35} {currentCols[col]}");

            sb.AppendLine();
            sb.AppendLine($"TYPE MISMATCHES (same column, different type) ({typeMismatches.Count}):");
            if (typeMismatches.Count == 0)
            {
                sb.AppendLine("  (none)");
            }
            else
            {
                sb.AppendLine($"  {"Column",-35} {"Staging Type",-25} {"Current Type"}");
                sb.AppendLine($"  {new string('─', 70)}");
                foreach (var col in typeMismatches)
                    sb.AppendLine($"  {col,-35} {stagingCols[col],-25} {currentCols[col]}");
            }

            sb.AppendLine();
            sb.AppendLine(new string('─', 80));
            sb.AppendLine($"  {onlyInStaging.Count} dropped   {onlyInCurrent.Count} added   {typeMismatches.Count} type mismatch(es)   {inBoth.Count - typeMismatches.Count} identical");

            return sb.ToString();
        }
        catch (Exception ex)
        {
            return $"ERROR: {ex.Message}";
        }
    }

    private sealed record FeedGroup(
        string BaseName,
        int StagingCount,
        DateTime Latest,
        DateTime Earliest,
        bool CurrentTableExists);

    private static async Task<(List<FeedGroup> Feeds, List<string> CurrentNames)> FetchFeedGroups(
        SqlConnection conn,
        CancellationToken cancellationToken)
    {
        const string sql = """
            SELECT name FROM sys.tables WHERE type = 'U' ORDER BY name
            """;

        var allNames = new List<string>();
        await using (var cmd = new SqlCommand(sql, conn))
        await using (var reader = await cmd.ExecuteReaderAsync(cancellationToken))
            while (await reader.ReadAsync(cancellationToken))
                allNames.Add(reader.GetString(0));

        var currentNames = allNames.Where(n => !IsStaging(n)).ToList();
        var currentNamesSet = new HashSet<string>(currentNames, StringComparer.OrdinalIgnoreCase);

        var feeds = allNames
            .Where(IsStaging)
            .GroupBy(n => n[..^16], StringComparer.OrdinalIgnoreCase)
            .Select(g =>
            {
                var dates = g
                    .Select(n =>
                    {
                        string suffix = n[^15..];
                        return DateTime.TryParseExact(
                            suffix, "yyyyMMdd_HHmmss",
                            CultureInfo.InvariantCulture,
                            DateTimeStyles.AssumeUniversal, out var dt) ? dt : (DateTime?)null;
                    })
                    .Where(d => d.HasValue)
                    .Select(d => d!.Value)
                    .ToList();

                return new FeedGroup(
                    BaseName: g.Key,
                    StagingCount: g.Count(),
                    Latest: dates.Count > 0 ? dates.Max() : DateTime.MinValue,
                    Earliest: dates.Count > 0 ? dates.Min() : DateTime.MinValue,
                    CurrentTableExists: currentNamesSet.Contains(g.Key));
            })
            .OrderBy(f => f.BaseName, StringComparer.OrdinalIgnoreCase)
            .ToList();

        return (feeds, currentNames);
    }
}
