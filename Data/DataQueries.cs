using System.Globalization;
using System.Text;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;
using SqlSchemaMcp.Configuration;

namespace SqlSchemaMcp.Data;

public sealed class DataQueries(IOptions<SqlServerOptions> options) : SqlQueryBase(options)
{
    public async Task<string> SampleTableData(
        string database,
        string tableName,
        int rows,
        CancellationToken cancellationToken = default)
    {
        if (!_databases.TryGetValue(database, out var connectionString))
            return UnknownDatabase(database);

        if (rows < 1) rows = 1;
        if (rows > 100) rows = 100;

        var (schema, table) = ParseSchemaTable(tableName);

        try
        {
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(cancellationToken);

            if (!await TableExists(conn, schema, table, cancellationToken))
                return $"ERROR: Table '{tableName}' not found in [{database}].";

            string sampleSql = $"SELECT TOP ({rows}) * FROM [{schema}].[{table}]";
            await using var cmd = new SqlCommand(sampleSql, conn);
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

            int fieldCount = reader.FieldCount;
            var colNames = new string[fieldCount];
            for (int i = 0; i < fieldCount; i++)
                colNames[i] = reader.GetName(i);

            // Buffer rows to compute column widths
            var rowBuffer = new List<string[]>();
            while (await reader.ReadAsync(cancellationToken))
            {
                var vals = new string[fieldCount];
                for (int i = 0; i < fieldCount; i++)
                {
                    string v = reader.IsDBNull(i) ? "NULL" : Convert.ToString(reader.GetValue(i), CultureInfo.InvariantCulture) ?? "";
                    vals[i] = v.Length > 30 ? v[..27] + "..." : v;
                }
                rowBuffer.Add(vals);
            }

            // Compute column widths (header vs data, capped at 30)
            var widths = new int[fieldCount];
            for (int i = 0; i < fieldCount; i++)
            {
                widths[i] = Math.Min(30, colNames[i].Length);
                foreach (var row in rowBuffer)
                    widths[i] = Math.Max(widths[i], row[i].Length);
            }

            var sb = new StringBuilder();
            sb.AppendLine($"SAMPLE DATA: [{schema}].[{table}] in [{database}]  (top {rows} rows)");
            sb.AppendLine(new string('─', 100));
            sb.AppendLine(string.Join("  ", colNames.Select((c, i) => c.Length > widths[i] ? c[..(widths[i] - 3)] + "..." : c.PadRight(widths[i]))));
            sb.AppendLine(new string('-', 100));

            foreach (var row in rowBuffer)
                sb.AppendLine(string.Join("  ", row.Select((v, i) => v.PadRight(widths[i]))));

            if (rowBuffer.Count == 0)
                sb.AppendLine("  (no rows)");
            else
                sb.AppendLine($"\n  {rowBuffer.Count} row(s)");

            return sb.ToString();
        }
        catch (Exception ex)
        {
            return $"ERROR: {ex.Message}";
        }
    }

    public async Task<string> AnalyzeColumnDistribution(
        string database,
        string tableName,
        string columnName,
        CancellationToken cancellationToken = default)
    {
        if (!_databases.TryGetValue(database, out var connectionString))
            return UnknownDatabase(database);

        var (schema, table) = ParseSchemaTable(tableName);

        try
        {
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(cancellationToken);

            if (!await TableExists(conn, schema, table, cancellationToken))
                return $"ERROR: Table '{tableName}' not found in [{database}].";

            // Validate column and get its declared type
            const string colInfoSql = """
                SELECT DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = @table AND COLUMN_NAME = @column
                """;

            string dataType = "";
            int? maxLength = null;
            int? precision = null;
            int? scale = null;
            bool isNullable = false;

            await using (var colCmd = new SqlCommand(colInfoSql, conn))
            {
                colCmd.Parameters.AddWithValue("@schema", schema);
                colCmd.Parameters.AddWithValue("@table", table);
                colCmd.Parameters.AddWithValue("@column", columnName);
                await using var colReader = await colCmd.ExecuteReaderAsync(cancellationToken);
                if (!await colReader.ReadAsync(cancellationToken))
                    return $"ERROR: Column '{columnName}' not found in [{schema}].[{table}].";
                dataType = colReader.GetString(0);
                maxLength = colReader.IsDBNull(1) ? null : colReader.GetInt32(1);
                precision = colReader.IsDBNull(2) ? null : (int)colReader.GetByte(2);
                scale = colReader.IsDBNull(3) ? null : (int)colReader.GetByte(3);
                isNullable = colReader.GetString(4) == "YES";
            }

            bool isTextType = dataType.ToLowerInvariant() is "nvarchar" or "varchar" or "nchar" or "char" or "text" or "ntext";

            string statsSql = isTextType
                ? $"""
                    SELECT
                        COUNT(*) AS TotalRows,
                        COUNT([{columnName}]) AS NonNullCount,
                        COUNT(*) - COUNT([{columnName}]) AS NullCount,
                        COUNT(DISTINCT [{columnName}]) AS DistinctCount,
                        MIN(CAST([{columnName}] AS nvarchar(255))) AS MinValue,
                        MAX(CAST([{columnName}] AS nvarchar(255))) AS MaxValue,
                        MAX(LEN([{columnName}])) AS MaxActualLength,
                        CAST(AVG(CAST(LEN([{columnName}]) AS float)) AS decimal(10,1)) AS AvgActualLength
                    FROM [{schema}].[{table}]
                    """
                : $"""
                    SELECT
                        COUNT(*) AS TotalRows,
                        COUNT([{columnName}]) AS NonNullCount,
                        COUNT(*) - COUNT([{columnName}]) AS NullCount,
                        COUNT(DISTINCT [{columnName}]) AS DistinctCount,
                        MIN(CAST([{columnName}] AS nvarchar(255))) AS MinValue,
                        MAX(CAST([{columnName}] AS nvarchar(255))) AS MaxValue,
                        NULL AS MaxActualLength,
                        NULL AS AvgActualLength
                    FROM [{schema}].[{table}]
                    """;

            var sb = new StringBuilder();
            sb.AppendLine($"COLUMN DISTRIBUTION: [{schema}].[{table}].[{columnName}] in [{database}]");
            sb.AppendLine(new string('─', 70));
            sb.AppendLine($"  Declared type: {FormatColumnType(dataType, maxLength, precision, scale)}  Nullable: {(isNullable ? "YES" : "NO")}");
            sb.AppendLine();

            await using var statsCmd = new SqlCommand(statsSql, conn);
            await using var statsReader = await statsCmd.ExecuteReaderAsync(cancellationToken);

            if (await statsReader.ReadAsync(cancellationToken))
            {
                long total = statsReader.GetInt64(0);
                long nonNull = statsReader.GetInt64(1);
                long nullCount = statsReader.GetInt64(2);
                long distinct = statsReader.GetInt64(3);
                string? minVal = statsReader.IsDBNull(4) ? null : statsReader.GetString(4);
                string? maxVal = statsReader.IsDBNull(5) ? null : statsReader.GetString(5);
                int? maxActual = statsReader.IsDBNull(6) ? null : statsReader.GetInt32(6);
                string? avgActual = statsReader.IsDBNull(7) ? null : statsReader.GetDecimal(7).ToString(CultureInfo.InvariantCulture);

                double nullPct = total == 0 ? 0 : Math.Round(nullCount * 100.0 / total, 1);

                sb.AppendLine($"  Total rows:      {total:N0}");
                sb.AppendLine($"  Non-null:        {nonNull:N0}");
                sb.AppendLine($"  Null:            {nullCount:N0}  ({nullPct}%)");
                sb.AppendLine($"  Distinct values: {distinct:N0}");
                sb.AppendLine($"  Min value:       {minVal ?? "NULL"}");
                sb.AppendLine($"  Max value:       {maxVal ?? "NULL"}");

                if (isTextType && maxLength.HasValue)
                {
                    int declared = maxLength.Value == -1 ? int.MaxValue : maxLength.Value;
                    sb.AppendLine($"  Max actual len:  {(maxActual.HasValue ? maxActual.Value.ToString(CultureInfo.InvariantCulture) : "N/A")}  (declared: {(maxLength.Value == -1 ? "max" : maxLength.Value.ToString(CultureInfo.InvariantCulture))})");
                    sb.AppendLine($"  Avg actual len:  {avgActual ?? "N/A"}");

                    if (maxActual.HasValue && declared != int.MaxValue && maxActual.Value < declared / 2)
                        sb.AppendLine($"  NOTE: column is declared {declared} chars but max actual content is {maxActual.Value} — consider downsizing.");
                }
            }

            return sb.ToString();
        }
        catch (Exception ex)
        {
            return $"ERROR: {ex.Message}";
        }
    }

    public async Task<string> FindNullableColumnsWithNoNulls(
        string database,
        string tableName,
        CancellationToken cancellationToken = default)
    {
        if (!_databases.TryGetValue(database, out var connectionString))
            return UnknownDatabase(database);

        var (schema, table) = ParseSchemaTable(tableName);

        try
        {
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(cancellationToken);

            if (!await TableExists(conn, schema, table, cancellationToken))
                return $"ERROR: Table '{tableName}' not found in [{database}].";

            const string colSql = """
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = @table AND IS_NULLABLE = 'YES'
                ORDER BY ORDINAL_POSITION
                """;

            var nullableColumns = new List<string>();
            await using (var colCmd = new SqlCommand(colSql, conn))
            {
                colCmd.Parameters.AddWithValue("@schema", schema);
                colCmd.Parameters.AddWithValue("@table", table);
                await using var colReader = await colCmd.ExecuteReaderAsync(cancellationToken);
                while (await colReader.ReadAsync(cancellationToken))
                    nullableColumns.Add(colReader.GetString(0));
            }

            if (nullableColumns.Count == 0)
            {
                return $"NULLABLE COLUMNS WITH NO NULLS: [{schema}].[{table}] in [{database}]\n" +
                       new string('─', 70) + "\n  (no nullable columns in this table)";
            }

            // Build one query with EXISTS per nullable column
            var unionParts = nullableColumns.Select(col =>
                $"SELECT '{col.Replace("'", "''")}' AS ColumnName, " +
                $"CASE WHEN EXISTS (SELECT 1 FROM [{schema}].[{table}] WHERE [{col}] IS NULL) THEN 0 ELSE 1 END AS HasNoNulls");

            string checkSql = string.Join("\nUNION ALL\n", unionParts);

            var noNullCols = new List<string>();

            await using (var checkCmd = new SqlCommand(checkSql, conn))
            {
                checkCmd.CommandTimeout = 120;
                await using var checkReader = await checkCmd.ExecuteReaderAsync(cancellationToken);
                while (await checkReader.ReadAsync(cancellationToken))
                {
                    if (checkReader.GetInt32(1) == 1)
                        noNullCols.Add(checkReader.GetString(0));
                }
            }

            var sb = new StringBuilder();
            sb.AppendLine($"NULLABLE COLUMNS WITH NO NULLS: [{schema}].[{table}] in [{database}]");
            sb.AppendLine(new string('─', 70));
            sb.AppendLine($"  Nullable columns checked: {nullableColumns.Count}");
            sb.AppendLine();

            if (noNullCols.Count == 0)
            {
                sb.AppendLine("  (all nullable columns contain at least one NULL — no candidates found)");
            }
            else
            {
                sb.AppendLine($"  Candidates for NOT NULL constraint ({noNullCols.Count}):");
                foreach (var col in noNullCols)
                    sb.AppendLine($"    {col}");
            }

            return sb.ToString();
        }
        catch (Exception ex)
        {
            return $"ERROR: {ex.Message}";
        }
    }

    public async Task<string> FindDuplicateRows(
        string database,
        string tableName,
        string columns,
        int top,
        CancellationToken cancellationToken = default)
    {
        if (!_databases.TryGetValue(database, out var connectionString))
            return UnknownDatabase(database);

        if (top < 1) top = 1;
        if (top > 200) top = 200;

        var (schema, table) = ParseSchemaTable(tableName);

        // Parse and validate column names
        var requestedCols = columns
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(c => c.Trim('[', ']'))
            .ToList();

        if (requestedCols.Count == 0)
            return "ERROR: No columns specified.";

        try
        {
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(cancellationToken);

            if (!await TableExists(conn, schema, table, cancellationToken))
                return $"ERROR: Table '{tableName}' not found in [{database}].";

            // Validate each column exists
            const string colCheckSql = """
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = @table
                """;

            var validCols = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            await using (var colCmd = new SqlCommand(colCheckSql, conn))
            {
                colCmd.Parameters.AddWithValue("@schema", schema);
                colCmd.Parameters.AddWithValue("@table", table);
                await using var colReader = await colCmd.ExecuteReaderAsync(cancellationToken);
                while (await colReader.ReadAsync(cancellationToken))
                    validCols.Add(colReader.GetString(0));
            }

            var invalidCols = requestedCols.Where(c => !validCols.Contains(c)).ToList();
            if (invalidCols.Count > 0)
                return $"ERROR: Column(s) not found in [{schema}].[{table}]: {string.Join(", ", invalidCols)}";

            string colList = string.Join(", ", requestedCols.Select(c => $"[{c}]"));
            string dupSql = $"""
                SELECT TOP ({top}) {colList}, COUNT(*) AS DuplicateCount
                FROM [{schema}].[{table}]
                GROUP BY {colList}
                HAVING COUNT(*) > 1
                ORDER BY DuplicateCount DESC
                """;

            await using var dupCmd = new SqlCommand(dupSql, conn);
            dupCmd.CommandTimeout = 120;
            await using var reader = await dupCmd.ExecuteReaderAsync(cancellationToken);

            var sb = new StringBuilder();
            sb.AppendLine($"DUPLICATE ROWS: [{schema}].[{table}] in [{database}]");
            sb.AppendLine($"  Grouped by: {string.Join(", ", requestedCols)}");
            sb.AppendLine(new string('─', 90));

            int fieldCount = reader.FieldCount;
            var headers = new string[fieldCount];
            for (int i = 0; i < fieldCount; i++)
                headers[i] = reader.GetName(i);
            sb.AppendLine(string.Join("  |  ", headers.Select(h => h.PadRight(25))));
            sb.AppendLine(new string('-', 90));

            int count = 0;
            while (await reader.ReadAsync(cancellationToken))
            {
                count++;
                var vals = new string[fieldCount];
                for (int i = 0; i < fieldCount; i++)
                {
                    string v = reader.IsDBNull(i) ? "NULL" : Convert.ToString(reader.GetValue(i), CultureInfo.InvariantCulture) ?? "";
                    vals[i] = v.Length > 25 ? v[..22] + "..." : v;
                }
                sb.AppendLine(string.Join("  |  ", vals.Select(v => v.PadRight(25))));
            }

            if (count == 0)
                sb.AppendLine("  (no duplicate rows found — column combination is unique)");
            else
                sb.AppendLine($"\n  {count} duplicate group(s) found");

            return sb.ToString();
        }
        catch (Exception ex)
        {
            return $"ERROR: {ex.Message}";
        }
    }
}
