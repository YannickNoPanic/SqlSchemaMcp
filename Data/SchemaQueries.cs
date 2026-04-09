using System.Globalization;
using System.Text;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;
using SqlSchemaMcp.Configuration;

namespace SqlSchemaMcp.Data;

public sealed class SchemaQueries(IOptions<SqlServerOptions> options) : SqlQueryBase(options)
{
    public async Task<string> ListTables(
        string database,
        string? schemaFilter,
        string? nameFilter,
        CancellationToken cancellationToken = default)
    {
        if (!_databases.TryGetValue(database, out var connectionString))
            return UnknownDatabase(database);

        const string sql = """
            SELECT
                t.TABLE_SCHEMA,
                t.TABLE_NAME,
                CAST(ep.value AS nvarchar(500)) AS Description,
                ISNULL(SUM(p.rows), 0) AS ApproxRows
            FROM INFORMATION_SCHEMA.TABLES t
            LEFT JOIN sys.extended_properties ep
                ON ep.major_id = OBJECT_ID(QUOTENAME(t.TABLE_SCHEMA) + '.' + QUOTENAME(t.TABLE_NAME))
                AND ep.minor_id = 0
                AND ep.name = 'MS_Description'
                AND ep.class = 1
            LEFT JOIN sys.partitions p
                ON p.object_id = OBJECT_ID(QUOTENAME(t.TABLE_SCHEMA) + '.' + QUOTENAME(t.TABLE_NAME))
                AND p.index_id <= 1
            WHERE t.TABLE_TYPE = 'BASE TABLE'
                AND (@schemaFilter IS NULL OR t.TABLE_SCHEMA = @schemaFilter)
                AND (@nameFilter IS NULL OR t.TABLE_NAME LIKE '%' + @nameFilter + '%')
            GROUP BY t.TABLE_SCHEMA, t.TABLE_NAME, ep.value
            ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME
            """;

        try
        {
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(cancellationToken);
            await using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@schemaFilter", (object?)schemaFilter ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@nameFilter", (object?)nameFilter ?? DBNull.Value);

            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
            var sb = new StringBuilder();
            sb.AppendLine($"TABLES in [{database}]");
            sb.AppendLine(new string('─', 70));
            sb.AppendLine($"{"Schema",-20} {"Table",-40} {"~Rows",10}");
            sb.AppendLine(new string('─', 70));

            int count = 0;
            while (await reader.ReadAsync(cancellationToken))
            {
                count++;
                string schema = reader.GetString(0);
                string table = reader.GetString(1);
                string? description = reader.IsDBNull(2) ? null : reader.GetString(2);
                long approxRows = reader.GetInt64(3);

                sb.AppendLine($"{schema,-20} {table,-40} {approxRows,10:N0}");
                if (description != null)
                    sb.AppendLine($"{"",20}   {description}");
            }

            if (count == 0)
                sb.AppendLine("  (no tables found)");
            else
                sb.AppendLine(new string('─', 70));
            sb.AppendLine($"  {count} table(s)");

            return sb.ToString();
        }
        catch (Exception ex)
        {
            return $"ERROR: {ex.Message}";
        }
    }

    public async Task<string> ListViews(
        string database,
        string? nameFilter,
        CancellationToken cancellationToken = default)
    {
        if (!_databases.TryGetValue(database, out var connectionString))
            return UnknownDatabase(database);

        const string sql = """
            SELECT TABLE_SCHEMA, TABLE_NAME
            FROM INFORMATION_SCHEMA.VIEWS
            WHERE (@nameFilter IS NULL OR TABLE_NAME LIKE '%' + @nameFilter + '%')
            ORDER BY TABLE_SCHEMA, TABLE_NAME
            """;

        try
        {
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(cancellationToken);
            await using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@nameFilter", (object?)nameFilter ?? DBNull.Value);

            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
            var sb = new StringBuilder();
            sb.AppendLine($"VIEWS in [{database}]");
            sb.AppendLine(new string('─', 60));

            int count = 0;
            while (await reader.ReadAsync(cancellationToken))
            {
                count++;
                sb.AppendLine($"  [{reader.GetString(0)}].[{reader.GetString(1)}]");
            }

            if (count == 0)
                sb.AppendLine("  (no views found)");
            sb.AppendLine($"\n  {count} view(s)");

            return sb.ToString();
        }
        catch (Exception ex)
        {
            return $"ERROR: {ex.Message}";
        }
    }

    public async Task<string> ListProcedures(
        string database,
        string? nameFilter,
        CancellationToken cancellationToken = default)
    {
        if (!_databases.TryGetValue(database, out var connectionString))
            return UnknownDatabase(database);

        const string sql = """
            SELECT ROUTINE_SCHEMA, ROUTINE_NAME, LAST_ALTERED
            FROM INFORMATION_SCHEMA.ROUTINES
            WHERE ROUTINE_TYPE = 'PROCEDURE'
                AND (@nameFilter IS NULL OR ROUTINE_NAME LIKE '%' + @nameFilter + '%')
            ORDER BY ROUTINE_SCHEMA, ROUTINE_NAME
            """;

        try
        {
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(cancellationToken);
            await using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@nameFilter", (object?)nameFilter ?? DBNull.Value);

            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
            var sb = new StringBuilder();
            sb.AppendLine($"STORED PROCEDURES in [{database}]");
            sb.AppendLine(new string('─', 70));
            sb.AppendLine($"{"Schema",-20} {"Procedure",-40} {"Last Modified",-20}");
            sb.AppendLine(new string('─', 70));

            int count = 0;
            while (await reader.ReadAsync(cancellationToken))
            {
                count++;
                sb.AppendLine($"{reader.GetString(0),-20} {reader.GetString(1),-40} {reader.GetDateTime(2):yyyy-MM-dd HH:mm}");
            }

            if (count == 0)
                sb.AppendLine("  (no procedures found)");
            else
                sb.AppendLine(new string('─', 70));
            sb.AppendLine($"  {count} procedure(s)");

            return sb.ToString();
        }
        catch (Exception ex)
        {
            return $"ERROR: {ex.Message}";
        }
    }

    public async Task<string> ListFunctions(
        string database,
        string? nameFilter,
        CancellationToken cancellationToken = default)
    {
        if (!_databases.TryGetValue(database, out var connectionString))
            return UnknownDatabase(database);

        const string sql = """
            SELECT
                SCHEMA_NAME(o.schema_id) AS SchemaName,
                o.name AS FunctionName,
                o.type_desc AS FunctionType,
                o.modify_date AS LastModified
            FROM sys.objects o
            WHERE o.type IN ('FN', 'IF', 'TF', 'FS', 'FT')
                AND (@nameFilter IS NULL OR o.name LIKE '%' + @nameFilter + '%')
            ORDER BY o.type_desc, SCHEMA_NAME(o.schema_id), o.name
            """;

        try
        {
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(cancellationToken);
            await using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@nameFilter", (object?)nameFilter ?? DBNull.Value);
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

            var sb = new StringBuilder();
            sb.AppendLine($"FUNCTIONS in [{database}]");
            sb.AppendLine(new string('─', 80));
            sb.AppendLine($"{"Schema",-20} {"Function",-40} {"Type",-30} {"Last Modified",-16}");
            sb.AppendLine(new string('─', 80));

            int count = 0;
            while (await reader.ReadAsync(cancellationToken))
            {
                count++;
                sb.AppendLine($"{reader.GetString(0),-20} {reader.GetString(1),-40} {reader.GetString(2),-30} {reader.GetDateTime(3):yyyy-MM-dd HH:mm}");
            }

            if (count == 0)
                sb.AppendLine("  (no functions found)");
            else
                sb.AppendLine(new string('─', 80));
            sb.AppendLine($"  {count} function(s)");
            return sb.ToString();
        }
        catch (Exception ex)
        {
            return $"ERROR: {ex.Message}";
        }
    }

    public async Task<string> ListTriggers(
        string database,
        string? nameFilter,
        CancellationToken cancellationToken = default)
    {
        if (!_databases.TryGetValue(database, out var connectionString))
            return UnknownDatabase(database);

        const string sql = """
            SELECT
                SCHEMA_NAME(p.schema_id) AS SchemaName,
                p.name AS TableName,
                t.name AS TriggerName,
                t.is_disabled,
                t.is_instead_of_trigger,
                MAX(CASE WHEN te.type_desc = 'INSERT' THEN 1 ELSE 0 END) AS OnInsert,
                MAX(CASE WHEN te.type_desc = 'UPDATE' THEN 1 ELSE 0 END) AS OnUpdate,
                MAX(CASE WHEN te.type_desc = 'DELETE' THEN 1 ELSE 0 END) AS OnDelete,
                o.modify_date
            FROM sys.triggers t
            JOIN sys.objects o ON o.object_id = t.object_id
            JOIN sys.objects p ON p.object_id = t.parent_id
            JOIN sys.trigger_events te ON te.object_id = t.object_id
            WHERE t.parent_class = 1
                AND (@nameFilter IS NULL OR t.name LIKE '%' + @nameFilter + '%')
            GROUP BY SCHEMA_NAME(p.schema_id), p.name, t.name,
                     t.is_disabled, t.is_instead_of_trigger, o.modify_date
            ORDER BY p.name, t.name
            """;

        try
        {
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(cancellationToken);
            await using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@nameFilter", (object?)nameFilter ?? DBNull.Value);
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

            var sb = new StringBuilder();
            sb.AppendLine($"TRIGGERS in [{database}]");
            sb.AppendLine(new string('─', 100));
            sb.AppendLine($"{"Table",-40} {"Trigger",-35} {"Events",-15} {"Type",-12} {"Enabled",-8} Modified");
            sb.AppendLine(new string('─', 100));

            int count = 0;
            while (await reader.ReadAsync(cancellationToken))
            {
                count++;
                string schema = reader.GetString(0);
                string table = reader.GetString(1);
                string trigger = reader.GetString(2);
                bool disabled = reader.GetBoolean(3);
                bool insteadOf = reader.GetBoolean(4);
                bool ins = reader.GetInt32(5) == 1;
                bool upd = reader.GetInt32(6) == 1;
                bool del = reader.GetInt32(7) == 1;
                DateTime modified = reader.GetDateTime(8);

                string events = string.Join(",", new[] { ins ? "INS" : null, upd ? "UPD" : null, del ? "DEL" : null }.Where(e => e != null));
                string type = insteadOf ? "INSTEAD OF" : "AFTER";
                string enabled = disabled ? "DISABLED" : "yes";

                sb.AppendLine($"{$"[{schema}].[{table}]",-40} {trigger,-35} {events,-15} {type,-12} {enabled,-8} {modified:yyyy-MM-dd}");
            }

            if (count == 0)
                sb.AppendLine("  (no triggers found)");
            else
                sb.AppendLine(new string('─', 100));
            sb.AppendLine($"  {count} trigger(s)");
            return sb.ToString();
        }
        catch (Exception ex)
        {
            return $"ERROR: {ex.Message}";
        }
    }

    public async Task<string> ListSynonyms(
        string database,
        string? nameFilter,
        CancellationToken cancellationToken = default)
    {
        if (!_databases.TryGetValue(database, out var connectionString))
            return UnknownDatabase(database);

        const string sql = """
            SELECT
                SCHEMA_NAME(s.schema_id) AS SchemaName,
                s.name AS SynonymName,
                s.base_object_name AS TargetObject,
                s.create_date
            FROM sys.synonyms s
            WHERE @nameFilter IS NULL OR s.name LIKE '%' + @nameFilter + '%'
            ORDER BY SCHEMA_NAME(s.schema_id), s.name
            """;

        try
        {
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(cancellationToken);
            await using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@nameFilter", (object?)nameFilter ?? DBNull.Value);
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

            var sb = new StringBuilder();
            sb.AppendLine($"SYNONYMS in [{database}]");
            sb.AppendLine(new string('─', 90));
            sb.AppendLine($"{"Schema",-20} {"Synonym",-35} {"Target Object",-30} Created");
            sb.AppendLine(new string('─', 90));

            int count = 0;
            while (await reader.ReadAsync(cancellationToken))
            {
                count++;
                sb.AppendLine($"{reader.GetString(0),-20} {reader.GetString(1),-35} {reader.GetString(2),-30} {reader.GetDateTime(3):yyyy-MM-dd}");
            }

            if (count == 0)
                sb.AppendLine("  (no synonyms found)");
            else
                sb.AppendLine(new string('─', 90));
            sb.AppendLine($"  {count} synonym(s)");
            return sb.ToString();
        }
        catch (Exception ex)
        {
            return $"ERROR: {ex.Message}";
        }
    }

    public async Task<string> ListCheckConstraints(
        string database,
        string? nameFilter,
        CancellationToken cancellationToken = default)
    {
        if (!_databases.TryGetValue(database, out var connectionString))
            return UnknownDatabase(database);

        const string sql = """
            SELECT
                SCHEMA_NAME(t.schema_id) AS SchemaName,
                t.name AS TableName,
                cc.name AS ConstraintName,
                COL_NAME(cc.parent_object_id, cc.parent_column_id) AS ColumnName,
                cc.definition AS CheckExpression,
                cc.is_disabled,
                cc.is_not_trusted
            FROM sys.check_constraints cc
            JOIN sys.objects t ON t.object_id = cc.parent_object_id
            WHERE @nameFilter IS NULL OR t.name LIKE '%' + @nameFilter + '%'
            ORDER BY SCHEMA_NAME(t.schema_id), t.name, cc.name
            """;

        try
        {
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(cancellationToken);
            await using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@nameFilter", (object?)nameFilter ?? DBNull.Value);
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

            var sb = new StringBuilder();
            sb.AppendLine($"CHECK CONSTRAINTS in [{database}]");
            sb.AppendLine(new string('─', 100));

            int count = 0;
            string? lastTable = null;
            while (await reader.ReadAsync(cancellationToken))
            {
                count++;
                string schema = reader.GetString(0);
                string table = reader.GetString(1);
                string constraint = reader.GetString(2);
                string? column = reader.IsDBNull(3) ? null : reader.GetString(3);
                string expression = reader.GetString(4);
                bool disabled = reader.GetBoolean(5);
                bool notTrusted = reader.GetBoolean(6);

                string tableKey = $"[{schema}].[{table}]";
                if (tableKey != lastTable)
                {
                    if (lastTable != null) sb.AppendLine();
                    sb.AppendLine($"  TABLE: {tableKey}");
                    lastTable = tableKey;
                }

                string flags = string.Join(", ", new[] { disabled ? "DISABLED" : null, notTrusted ? "NOT TRUSTED" : null }.Where(f => f != null));
                string colInfo = column != null ? $"  column: {column}" : "  (table-level)";
                sb.AppendLine($"    [{constraint}]{colInfo}");
                sb.AppendLine($"      {expression}{(flags.Length > 0 ? $"  [{flags}]" : "")}");
            }

            if (count == 0)
                sb.AppendLine("  (no check constraints found)");
            sb.AppendLine();
            sb.AppendLine($"  {count} check constraint(s)");
            return sb.ToString();
        }
        catch (Exception ex)
        {
            return $"ERROR: {ex.Message}";
        }
    }

    public async Task<string> GetTableSchema(
        string database,
        string tableName,
        CancellationToken cancellationToken = default)
    {
        if (!_databases.TryGetValue(database, out var connectionString))
            return UnknownDatabase(database);

        var (schema, table) = ParseSchemaTable(tableName);
        string schemaTable = $"[{schema}].[{table}]";

        const string columnSql = """
            SELECT
                c.COLUMN_NAME,
                c.DATA_TYPE,
                c.CHARACTER_MAXIMUM_LENGTH,
                c.NUMERIC_PRECISION,
                c.NUMERIC_SCALE,
                c.IS_NULLABLE,
                c.COLUMN_DEFAULT,
                CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 'YES' ELSE 'NO' END AS IS_PK,
                CAST(ep.value AS nvarchar(500)) AS Description
            FROM INFORMATION_SCHEMA.COLUMNS c
            LEFT JOIN (
                SELECT ku.COLUMN_NAME
                FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku
                    ON ku.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
                    AND ku.TABLE_SCHEMA = tc.TABLE_SCHEMA
                    AND ku.TABLE_NAME = tc.TABLE_NAME
                WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                    AND tc.TABLE_SCHEMA = @schema
                    AND tc.TABLE_NAME = @table
            ) pk ON pk.COLUMN_NAME = c.COLUMN_NAME
            LEFT JOIN sys.columns sc
                ON sc.object_id = OBJECT_ID(@schemaTable)
                AND sc.name = c.COLUMN_NAME
            LEFT JOIN sys.extended_properties ep
                ON ep.major_id = sc.object_id
                AND ep.minor_id = sc.column_id
                AND ep.name = 'MS_Description'
                AND ep.class = 1
            WHERE c.TABLE_SCHEMA = @schema
                AND c.TABLE_NAME = @table
            ORDER BY c.ORDINAL_POSITION
            """;

        const string fkSql = """
            SELECT
                fk.name AS FkName,
                COL_NAME(fkc.parent_object_id, fkc.parent_column_id) AS ColumnName,
                OBJECT_SCHEMA_NAME(fkc.referenced_object_id) AS RefSchema,
                OBJECT_NAME(fkc.referenced_object_id) AS RefTable,
                COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id) AS RefColumn
            FROM sys.foreign_keys fk
            JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id
            WHERE fk.parent_object_id = OBJECT_ID(@schemaTable)
            ORDER BY fk.name
            """;

        const string indexSql = """
            SELECT
                i.name AS IndexName,
                i.is_unique,
                STUFF((
                    SELECT ', ' + c2.name
                    FROM sys.index_columns ic2
                    JOIN sys.columns c2
                        ON c2.object_id = ic2.object_id AND c2.column_id = ic2.column_id
                    WHERE ic2.object_id = i.object_id
                        AND ic2.index_id = i.index_id
                        AND ic2.is_included_column = 0
                    ORDER BY ic2.key_ordinal
                    FOR XML PATH(''), TYPE).value('.', 'nvarchar(max)'), 1, 2, '') AS Columns
            FROM sys.indexes i
            WHERE i.object_id = OBJECT_ID(@schemaTable)
                AND i.is_primary_key = 0
                AND i.type > 0
            GROUP BY i.name, i.is_unique, i.object_id, i.index_id
            ORDER BY i.name
            """;

        try
        {
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(cancellationToken);

            var sb = new StringBuilder();
            sb.AppendLine($"TABLE: [{schema}].[{table}]");
            sb.AppendLine(new string('─', 90));

            // Columns
            await using (var cmd = new SqlCommand(columnSql, conn))
            {
                cmd.Parameters.AddWithValue("@schema", schema);
                cmd.Parameters.AddWithValue("@table", table);
                cmd.Parameters.AddWithValue("@schemaTable", schemaTable);

                await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

                sb.AppendLine($"{"Column",-35} {"Type",-25} {"Null",-6} {"PK",-5} {"Default"}");
                sb.AppendLine(new string('─', 90));

                bool any = false;
                while (await reader.ReadAsync(cancellationToken))
                {
                    any = true;
                    string colName = reader.GetString(0);
                    string dataType = reader.GetString(1);
                    int? maxLen = reader.IsDBNull(2) ? null : reader.GetInt32(2);
                    int? precision = reader.IsDBNull(3) ? null : Convert.ToInt32(reader.GetValue(3));
                    int? scale = reader.IsDBNull(4) ? null : Convert.ToInt32(reader.GetValue(4));
                    string nullable = reader.GetString(5);
                    string? defaultVal = reader.IsDBNull(6) ? null : reader.GetString(6);
                    string isPk = reader.GetString(7);
                    string? description = reader.IsDBNull(8) ? null : reader.GetString(8);

                    string typeStr = FormatColumnType(dataType, maxLen, precision, scale);
                    string pkFlag = isPk == "YES" ? "PK" : "";
                    string nullFlag = nullable == "YES" ? "YES" : "NO";

                    sb.AppendLine($"{colName,-35} {typeStr,-25} {nullFlag,-6} {pkFlag,-5} {defaultVal ?? ""}");
                    if (description != null)
                        sb.AppendLine($"{"",35}   -- {description}");
                }

                if (!any)
                {
                    sb.AppendLine("  (table not found or no columns)");
                    return sb.ToString();
                }
            }

            // Foreign keys
            sb.AppendLine();
            sb.AppendLine("FOREIGN KEYS");
            sb.AppendLine(new string('─', 90));

            await using (var cmd = new SqlCommand(fkSql, conn))
            {
                cmd.Parameters.AddWithValue("@schemaTable", schemaTable);
                await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

                bool any = false;
                while (await reader.ReadAsync(cancellationToken))
                {
                    any = true;
                    sb.AppendLine($"  [{reader.GetString(0)}]");
                    sb.AppendLine($"    {reader.GetString(1)} -> [{reader.GetString(2)}].[{reader.GetString(3)}]({reader.GetString(4)})");
                }

                if (!any)
                    sb.AppendLine("  (none)");
            }

            // Indexes
            sb.AppendLine();
            sb.AppendLine("INDEXES");
            sb.AppendLine(new string('─', 90));

            await using (var cmd = new SqlCommand(indexSql, conn))
            {
                cmd.Parameters.AddWithValue("@schemaTable", schemaTable);
                await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

                bool any = false;
                while (await reader.ReadAsync(cancellationToken))
                {
                    any = true;
                    string unique = reader.GetBoolean(1) ? " UNIQUE" : "";
                    sb.AppendLine($"  [{reader.GetString(0)}]{unique}  ({reader.GetString(2)})");
                }

                if (!any)
                    sb.AppendLine("  (none)");
            }

            return sb.ToString();
        }
        catch (Exception ex)
        {
            return $"ERROR: {ex.Message}";
        }
    }

    public async Task<string> GetViewDefinition(
        string database,
        string viewName,
        CancellationToken cancellationToken = default)
    {
        if (!_databases.TryGetValue(database, out var connectionString))
            return UnknownDatabase(database);

        const string sql = """
            SELECT m.definition
            FROM sys.sql_modules m
            JOIN sys.objects o ON o.object_id = m.object_id
            WHERE o.type = 'V'
                AND o.name = @name
            """;

        try
        {
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(cancellationToken);
            await using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@name", viewName.Trim('[', ']'));

            var result = await cmd.ExecuteScalarAsync(cancellationToken);
            if (result is null or DBNull)
                return $"ERROR: View '{viewName}' not found in [{database}].";

            return $"VIEW DEFINITION: [{viewName}] in [{database}]\n{new string('─', 60)}\n{result}";
        }
        catch (Exception ex)
        {
            return $"ERROR: {ex.Message}";
        }
    }

    public async Task<string> GetProcedureDefinition(
        string database,
        string procName,
        CancellationToken cancellationToken = default)
    {
        if (!_databases.TryGetValue(database, out var connectionString))
            return UnknownDatabase(database);

        const string sql = """
            SELECT m.definition
            FROM sys.sql_modules m
            JOIN sys.objects o ON o.object_id = m.object_id
            WHERE o.type = 'P'
                AND o.name = @name
            """;

        try
        {
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(cancellationToken);
            await using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@name", procName.Trim('[', ']'));

            var result = await cmd.ExecuteScalarAsync(cancellationToken);
            if (result is null or DBNull)
                return $"ERROR: Procedure '{procName}' not found in [{database}].";

            return $"PROCEDURE DEFINITION: [{procName}] in [{database}]\n{new string('─', 60)}\n{result}";
        }
        catch (Exception ex)
        {
            return $"ERROR: {ex.Message}";
        }
    }

    public async Task<string> GetFunctionDefinition(
        string database,
        string functionName,
        CancellationToken cancellationToken = default)
    {
        if (!_databases.TryGetValue(database, out var connectionString))
            return UnknownDatabase(database);

        const string sql = """
            SELECT m.definition
            FROM sys.sql_modules m
            JOIN sys.objects o ON o.object_id = m.object_id
            WHERE o.type IN ('FN', 'IF', 'TF')
                AND o.name = @name
            """;

        try
        {
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(cancellationToken);
            await using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@name", functionName.Trim('[', ']'));

            var result = await cmd.ExecuteScalarAsync(cancellationToken);
            if (result is null or DBNull)
                return $"ERROR: Function '{functionName}' not found in [{database}].";

            return $"FUNCTION DEFINITION: [{functionName}] in [{database}]\n{new string('─', 60)}\n{result}";
        }
        catch (Exception ex)
        {
            return $"ERROR: {ex.Message}";
        }
    }

    public async Task<string> GetTriggerDefinition(
        string database,
        string triggerName,
        CancellationToken cancellationToken = default)
    {
        if (!_databases.TryGetValue(database, out var connectionString))
            return UnknownDatabase(database);

        const string sql = """
            SELECT m.definition
            FROM sys.sql_modules m
            JOIN sys.triggers t ON t.object_id = m.object_id
            WHERE t.name = @name
            """;

        try
        {
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(cancellationToken);
            await using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@name", triggerName.Trim('[', ']'));

            var result = await cmd.ExecuteScalarAsync(cancellationToken);
            if (result is null or DBNull)
                return $"ERROR: Trigger '{triggerName}' not found in [{database}].";

            return $"TRIGGER DEFINITION: [{triggerName}] in [{database}]\n{new string('─', 60)}\n{result}";
        }
        catch (Exception ex)
        {
            return $"ERROR: {ex.Message}";
        }
    }

    public async Task<string> FindReferences(
        string database,
        string objectName,
        CancellationToken cancellationToken = default)
    {
        if (!_databases.TryGetValue(database, out var connectionString))
            return UnknownDatabase(database);

        // Ensure two-part name for sys.dm_sql_referencing_entities
        string qualifiedName = objectName.Contains('.') ? objectName : $"dbo.{objectName}";

        const string sql = """
            SELECT
                referencing_schema_name,
                referencing_entity_name,
                referencing_class_desc
            FROM sys.dm_sql_referencing_entities(@objectName, 'OBJECT')
            ORDER BY referencing_schema_name, referencing_entity_name
            """;

        try
        {
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(cancellationToken);
            await using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@objectName", qualifiedName);

            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
            var sb = new StringBuilder();
            sb.AppendLine($"REFERENCES TO [{objectName}] in [{database}]");
            sb.AppendLine(new string('─', 60));

            int count = 0;
            while (await reader.ReadAsync(cancellationToken))
            {
                count++;
                string refSchema = reader.IsDBNull(0) ? "" : reader.GetString(0);
                string refName = reader.GetString(1);
                string refClass = reader.GetString(2);
                sb.AppendLine($"  [{refSchema}].[{refName}]  ({refClass})");
            }

            if (count == 0)
                sb.AppendLine("  (no references found)");
            sb.AppendLine($"\n  {count} reference(s)");

            return sb.ToString();
        }
        catch (Exception ex)
        {
            return $"ERROR: {ex.Message}";
        }
    }

    public async Task<string> SearchDefinitions(
        string database,
        string keyword,
        CancellationToken cancellationToken = default)
    {
        if (!_databases.TryGetValue(database, out var connectionString))
            return UnknownDatabase(database);

        const string sql = """
            SELECT o.name, o.type_desc
            FROM sys.sql_modules m
            JOIN sys.objects o ON o.object_id = m.object_id
            WHERE o.type IN ('P', 'V')
                AND m.definition LIKE '%' + @keyword + '%'
            ORDER BY o.type_desc, o.name
            """;

        try
        {
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(cancellationToken);
            await using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@keyword", keyword);

            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
            var sb = new StringBuilder();
            sb.AppendLine($"SEARCH RESULTS for '{keyword}' in [{database}]");
            sb.AppendLine(new string('─', 60));

            int count = 0;
            while (await reader.ReadAsync(cancellationToken))
            {
                count++;
                sb.AppendLine($"  {reader.GetString(1),-25} {reader.GetString(0)}");
            }

            if (count == 0)
                sb.AppendLine("  (no matches)");
            sb.AppendLine($"\n  {count} match(es)");

            return sb.ToString();
        }
        catch (Exception ex)
        {
            return $"ERROR: {ex.Message}";
        }
    }

    public async Task<string> ListDdlTriggers(
        string database,
        CancellationToken cancellationToken = default)
    {
        if (!_databases.TryGetValue(database, out var connectionString))
            return UnknownDatabase(database);

        const string sql = """
            SELECT
                t.name,
                t.is_disabled,
                t.create_date,
                t.modify_date,
                STUFF((
                    SELECT ', ' + te.type_desc
                    FROM sys.trigger_events te
                    WHERE te.object_id = t.object_id
                    FOR XML PATH(''), TYPE).value('.', 'nvarchar(max)'), 1, 2, '') AS events
            FROM sys.triggers t
            WHERE t.parent_class = 0
            ORDER BY t.name
            """;

        SqlCommandGuard.AssertReadOnly(sql);

        try
        {
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(cancellationToken);
            await using var cmd = new SqlCommand(sql, conn);
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

            var sb = new StringBuilder();
            sb.AppendLine($"DDL TRIGGERS in [{database}]");
            sb.AppendLine(new string('─', 90));
            sb.AppendLine($"{"Trigger",-40} {"Enabled",-8} {"Events",-40} Modified");
            sb.AppendLine(new string('─', 90));

            int count = 0;
            while (await reader.ReadAsync(cancellationToken))
            {
                count++;
                string name = reader.GetString(0);
                bool disabled = reader.GetBoolean(1);
                DateTime modified = reader.GetDateTime(3);
                string events = reader.IsDBNull(4) ? "(none)" : reader.GetString(4);

                sb.AppendLine($"{name,-40} {(disabled ? "no" : "yes"),-8} {events,-40} {modified:yyyy-MM-dd}");
            }

            if (count == 0)
                sb.AppendLine("  (no DDL triggers found)");
            else
                sb.AppendLine(new string('─', 90));
            sb.AppendLine($"  {count} DDL trigger(s)");

            return sb.ToString();
        }
        catch (Exception ex)
        {
            return $"ERROR: {ex.Message}";
        }
    }

    public async Task<string> GetDdlTriggerDefinition(
        string database,
        string triggerName,
        CancellationToken cancellationToken = default)
    {
        if (!_databases.TryGetValue(database, out var connectionString))
            return UnknownDatabase(database);

        const string sql = """
            SELECT m.definition
            FROM sys.sql_modules m
            JOIN sys.triggers t ON t.object_id = m.object_id
            WHERE t.parent_class = 0
                AND t.name = @name
            """;

        SqlCommandGuard.AssertReadOnly(sql);

        try
        {
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(cancellationToken);
            await using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@name", triggerName);
            var result = await cmd.ExecuteScalarAsync(cancellationToken);

            if (result is null or DBNull)
                return $"DDL trigger '{triggerName}' not found in [{database}].";

            return $"DDL TRIGGER: {triggerName}\n{new string('─', 60)}\n{result}";
        }
        catch (Exception ex)
        {
            return $"ERROR: {ex.Message}";
        }
    }
}
