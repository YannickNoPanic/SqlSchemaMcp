using System.Globalization;
using System.Text;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;
using SqlSchemaMcp.Configuration;

namespace SqlSchemaMcp.Data;

public sealed class DiagnosticsQueries(IOptions<SqlServerOptions> options) : SqlQueryBase(options)
{
    public async Task<string> ListAgentJobs(
        string database,
        CancellationToken cancellationToken = default)
    {
        if (!_databases.TryGetValue(database, out var connectionString))
            return UnknownDatabase(database);

        const string sql = """
            SELECT
                j.name,
                j.enabled,
                j.description,
                h.run_date,
                h.run_time,
                h.run_status,
                h.message
            FROM msdb.dbo.sysjobs j
            LEFT JOIN (
                SELECT job_id, run_date, run_time, run_status, message,
                       ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY run_date DESC, run_time DESC) AS rn
                FROM msdb.dbo.sysjobhistory
                WHERE step_id = 0
            ) h ON h.job_id = j.job_id AND h.rn = 1
            ORDER BY j.name
            """;

        SqlCommandGuard.AssertReadOnly(sql);

        try
        {
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(cancellationToken);
            await using var cmd = new SqlCommand(sql, conn);
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

            var sb = new StringBuilder();
            sb.AppendLine("SQL AGENT JOBS");
            sb.AppendLine(new string('─', 100));
            sb.AppendLine($"{"Job Name",-45} {"Enabled",-8} {"Last Run",-20} {"Status",-12} Message");
            sb.AppendLine(new string('─', 100));

            int count = 0;
            while (await reader.ReadAsync(cancellationToken))
            {
                count++;
                string name = reader.GetString(0);
                bool enabled = reader.GetByte(1) == 1;
                int? runDate = reader.IsDBNull(3) ? null : reader.GetInt32(3);
                int? runTime = reader.IsDBNull(4) ? null : reader.GetInt32(4);
                int? runStatus = reader.IsDBNull(5) ? null : reader.GetInt32(5);
                string? message = reader.IsDBNull(6) ? null : reader.GetString(6);

                string lastRun = runDate.HasValue ? FormatAgentDateTime(runDate.Value, runTime!.Value) : "(never run)";
                string status = runStatus.HasValue ? FormatRunStatus(runStatus.Value) : "-";
                string truncatedMessage = message != null && message.Length > 60 ? message[..60] + "..." : message ?? "";

                sb.AppendLine($"{name,-45} {(enabled ? "yes" : "no"),-8} {lastRun,-20} {status,-12} {truncatedMessage}");
            }

            sb.AppendLine(new string('─', 100));
            sb.AppendLine($"  {count} job(s)");
            return sb.ToString();
        }
        catch (Exception ex)
        {
            return $"ERROR: {ex.Message}";
        }
    }

    public async Task<string> GetFailingJobs(
        string database,
        CancellationToken cancellationToken = default)
    {
        if (!_databases.TryGetValue(database, out var connectionString))
            return UnknownDatabase(database);

        const string sql = """
            SELECT
                j.name,
                h.run_date,
                h.run_time,
                h.run_duration,
                h.message
            FROM msdb.dbo.sysjobs j
            JOIN msdb.dbo.sysjobhistory h ON h.job_id = j.job_id
            WHERE h.step_id = 0
                AND h.run_status = 0
                AND h.run_date >= CONVERT(int, CONVERT(varchar(8), DATEADD(day, -7, GETDATE()), 112))
            ORDER BY h.run_date DESC, h.run_time DESC
            """;

        SqlCommandGuard.AssertReadOnly(sql);

        try
        {
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(cancellationToken);
            await using var cmd = new SqlCommand(sql, conn);
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

            var sb = new StringBuilder();
            sb.AppendLine("FAILING SQL AGENT JOBS (last 7 days)");
            sb.AppendLine(new string('─', 100));

            int count = 0;
            while (await reader.ReadAsync(cancellationToken))
            {
                count++;
                string name = reader.GetString(0);
                int runDate = reader.GetInt32(1);
                int runTime = reader.GetInt32(2);
                int runDuration = reader.GetInt32(3);
                string message = reader.GetString(4);

                sb.AppendLine($"Job:      {name}");
                sb.AppendLine($"Run at:   {FormatAgentDateTime(runDate, runTime)}");
                sb.AppendLine($"Duration: {FormatDuration(runDuration)}");
                sb.AppendLine($"Error:    {message}");
                sb.AppendLine(new string('─', 100));
            }

            if (count == 0)
                sb.AppendLine("  No job failures in the last 7 days.");
            else
                sb.AppendLine($"  {count} failure(s)");

            return sb.ToString();
        }
        catch (Exception ex)
        {
            return $"ERROR: {ex.Message}";
        }
    }

    public async Task<string> GetJobHistory(
        string database,
        string jobName,
        int lastN = 20,
        CancellationToken cancellationToken = default)
    {
        if (!_databases.TryGetValue(database, out var connectionString))
            return UnknownDatabase(database);

        const string sql = """
            SELECT TOP (@lastN)
                h.step_id,
                h.step_name,
                h.run_date,
                h.run_time,
                h.run_status,
                h.run_duration,
                h.message
            FROM msdb.dbo.sysjobs j
            JOIN msdb.dbo.sysjobhistory h ON h.job_id = j.job_id
            WHERE j.name = @jobName
            ORDER BY h.run_date DESC, h.run_time DESC, h.step_id
            """;

        SqlCommandGuard.AssertReadOnly(sql);

        try
        {
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(cancellationToken);
            await using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@jobName", jobName);
            cmd.Parameters.AddWithValue("@lastN", lastN);

            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

            var sb = new StringBuilder();
            sb.AppendLine($"JOB HISTORY: {jobName}  (last {lastN} entries)");
            sb.AppendLine(new string('─', 100));
            sb.AppendLine($"{"Step",-6} {"Step Name",-35} {"Run At",-20} {"Status",-12} {"Duration",-12} Message");
            sb.AppendLine(new string('─', 100));

            int count = 0;
            while (await reader.ReadAsync(cancellationToken))
            {
                count++;
                int stepId = reader.GetInt32(0);
                string stepName = reader.IsDBNull(1) ? "(job outcome)" : reader.GetString(1);
                int runDate = reader.GetInt32(2);
                int runTime = reader.GetInt32(3);
                int runStatus = reader.GetInt32(4);
                int runDuration = reader.GetInt32(5);
                string message = reader.GetString(6);

                string label = stepId == 0 ? "JOB" : stepId.ToString(CultureInfo.InvariantCulture);
                string truncatedMessage = message.Length > 60 ? message[..60] + "..." : message;

                sb.AppendLine($"{label,-6} {stepName,-35} {FormatAgentDateTime(runDate, runTime),-20} {FormatRunStatus(runStatus),-12} {FormatDuration(runDuration),-12} {truncatedMessage}");
            }

            if (count == 0)
                sb.AppendLine($"  No history found for job '{jobName}'.");
            else
                sb.AppendLine(new string('─', 100));

            return sb.ToString();
        }
        catch (Exception ex)
        {
            return $"ERROR: {ex.Message}";
        }
    }

    private static string FormatAgentDateTime(int runDate, int runTime)
    {
        // runDate = YYYYMMDD, runTime = HHMMSS (both as int)
        string d = runDate.ToString(CultureInfo.InvariantCulture).PadLeft(8, '0');
        string t = runTime.ToString(CultureInfo.InvariantCulture).PadLeft(6, '0');
        return $"{d[..4]}-{d[4..6]}-{d[6..8]} {t[..2]}:{t[2..4]}:{t[4..6]}";
    }

    private static string FormatDuration(int runDuration)
    {
        // runDuration = HHMMSS as int
        string d = runDuration.ToString(CultureInfo.InvariantCulture).PadLeft(6, '0');
        return $"{d[..2]}h {d[2..4]}m {d[4..6]}s";
    }

    private static string FormatRunStatus(int status) => status switch
    {
        0 => "FAILED",
        1 => "Succeeded",
        2 => "Retry",
        3 => "Cancelled",
        5 => "Unknown",
        _ => status.ToString(CultureInfo.InvariantCulture)
    };

    public async Task<string> ListLinkedServers(
        string database,
        CancellationToken cancellationToken = default)
    {
        if (!_databases.TryGetValue(database, out var connectionString))
            return UnknownDatabase(database);

        const string sql = """
            SELECT
                s.name,
                s.product,
                s.provider,
                s.data_source,
                s.is_remote_login_enabled,
                s.modify_date
            FROM sys.servers s
            WHERE s.is_linked = 1
            ORDER BY s.name
            """;

        SqlCommandGuard.AssertReadOnly(sql);

        try
        {
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(cancellationToken);
            await using var cmd = new SqlCommand(sql, conn);
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

            var sb = new StringBuilder();
            sb.AppendLine("LINKED SERVERS");
            sb.AppendLine(new string('─', 100));
            sb.AppendLine($"{"Name",-30} {"Product",-15} {"Provider",-20} {"Data Source",-30} Login");
            sb.AppendLine(new string('─', 100));

            int count = 0;
            while (await reader.ReadAsync(cancellationToken))
            {
                count++;
                string name = reader.GetString(0);
                string product = reader.IsDBNull(1) ? "" : reader.GetString(1);
                string provider = reader.IsDBNull(2) ? "" : reader.GetString(2);
                string dataSource = reader.IsDBNull(3) ? "" : reader.GetString(3);
                bool loginEnabled = reader.GetBoolean(4);

                sb.AppendLine($"{name,-30} {product,-15} {provider,-20} {dataSource,-30} {(loginEnabled ? "yes" : "no")}");
            }

            if (count == 0)
                sb.AppendLine("  (no linked servers configured)");
            else
                sb.AppendLine(new string('─', 100));
            sb.AppendLine($"  {count} linked server(s)");

            return sb.ToString();
        }
        catch (Exception ex)
        {
            return $"ERROR: {ex.Message}";
        }
    }

    public async Task<string> FindLinkedServerUsage(
        string database,
        string? linkedServerName = null,
        CancellationToken cancellationToken = default)
    {
        if (!_databases.TryGetValue(database, out var connectionString))
            return UnknownDatabase(database);

        const string sql = """
            SELECT DISTINCT
                o.name AS ObjectName,
                o.type_desc AS ObjectType,
                o.modify_date
            FROM sys.sql_modules m
            JOIN sys.objects o ON o.object_id = m.object_id
            WHERE m.definition LIKE '%' + @pattern + '%'
            ORDER BY o.type_desc, o.name
            """;

        SqlCommandGuard.AssertReadOnly(sql);

        // If a specific linked server name is given search for it, otherwise search for the
        // four-part name pattern [server].[db]. that indicates any linked server call.
        string pattern = linkedServerName != null
            ? $"[{linkedServerName}]."
            : "].[";

        try
        {
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(cancellationToken);
            await using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@pattern", pattern);
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

            var sb = new StringBuilder();
            string header = linkedServerName != null
                ? $"OBJECTS REFERENCING LINKED SERVER [{linkedServerName}] in [{database}]"
                : $"OBJECTS WITH LINKED SERVER CALLS in [{database}]";
            sb.AppendLine(header);
            sb.AppendLine(new string('─', 80));
            sb.AppendLine($"{"Object",-45} {"Type",-25} Modified");
            sb.AppendLine(new string('─', 80));

            int count = 0;
            while (await reader.ReadAsync(cancellationToken))
            {
                count++;
                string name = reader.GetString(0);
                string type = reader.GetString(1);
                DateTime modified = reader.GetDateTime(2);
                sb.AppendLine($"{name,-45} {type,-25} {modified:yyyy-MM-dd}");
            }

            if (count == 0)
                sb.AppendLine("  (none found)");
            else
                sb.AppendLine(new string('─', 80));
            sb.AppendLine($"  {count} object(s)");

            return sb.ToString();
        }
        catch (Exception ex)
        {
            return $"ERROR: {ex.Message}";
        }
    }

    public async Task<string> ListServiceBroker(
        string database,
        CancellationToken cancellationToken = default)
    {
        if (!_databases.TryGetValue(database, out var connectionString))
            return UnknownDatabase(database);

        const string queueSql = """
            SELECT q.name, q.is_enqueue_enabled, q.is_activation_enabled,
                   OBJECT_SCHEMA_NAME(q.object_id) AS schema_name
            FROM sys.service_queues q
            WHERE q.is_ms_shipped = 0
            ORDER BY q.name
            """;

        const string serviceSql = """
            SELECT s.name, s.broker_instance
            FROM sys.services s
            WHERE s.name NOT LIKE 'http://schemas.microsoft.com/%'
            ORDER BY s.name
            """;

        SqlCommandGuard.AssertReadOnly(queueSql);
        SqlCommandGuard.AssertReadOnly(serviceSql);

        try
        {
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(cancellationToken);

            var sb = new StringBuilder();
            sb.AppendLine($"SERVICE BROKER in [{database}]");
            sb.AppendLine(new string('─', 70));

            sb.AppendLine("QUEUES");
            sb.AppendLine(new string('─', 70));

            int queueCount = 0;
            await using (var cmd = new SqlCommand(queueSql, conn))
            await using (var reader = await cmd.ExecuteReaderAsync(cancellationToken))
            {
                while (await reader.ReadAsync(cancellationToken))
                {
                    queueCount++;
                    string name = reader.GetString(0);
                    bool enqueue = reader.GetBoolean(1);
                    bool activation = reader.GetBoolean(2);
                    string schema = reader.GetString(3);
                    sb.AppendLine($"  [{schema}].[{name}]  enqueue={enqueue}  activation={activation}");
                }
            }

            if (queueCount == 0)
                sb.AppendLine("  (no user queues)");

            sb.AppendLine();
            sb.AppendLine("SERVICES");
            sb.AppendLine(new string('─', 70));

            int serviceCount = 0;
            await using (var cmd = new SqlCommand(serviceSql, conn))
            await using (var reader = await cmd.ExecuteReaderAsync(cancellationToken))
            {
                while (await reader.ReadAsync(cancellationToken))
                {
                    serviceCount++;
                    sb.AppendLine($"  {reader.GetString(0)}");
                }
            }

            if (serviceCount == 0)
                sb.AppendLine("  (no user services)");

            return sb.ToString();
        }
        catch (Exception ex)
        {
            return $"ERROR: {ex.Message}";
        }
    }

    public async Task<string> ListClrAssemblies(
        string database,
        CancellationToken cancellationToken = default)
    {
        if (!_databases.TryGetValue(database, out var connectionString))
            return UnknownDatabase(database);

        const string sql = """
            SELECT
                a.name,
                a.clr_name,
                a.permission_set_desc,
                a.create_date,
                a.modify_date
            FROM sys.assemblies a
            WHERE a.is_user_defined = 1
            ORDER BY a.name
            """;

        SqlCommandGuard.AssertReadOnly(sql);

        try
        {
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(cancellationToken);
            await using var cmd = new SqlCommand(sql, conn);
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

            var sb = new StringBuilder();
            sb.AppendLine($"CLR ASSEMBLIES in [{database}]");
            sb.AppendLine(new string('─', 90));
            sb.AppendLine($"{"Name",-35} {"CLR Name",-35} {"Permission",-15} Modified");
            sb.AppendLine(new string('─', 90));

            int count = 0;
            while (await reader.ReadAsync(cancellationToken))
            {
                count++;
                string name = reader.GetString(0);
                string clrName = reader.IsDBNull(1) ? "" : reader.GetString(1);
                string permission = reader.GetString(2);
                DateTime modified = reader.GetDateTime(4);
                sb.AppendLine($"{name,-35} {clrName,-35} {permission,-15} {modified:yyyy-MM-dd}");
            }

            if (count == 0)
                sb.AppendLine("  (no user-defined CLR assemblies)");
            else
                sb.AppendLine(new string('─', 90));
            sb.AppendLine($"  {count} assembly/assemblies");

            return sb.ToString();
        }
        catch (Exception ex)
        {
            return $"ERROR: {ex.Message}";
        }
    }

    public async Task<string> AnalyzeTopExpensiveQueries(
        string database,
        int top,
        CancellationToken cancellationToken = default)
    {
        if (!_databases.TryGetValue(database, out var connectionString))
            return UnknownDatabase(database);

        if (top < 1) top = 1;
        if (top > 100) top = 100;

        string sql = $"""
            SELECT TOP ({top})
                qs.execution_count,
                ROUND(qs.total_worker_time / 1000.0, 0) AS TotalCPUms,
                ROUND(qs.total_worker_time / qs.execution_count / 1000.0, 1) AS AvgCPUms,
                qs.total_logical_reads AS TotalReads,
                ROUND(CAST(qs.total_logical_reads AS float) / qs.execution_count, 0) AS AvgReads,
                qs.last_execution_time,
                SUBSTRING(st.text, (qs.statement_start_offset / 2) + 1,
                    ((CASE qs.statement_end_offset WHEN -1 THEN DATALENGTH(st.text)
                      ELSE qs.statement_end_offset END - qs.statement_start_offset) / 2) + 1) AS QueryText
            FROM sys.dm_exec_query_stats qs
            CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) st
            WHERE DB_NAME(st.dbid) = DB_NAME()
            ORDER BY qs.total_worker_time DESC
            """;

        SqlCommandGuard.AssertReadOnly(sql);

        try
        {
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(cancellationToken);
            await using var cmd = new SqlCommand(sql, conn);
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

            var sb = new StringBuilder();
            sb.AppendLine($"TOP {top} EXPENSIVE QUERIES (by total CPU): [{database}]");
            sb.AppendLine(new string('─', 100));

            int count = 0;
            while (await reader.ReadAsync(cancellationToken))
            {
                count++;
                long execs = reader.GetInt64(0);
                double totalCpu = reader.GetDouble(1);
                double avgCpu = reader.GetDouble(2);
                long totalReads = reader.GetInt64(3);
                double avgReads = reader.GetDouble(4);
                DateTime lastRun = reader.GetDateTime(5);
                string text = reader.IsDBNull(6) ? "(unavailable)" : reader.GetString(6).Trim();
                string preview = text.Length > 200 ? text[..197] + "..." : text;

                sb.AppendLine($"  #{count}");
                sb.AppendLine($"    Executions: {execs:N0}   Total CPU: {totalCpu:N0}ms   Avg CPU: {avgCpu:N1}ms");
                sb.AppendLine($"    Total reads: {totalReads:N0}   Avg reads: {avgReads:N0}   Last run: {lastRun:yyyy-MM-dd HH:mm}");
                sb.AppendLine($"    {preview}");
                sb.AppendLine();
            }

            if (count == 0)
                sb.AppendLine("  (no query stats available — data resets on server restart)");

            return sb.ToString();
        }
        catch (Exception ex)
        {
            return $"ERROR: {ex.Message}";
        }
    }

    public async Task<string> AnalyzeWaitStats(
        string database,
        CancellationToken cancellationToken = default)
    {
        if (!_databases.TryGetValue(database, out var connectionString))
            return UnknownDatabase(database);

        const string sql = """
            SELECT TOP 20
                wait_type,
                wait_time_ms,
                signal_wait_time_ms,
                waiting_tasks_count,
                ROUND(100.0 * wait_time_ms / NULLIF(SUM(wait_time_ms) OVER(), 0), 2) AS PctTotal
            FROM sys.dm_os_wait_stats
            WHERE wait_type NOT IN (
                'SLEEP_TASK','SLEEP_SYSTEMTASK','SLEEP_DBSTARTUP','SLEEP_DBTASK',
                'SLEEP_TEMPDBSTARTUP','SNI_HTTP_ACCEPT','DISPATCHER_QUEUE_SEMAPHORE',
                'BROKER_TO_FLUSH','BROKER_TASK_STOP','CLR_AUTO_EVENT','CLR_MANUAL_EVENT',
                'FT_IFTS_SCHEDULER_IDLE_WAIT','LAZYWRITER_SLEEP','LOGMGR_QUEUE',
                'ONDEMAND_TASK_QUEUE','REQUEST_FOR_DEADLOCK_SEARCH','RESOURCE_QUEUE',
                'SERVER_IDLE_CHECK','SLEEP_MASTERDBREADY','SLEEP_MASTERMDREADY',
                'SLEEP_MASTERUPGRADED','SLEEP_MSDBSTARTUP','SP_SERVER_DIAGNOSTICS_SLEEP',
                'SQLTRACE_BUFFER_FLUSH','SQLTRACE_INCREMENTAL_FLUSH_SLEEP',
                'WAIT_XTP_OFFLINE_CKPT_NEW_LOG','WAITFOR','XE_DISPATCHER_WAIT',
                'XE_TIMER_EVENT','BROKER_EVENTHANDLER','CHECKPOINT_QUEUE',
                'DBMIRROR_EVENTS_QUEUE','SQLTRACE_WAIT_ENTRIES','HADR_WORK_QUEUE',
                'HADR_FILESTREAM_IOMGR_IOCOMPLETION')
            ORDER BY wait_time_ms DESC
            """;

        SqlCommandGuard.AssertReadOnly(sql);

        try
        {
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(cancellationToken);
            await using var cmd = new SqlCommand(sql, conn);
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

            var sb = new StringBuilder();
            sb.AppendLine($"WAIT STATS (server-level, top 20 non-idle waits)");
            sb.AppendLine(new string('─', 90));
            sb.AppendLine($"{"Wait Type",-40} {"Wait ms",12} {"Signal ms",10} {"Tasks",10} {"% Total",8}");
            sb.AppendLine(new string('─', 90));

            int count = 0;
            while (await reader.ReadAsync(cancellationToken))
            {
                count++;
                string waitType = reader.GetString(0);
                long waitMs = reader.GetInt64(1);
                long signalMs = reader.GetInt64(2);
                long tasks = reader.GetInt64(3);
                double pct = reader.GetDouble(4);
                sb.AppendLine($"{waitType,-40} {waitMs,12:N0} {signalMs,10:N0} {tasks,10:N0} {pct,7:F2}%");
            }

            if (count == 0)
                sb.AppendLine("  (no wait stats available)");
            else
                sb.AppendLine(new string('─', 90));

            sb.AppendLine("  NOTE: wait stats are cumulative since last server restart.");
            return sb.ToString();
        }
        catch (Exception ex)
        {
            return $"ERROR: {ex.Message}";
        }
    }
}
