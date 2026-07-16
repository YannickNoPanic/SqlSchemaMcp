using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using SqlSchemaMcp.Abstractions.Security;

namespace SqlSchemaMcp.SqlServer.Security;

/// <summary>
/// Probes a login's effective write capability. A login is considered writable if it is a
/// member of any writing/owning/DDL role, is sysadmin, or holds any explicit DB-scoped write
/// permission. Read intent only — this query performs no writes.
/// </summary>
public sealed class SqlServerPermissionProbe : IPermissionProbe
{
    private const string Sql = """
        SELECT
            CAST(ISNULL(IS_SRVROLEMEMBER('sysadmin'), 0) AS int) AS IsSysadmin,
            CAST(ISNULL(IS_ROLEMEMBER('db_owner'), 0) AS int) AS IsDbOwner,
            CAST(ISNULL(IS_ROLEMEMBER('db_datawriter'), 0) AS int) AS IsDataWriter,
            CAST(ISNULL(IS_ROLEMEMBER('db_ddladmin'), 0) AS int) AS IsDdlAdmin,
            (SELECT COUNT(*) FROM sys.fn_my_permissions(NULL, 'DATABASE')
             WHERE permission_name IN ('INSERT','UPDATE','DELETE','ALTER','CONTROL','CREATE TABLE')) AS WriteGrants
        """;

    public async Task<LoginPermissionResult> ProbeAsync(string database, string connectionString, CancellationToken ct)
    {
        try
        {
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(ct);
            await using var cmd = new SqlCommand(Sql, conn);
            await using var reader = await cmd.ExecuteReaderAsync(ct);

            var grants = new List<string>();
            if (await reader.ReadAsync(ct))
            {
                if (reader.GetInt32(0) == 1) grants.Add("sysadmin");
                if (reader.GetInt32(1) == 1) grants.Add("db_owner");
                if (reader.GetInt32(2) == 1) grants.Add("db_datawriter");
                if (reader.GetInt32(3) == 1) grants.Add("db_ddladmin");
                if (reader.GetInt32(4) > 0) grants.Add("explicit write grants");
            }

            return new LoginPermissionResult(database, Reachable: true, CanWrite: grants.Count > 0, grants);
        }
        catch (Exception)
        {
            // Unreachable / auth failure at startup — the gate treats this as "unverified", not "writable".
            return new LoginPermissionResult(database, Reachable: false, CanWrite: false, []);
        }
    }
}
