using System.Globalization;
using System.Text;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;
using SqlSchemaMcp.Configuration;

namespace SqlSchemaMcp.Data;

public sealed class SecurityQueries(IOptions<SqlServerOptions> options) : SqlQueryBase(options)
{
    public async Task<string> ListDatabaseUsers(
        string database,
        CancellationToken cancellationToken = default)
    {
        if (!_databases.TryGetValue(database, out var connectionString))
            return UnknownDatabase(database);

        const string sql = """
            SELECT
                dp.name,
                dp.type_desc,
                dp.create_date,
                STUFF((
                    SELECT ', ' + r2.name
                    FROM sys.database_role_members rm2
                    JOIN sys.database_principals r2
                        ON r2.principal_id = rm2.role_principal_id AND r2.type = 'R'
                    WHERE rm2.member_principal_id = dp.principal_id
                    ORDER BY r2.name
                    FOR XML PATH(''), TYPE).value('.', 'nvarchar(max)'), 1, 2, '') AS Roles
            FROM sys.database_principals dp
            WHERE dp.type IN ('S', 'U', 'G', 'E', 'X')
                AND dp.name NOT IN ('dbo', 'guest', 'INFORMATION_SCHEMA', 'sys')
            ORDER BY dp.type_desc, dp.name
            """;

        try
        {
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(cancellationToken);
            await using var cmd = new SqlCommand(sql, conn);
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

            var sb = new StringBuilder();
            sb.AppendLine($"DATABASE USERS: [{database}]");
            sb.AppendLine(new string('─', 80));
            sb.AppendLine($"{"Name",-35} {"Type",-25} {"Created",-12} Roles");
            sb.AppendLine(new string('─', 80));

            int count = 0;
            while (await reader.ReadAsync(cancellationToken))
            {
                count++;
                string name = reader.GetString(0);
                string type = reader.GetString(1);
                DateTime created = reader.GetDateTime(2);
                string roles = reader.IsDBNull(3) ? "(none)" : reader.GetString(3);
                sb.AppendLine($"{name,-35} {type,-25} {created:yyyy-MM-dd}  {roles}");
            }

            if (count == 0)
                sb.AppendLine("  (no users found)");
            else
                sb.AppendLine(new string('─', 80));

            sb.AppendLine($"  {count} user(s)");
            return sb.ToString();
        }
        catch (Exception ex)
        {
            return $"ERROR: {ex.Message}";
        }
    }

    public async Task<string> ListObjectPermissions(
        string database,
        CancellationToken cancellationToken = default)
    {
        if (!_databases.TryGetValue(database, out var connectionString))
            return UnknownDatabase(database);

        const string sql = """
            SELECT
                OBJECT_SCHEMA_NAME(dp.major_id) AS SchemaName,
                OBJECT_NAME(dp.major_id) AS ObjectName,
                o.type_desc AS ObjectType,
                pr.name AS Principal,
                dp.permission_name,
                dp.state_desc AS GrantOrDeny
            FROM sys.database_permissions dp
            JOIN sys.database_principals pr ON pr.principal_id = dp.grantee_principal_id
            JOIN sys.objects o ON o.object_id = dp.major_id
            WHERE dp.class = 1
                AND dp.major_id > 0
                AND pr.name NOT IN ('dbo', 'public')
            ORDER BY OBJECT_NAME(dp.major_id), pr.name, dp.permission_name
            """;

        try
        {
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(cancellationToken);
            await using var cmd = new SqlCommand(sql, conn);
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

            var sb = new StringBuilder();
            sb.AppendLine($"OBJECT PERMISSIONS: [{database}]");
            sb.AppendLine(new string('─', 100));
            sb.AppendLine($"{"Object",-40} {"Type",-20} {"Principal",-25} {"Permission",-20} State");
            sb.AppendLine(new string('─', 100));

            int count = 0;
            while (await reader.ReadAsync(cancellationToken))
            {
                count++;
                string schema = reader.IsDBNull(0) ? "" : reader.GetString(0);
                string obj = reader.IsDBNull(1) ? "(unknown)" : reader.GetString(1);
                string objType = reader.GetString(2);
                string principal = reader.GetString(3);
                string permission = reader.GetString(4);
                string state = reader.GetString(5);

                sb.AppendLine($"{$"[{schema}].[{obj}]",-40} {objType,-20} {principal,-25} {permission,-20} {state}");
            }

            if (count == 0)
                sb.AppendLine("  (no explicit object permissions found)");
            else
                sb.AppendLine(new string('─', 100));

            sb.AppendLine($"  {count} permission(s)");
            return sb.ToString();
        }
        catch (Exception ex)
        {
            return $"ERROR: {ex.Message}";
        }
    }
}
