using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace SqlSchemaMcp.Security;

public sealed record LoginPermissionResult(
    string Database,
    bool Reachable,
    bool CanWrite,
    IReadOnlyList<string> GrantedWrites);

public interface IPermissionProbe
{
    Task<LoginPermissionResult> ProbeAsync(string database, string connectionString, CancellationToken ct);
}
