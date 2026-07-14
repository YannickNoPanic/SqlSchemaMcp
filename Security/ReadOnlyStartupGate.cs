using System.Collections.Generic;
using System.Linq;
using SqlSchemaMcp.Configuration;

namespace SqlSchemaMcp.Security;

public sealed record GateDecision(
    bool ShouldStart,
    IReadOnlyList<string> Errors,
    IReadOnlyList<string> Warnings);

public static class ReadOnlyStartupGate
{
    public static GateDecision Evaluate(IReadOnlyList<LoginPermissionResult> probes, SecurityOptions options)
    {
        var errors = new List<string>();
        var warnings = new List<string>();

        if (!options.VerifyLoginsAtStartup)
            return new GateDecision(ShouldStart: true, errors, warnings);

        foreach (var probe in probes)
        {
            if (!probe.Reachable)
            {
                warnings.Add($"Read-only status of login for database '{probe.Database}' could not be verified (unreachable at startup).");
                continue;
            }

            if (!probe.CanWrite)
                continue;

            var grants = string.Join(", ", probe.GrantedWrites);
            if (options.AllowWritableLogin)
                warnings.Add($"Login for database '{probe.Database}' can WRITE ({grants}). Continuing because Security:AllowWritableLogin is true.");
            else
                errors.Add($"Login for database '{probe.Database}' can WRITE ({grants}). Refusing to start. Use a read-only login (db_datareader only) or set Security:AllowWritableLogin=true to override.");
        }

        return new GateDecision(ShouldStart: errors.Count == 0, errors, warnings);
    }
}
