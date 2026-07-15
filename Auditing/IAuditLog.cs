using System;
using System.Threading.Tasks;

namespace SqlSchemaMcp.Auditing;

public interface IAuditLog
{
    /// <summary>
    /// Records the invocation of a tool and returns the tool's result. Timing and outcome
    /// are captured even when the body throws (the exception is re-thrown after recording).
    /// Success reflects both the absence of a thrown exception and that the result does not
    /// begin with the ERROR: or UNSUPPORTED: sentinel used by this codebase's Result-as-string convention.
    /// </summary>
    Task<string> Invoke(string tool, string database, string parametersSummary, Func<Task<string>> body);
}

public static class AuditSummary
{
    public static string Truncate(string? value, int max = 200) =>
        string.IsNullOrEmpty(value) ? "" : value.Length <= max ? value : value[..max] + "...";
}
