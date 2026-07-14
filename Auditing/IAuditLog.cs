using System;
using System.Threading.Tasks;

namespace SqlSchemaMcp.Auditing;

public interface IAuditLog
{
    /// <summary>
    /// Records the invocation of a tool and returns the tool's result. Timing and outcome
    /// are captured even when the body throws (the exception is re-thrown after recording).
    /// </summary>
    Task<string> Invoke(string tool, string database, string parametersSummary, Func<Task<string>> body);
}
