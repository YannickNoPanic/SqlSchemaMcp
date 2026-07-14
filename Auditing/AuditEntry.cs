using System;

namespace SqlSchemaMcp.Auditing;

public sealed record AuditEntry(
    DateTimeOffset TimestampUtc,
    string Tool,
    string Database,
    string ParametersSummary,
    long DurationMs,
    bool Success);
