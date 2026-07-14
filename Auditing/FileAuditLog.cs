using System;
using System.Diagnostics;
using System.IO;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using SqlSchemaMcp.Configuration;

namespace SqlSchemaMcp.Auditing;

public sealed class FileAuditLog : IAuditLog
{
    private readonly AuditOptions _options;
    private readonly string _path;
    private readonly SemaphoreSlim _writeLock = new(1, 1);

    public FileAuditLog(IOptions<AuditOptions> options)
    {
        _options = options.Value;
        _path = ResolvePath(_options.Path);
    }

    public async Task<string> Invoke(string tool, string database, string parametersSummary, Func<Task<string>> body)
    {
        if (!_options.Enabled)
            return await body();

        var stopwatch = Stopwatch.StartNew();
        var success = false;
        try
        {
            var result = await body();
            success = true;
            return result;
        }
        finally
        {
            stopwatch.Stop();
            await WriteAsync(new AuditEntry(
                DateTimeOffset.UtcNow, tool, database, parametersSummary, stopwatch.ElapsedMilliseconds, success));
        }
    }

    private async Task WriteAsync(AuditEntry entry)
    {
        var line = JsonSerializer.Serialize(entry, JsonSerializerOptions.Web) + Environment.NewLine;
        await _writeLock.WaitAsync();
        try
        {
            // FileShare.ReadWrite so a concurrent process (stdio-per-session) can also append.
            await using var stream = new FileStream(_path, FileMode.Append, FileAccess.Write, FileShare.ReadWrite);
            await using var writer = new StreamWriter(stream);
            await writer.WriteAsync(line);
        }
        catch
        {
            // Auditing must never take down a query. Swallow write failures silently;
            // the diagnostic logger (Phase 3) still captures operational errors.
        }
        finally
        {
            _writeLock.Release();
        }
    }

    private static string ResolvePath(string? configured)
    {
        if (!string.IsNullOrWhiteSpace(configured))
            return configured;

        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir != null)
        {
            if (dir.GetFiles("*.csproj").Length > 0)
                return Path.Combine(dir.FullName, "audit-log.jsonl");
            dir = dir.Parent;
        }
        return Path.Combine(AppContext.BaseDirectory, "audit-log.jsonl");
    }
}
