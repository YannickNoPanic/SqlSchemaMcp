using System;
using System.IO;
using System.Text.Json;
using System.Threading.Tasks;
using FluentAssertions;
using Microsoft.Extensions.Options;
using SqlSchemaMcp.Auditing;
using SqlSchemaMcp.Configuration;
using Xunit;

namespace SqlSchemaMcp.Tests.Auditing;

public sealed class FileAuditLogTests : IDisposable
{
    private readonly string _path = Path.Combine(Path.GetTempPath(), $"audit-{Guid.NewGuid():N}.jsonl");

    [Fact]
    public async Task Invoke_SuccessfulBody_WritesEntryAndReturnsResult()
    {
        var sut = new FileAuditLog(Options.Create(new AuditOptions { Enabled = true, Path = _path }));

        var result = await sut.Invoke("ExecuteQuery", "poc", "sql=SELECT 1", () => Task.FromResult("rows: 1"));

        result.Should().Be("rows: 1");
        var line = (await File.ReadAllLinesAsync(_path))[0];
        var entry = JsonSerializer.Deserialize<AuditEntry>(line, JsonSerializerOptions.Web);
        entry!.Tool.Should().Be("ExecuteQuery");
        entry.Database.Should().Be("poc");
        entry.Success.Should().BeTrue();
    }

    [Fact]
    public async Task Invoke_BodyReturnsErrorString_RecordsFailureWithoutThrowing()
    {
        var sut = new FileAuditLog(Options.Create(new AuditOptions { Enabled = true, Path = _path }));

        var result = await sut.Invoke("ExecuteQuery", "poc", "sql=bad", () => Task.FromResult("ERROR: Unknown database 'poc'."));

        result.Should().Be("ERROR: Unknown database 'poc'.");
        var line = (await File.ReadAllLinesAsync(_path))[0];
        var entry = JsonSerializer.Deserialize<AuditEntry>(line, JsonSerializerOptions.Web);
        entry!.Success.Should().BeFalse();
    }

    [Fact]
    public async Task Invoke_BodyThrows_RecordsFailureAndRethrows()
    {
        var sut = new FileAuditLog(Options.Create(new AuditOptions { Enabled = true, Path = _path }));

        var act = async () => await sut.Invoke("ExecuteQuery", "poc", "sql=bad", () => throw new InvalidOperationException("boom"));

        await act.Should().ThrowAsync<InvalidOperationException>();
        var line = (await File.ReadAllLinesAsync(_path))[0];
        var entry = JsonSerializer.Deserialize<AuditEntry>(line, JsonSerializerOptions.Web);
        entry!.Success.Should().BeFalse();
    }

    [Fact]
    public async Task Invoke_Disabled_DoesNotWriteFile()
    {
        var sut = new FileAuditLog(Options.Create(new AuditOptions { Enabled = false, Path = _path }));

        await sut.Invoke("ExecuteQuery", "poc", "sql=SELECT 1", () => Task.FromResult("ok"));

        File.Exists(_path).Should().BeFalse();
    }

    public void Dispose()
    {
        if (File.Exists(_path)) File.Delete(_path);
    }
}
