using System;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using SqlSchemaMcp.SqlServer.Configuration;
using SqlSchemaMcp.SqlServer.Data;
using Xunit;

namespace SqlSchemaMcp.Tests.Data;

public sealed class SafeErrorTests
{
    private sealed class TestQueries(IOptions<SqlServerEngineOptions> options)
        : SqlQueryBase(options, NullLogger<TestQueries>.Instance)
    {
        public string CallSafeError(Exception ex) => SafeError(ex);
    }

    [Fact]
    public void SafeError_WithSensitiveException_ReturnsGenericMessage()
    {
        var options = Options.Create(new SqlServerEngineOptions());
        var sut = new TestQueries(options);
        var leaky = new InvalidOperationException("Login failed for user 'sa' on server 'prod-sql-01'.");

        var result = sut.CallSafeError(leaky);

        result.Should().NotContain("prod-sql-01");
        result.Should().NotContain("sa");
        result.Should().StartWith("ERROR:");
    }
}
