using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using SqlSchemaMcp.MariaDb.Configuration;
using SqlSchemaMcp.MariaDb.Data;
using Xunit;

namespace SqlSchemaMcp.Tests.MariaDb;

public sealed class MariaDbSafeErrorTests
{
    private sealed class TestQueries(IOptions<MariaDbEngineOptions> options)
        : MariaDbQueryBase(options, NullLogger<TestQueries>.Instance)
    {
        public string CallSafeError(Exception ex) => SafeError(ex);
    }

    [Fact]
    public void SafeError_WithSensitiveException_ReturnsGenericMessage()
    {
        var sut = new TestQueries(Options.Create(new MariaDbEngineOptions()));
        var leaky = new InvalidOperationException("Access denied for user 'root' on server 'prod-maria-01'.");

        var result = sut.CallSafeError(leaky);

        result.Should().Be("ERROR: the query failed. Check the server log for details.");
        result.Should().NotContain("prod-maria-01");
        result.Should().NotContain("root");
    }

    [Fact]
    public async Task ListTables_InvalidConnectionString_ReturnsGenericError()
    {
        var options = Options.Create(new MariaDbEngineOptions
        {
            Databases =
            {
                ["legacy"] = "not a connection string"
            }
        });
        var sut = new MariaDbSchema(options, NullLogger<MariaDbSchema>.Instance);

        var result = await sut.ListTables("legacy", null, null, CancellationToken.None);

        result.Should().Be("ERROR: the query failed. Check the server log for details.");
    }
}
