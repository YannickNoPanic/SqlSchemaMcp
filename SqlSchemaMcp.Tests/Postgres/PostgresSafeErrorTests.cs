using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using SqlSchemaMcp.Postgres.Configuration;
using SqlSchemaMcp.Postgres.Data;
using Xunit;

namespace SqlSchemaMcp.Tests.Postgres;

public sealed class PostgresSafeErrorTests
{
    private sealed class TestQueries(IOptions<PostgresEngineOptions> options)
        : PostgresQueryBase(options, NullLogger<TestQueries>.Instance)
    {
        public string CallSafeError(Exception ex) => SafeError(ex);
    }

    [Fact]
    public void SafeError_WithSensitiveException_ReturnsGenericMessage()
    {
        var sut = new TestQueries(Options.Create(new PostgresEngineOptions()));
        var leaky = new InvalidOperationException("Login failed for user 'postgres' on server 'prod-pg-01'.");

        var result = sut.CallSafeError(leaky);

        result.Should().Be("ERROR: the query failed. Check the server log for details.");
        result.Should().NotContain("prod-pg-01");
        result.Should().NotContain("postgres");
    }

    [Fact]
    public async Task ListTables_InvalidConnectionString_ReturnsGenericError()
    {
        var options = Options.Create(new PostgresEngineOptions
        {
            Databases =
            {
                ["reporting"] = "not a connection string"
            }
        });
        var sut = new PostgresSchema(options, NullLogger<PostgresSchema>.Instance);

        var result = await sut.ListTables("reporting", null, null, CancellationToken.None);

        result.Should().Be("ERROR: the query failed. Check the server log for details.");
    }
}
