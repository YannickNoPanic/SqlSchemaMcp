using FluentAssertions;
using SqlSchemaMcp.Configuration;
using Xunit;

namespace SqlSchemaMcp.Tests.Configuration;

public sealed class SqlServerOptionsTests
{
    private const string DatabaseName = "azure";
    private const string SecretVariableAssignment = "sqlmcp_azure_connection_string=Server=tcp:example.database.windows.net;";
    private const string CommaSeparatedEncrypt = "Server=tcp:example.database.windows.net;Database=Example;User Id=sqlschema_ro;Password=secret,Encrypt=True;";
    private const string ValidConnectionString = "Server=tcp:example.database.windows.net;Database=Example;User Id=sqlschema_ro;Password=secret;";

    [Fact]
    public void GetConfigurationErrors_ConnectionStringStartsWithEnvironmentVariableAssignment_ReturnsActionableError()
    {
        var options = new SqlServerOptions
        {
            Databases =
            {
                [DatabaseName] = SecretVariableAssignment
            }
        };

        var result = options.GetConfigurationErrors();

        result.Should().ContainSingle()
            .Which.Should().Contain("SQLMCP_SqlServer__Databases__azure");
        result.Single().Should().Contain("must contain the SQL Server connection string value directly");
    }

    [Fact]
    public void GetConfigurationErrors_ValidConnectionString_ReturnsNoErrors()
    {
        var options = new SqlServerOptions
        {
            Databases =
            {
                [DatabaseName] = ValidConnectionString
            }
        };

        var result = options.GetConfigurationErrors();

        result.Should().BeEmpty();
    }

    [Fact]
    public void GetConfigurationErrors_ConnectionStringUsesCommaBeforeEncrypt_ReturnsActionableError()
    {
        var options = new SqlServerOptions
        {
            Databases =
            {
                [DatabaseName] = CommaSeparatedEncrypt
            }
        };

        var result = options.GetConfigurationErrors();

        result.Should().ContainSingle()
            .Which.Should().Contain("uses a comma before 'Encrypt='");
    }
}
