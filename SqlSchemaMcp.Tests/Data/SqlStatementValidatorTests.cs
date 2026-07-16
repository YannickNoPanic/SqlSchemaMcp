using FluentAssertions;
using SqlSchemaMcp.SqlServer.Data;
using Xunit;

namespace SqlSchemaMcp.Tests.Data;

public sealed class SqlStatementValidatorTests
{
    [Theory]
    [InlineData("SELECT * FROM dbo.Orders")]
    [InlineData("SELECT TOP 10 Id, Name FROM dbo.Customers WHERE IsActive = 1")]
    [InlineData("WITH cte AS (SELECT Id FROM dbo.Orders) SELECT * FROM cte")]
    [InlineData("select o.Id from dbo.Orders o join dbo.Lines l on l.OrderId = o.Id")]
    public void Validate_PlainSelect_IsAllowed(string sql)
    {
        var result = SqlStatementValidator.Validate(sql);

        result.IsAllowed.Should().BeTrue(because: sql);
        result.Reason.Should().BeNull();
    }

    [Theory]
    [InlineData("SELECT * INTO dbo.Copy FROM dbo.Orders")]
    [InlineData("INSERT INTO dbo.Orders (Id) VALUES (1)")]
    [InlineData("UPDATE dbo.Orders SET Name = 'x'")]
    [InlineData("DELETE FROM dbo.Orders")]
    [InlineData("DROP TABLE dbo.Orders")]
    [InlineData("TRUNCATE TABLE dbo.Orders")]
    [InlineData("ALTER TABLE dbo.Orders ADD X int")]
    [InlineData("CREATE TABLE dbo.X (Id int)")]
    [InlineData("EXEC sp_who")]
    [InlineData("MERGE dbo.Orders AS t USING dbo.Src AS s ON t.Id = s.Id WHEN MATCHED THEN DELETE;")]
    [InlineData("GRANT SELECT ON dbo.Orders TO someone")]
    [InlineData("WAITFOR DELAY '00:00:10'")]
    public void Validate_WriteOrControlStatement_IsRejected(string sql)
    {
        var result = SqlStatementValidator.Validate(sql);

        result.IsAllowed.Should().BeFalse(because: sql);
        result.Reason.Should().NotBeNullOrEmpty();
    }

    [Theory]
    [InlineData("SELECT * FROM OPENQUERY(link, 'DELETE FROM x')")]
    [InlineData("SELECT * FROM OPENROWSET('SQLNCLI', 'server';'u';'p', 'SELECT 1')")]
    [InlineData("SELECT * FROM OPENDATASOURCE('SQLNCLI', 'x').db.dbo.t")]
    public void Validate_OpenRowsetFamily_IsRejected(string sql)
    {
        var result = SqlStatementValidator.Validate(sql);

        result.IsAllowed.Should().BeFalse(because: sql);
        result.Reason.Should().Contain("OPEN");
    }

    [Fact]
    public void Validate_MultipleStatements_IsRejected()
    {
        var sql = "SELECT 1; DROP TABLE dbo.Orders";

        var result = SqlStatementValidator.Validate(sql);

        result.IsAllowed.Should().BeFalse();
        result.Reason.Should().Contain("single");
    }

    [Fact]
    public void Validate_Unparseable_IsRejected()
    {
        var sql = "SELECT FROM WHERE )(";

        var result = SqlStatementValidator.Validate(sql);

        result.IsAllowed.Should().BeFalse();
        result.Reason.Should().Contain("parse");
    }

    [Fact]
    public void Validate_Empty_IsRejected()
    {
        var result = SqlStatementValidator.Validate("   ");

        result.IsAllowed.Should().BeFalse();
    }
}
