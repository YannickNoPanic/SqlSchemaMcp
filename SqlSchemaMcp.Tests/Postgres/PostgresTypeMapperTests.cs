using FluentAssertions;
using SqlSchemaMcp.Abstractions;
using SqlSchemaMcp.Postgres.Data;
using Xunit;

namespace SqlSchemaMcp.Tests.Postgres;

public sealed class PostgresTypeMapperTests
{
    [Theory]
    [InlineData("integer", ColumnTypeCategory.Integer)]
    [InlineData("uuid", ColumnTypeCategory.Guid)]
    [InlineData("text", ColumnTypeCategory.Text)]
    [InlineData("boolean", ColumnTypeCategory.Boolean)]
    [InlineData("timestamp with time zone", ColumnTypeCategory.Temporal)]
    [InlineData("numeric", ColumnTypeCategory.Decimal)]
    [InlineData("jsonb", ColumnTypeCategory.Other)]
    public void ToCategory_MapsPostgresTypes(string dataType, ColumnTypeCategory expected)
    {
        var result = PostgresTypeMapper.ToCategory(dataType);

        result.Should().Be(expected);
    }
}
