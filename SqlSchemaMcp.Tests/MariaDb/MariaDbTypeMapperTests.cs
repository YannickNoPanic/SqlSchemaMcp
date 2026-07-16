using FluentAssertions;
using SqlSchemaMcp.Abstractions;
using SqlSchemaMcp.MariaDb.Data;
using Xunit;

namespace SqlSchemaMcp.Tests.MariaDb;

public sealed class MariaDbTypeMapperTests
{
    [Theory]
    [InlineData("int", ColumnTypeCategory.Integer)]
    [InlineData("char", ColumnTypeCategory.Guid)]
    [InlineData("varchar", ColumnTypeCategory.Text)]
    [InlineData("tinyint", ColumnTypeCategory.Boolean)]
    [InlineData("datetime", ColumnTypeCategory.Temporal)]
    [InlineData("decimal", ColumnTypeCategory.Decimal)]
    [InlineData("json", ColumnTypeCategory.Other)]
    public void ToCategory_MapsMariaDbTypes(string dataType, ColumnTypeCategory expected)
    {
        var result = MariaDbTypeMapper.ToCategory(dataType);

        result.Should().Be(expected);
    }
}
