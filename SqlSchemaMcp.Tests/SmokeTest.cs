using FluentAssertions;
using Xunit;

namespace SqlSchemaMcp.Tests;

public sealed class SmokeTest
{
    [Fact]
    public void TestHarness_Runs_Passes()
    {
        var actual = 1 + 1;

        actual.Should().Be(2);
    }
}
