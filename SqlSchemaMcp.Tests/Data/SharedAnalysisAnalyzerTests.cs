using FluentAssertions;
using SqlSchemaMcp.Abstractions;
using SqlSchemaMcp.Data.SharedAnalysis;
using Xunit;

namespace SqlSchemaMcp.Tests.Data;

public sealed class SharedAnalysisAnalyzerTests
{
    [Fact]
    public void NamingAnalyzer_WithViolations_BuildsExpectedSections()
    {
        var snapshot = new SchemaSnapshot(
            [new SchemaObject("TABLE", "dbo", "tbl_Customers_v2")],
            [new SchemaColumn("dbo", "tbl_Customers_v2", "CUSTOMER_ID", ColumnTypeCategory.Integer, "int", "NO")],
            EmptySet(),
            EmptySet(),
            EmptySet());

        var result = NamingAnalyzer.Build("poc", snapshot);

        result.Should().Contain("NAMING CONVENTION ANALYSIS: [poc]");
        result.Should().Contain("HUNGARIAN PREFIXES (objects) (1)");
        result.Should().Contain("VERSION SUFFIXES (_v2, _OLD, _FINAL, etc.) (1)");
        result.Should().Contain("ALL_CAPS COLUMNS (1)");
    }

    [Fact]
    public void MissingForeignKeyAnalyzer_FkPatternWithoutConstraint_ReportsCandidate()
    {
        var snapshot = new SchemaSnapshot(
            [],
            [new SchemaColumn("dbo", "Orders", "CustomerId", ColumnTypeCategory.Integer, "int", "NO")],
            EmptySet(),
            EmptySet(),
            EmptySet());

        var result = MissingForeignKeyAnalyzer.Build("poc", snapshot);

        result.Should().Contain("[dbo].[Orders].CustomerId (int)");
        result.Should().Contain("1 potential missing FK(s)");
    }

    [Fact]
    public void MissingForeignKeyAnalyzer_LowercaseIdSuffixNoUnderscore_ReportsCandidate()
    {
        // Regression test: original SQL Server implementation used a case-insensitive
        // LIKE '%Id' match, which also matches a lowercase "id" suffix with no underscore.
        var snapshot = new SchemaSnapshot(
            [],
            [new SchemaColumn("dbo", "Orders", "Tenantid", ColumnTypeCategory.Integer, "int", "NO")],
            EmptySet(),
            EmptySet(),
            EmptySet());

        var result = MissingForeignKeyAnalyzer.Build("poc", snapshot);

        result.Should().Contain("[dbo].[Orders].Tenantid (int)");
        result.Should().Contain("1 potential missing FK(s)");
    }

    [Fact]
    public void MissingForeignKeyAnalyzer_TinyIntId_IsNotCandidate()
    {
        var snapshot = new SchemaSnapshot(
            [],
            [new SchemaColumn("dbo", "Orders", "StatusId", ColumnTypeCategory.Other, "tinyint", "NO")],
            EmptySet(),
            EmptySet(),
            EmptySet());

        var result = MissingForeignKeyAnalyzer.Build("poc", snapshot);

        result.Should().Contain("0 potential missing FK(s)");
        result.Should().NotContain("[dbo].[Orders].StatusId");
    }

    [Fact]
    public void MissingIndexAnalyzer_StagingTable_IsExcluded()
    {
        var snapshot = new SchemaSnapshot(
            [],
            [new SchemaColumn("dbo", "Orders_20260101_010203", "TenantId", ColumnTypeCategory.Integer, "int", "NO")],
            EmptySet(),
            EmptySet(),
            EmptySet());

        var result = MissingIndexAnalyzer.Build("poc", snapshot);

        result.Should().Contain("0 potentially unindexed column(s)");
        result.Should().NotContain("Orders_20260101_010203");
    }

    [Fact]
    public void MissingIndexAnalyzer_FilterColumnWithoutIndex_ReportsCandidate()
    {
        var snapshot = new SchemaSnapshot(
            [],
            [new SchemaColumn("dbo", "Orders", "TenantId", ColumnTypeCategory.Integer, "int", "NO")],
            EmptySet(),
            EmptySet(),
            EmptySet());

        var result = MissingIndexAnalyzer.Build("poc", snapshot);

        result.Should().Contain("[dbo].[Orders].TenantId");
        result.Should().Contain("1 potentially unindexed column(s)");
    }

    [Fact]
    public void MissingIndexAnalyzer_LowercaseIdSuffixNoUnderscore_ReportsCandidate()
    {
        // Regression test: original SQL Server implementation used a case-insensitive
        // LIKE '%Id' match, which also matches a lowercase "id" suffix with no underscore.
        var snapshot = new SchemaSnapshot(
            [],
            [new SchemaColumn("dbo", "Orders", "Tenantid", ColumnTypeCategory.Integer, "int", "NO")],
            EmptySet(),
            EmptySet(),
            EmptySet());

        var result = MissingIndexAnalyzer.Build("poc", snapshot);

        result.Should().Contain("[dbo].[Orders].Tenantid");
        result.Should().Contain("1 potentially unindexed column(s)");
    }

    private static HashSet<string> EmptySet() => new(StringComparer.OrdinalIgnoreCase);
}
