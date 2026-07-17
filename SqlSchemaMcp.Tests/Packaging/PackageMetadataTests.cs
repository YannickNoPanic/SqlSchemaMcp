using System.Xml.Linq;
using FluentAssertions;
using Xunit;

namespace SqlSchemaMcp.Tests.Packaging;

public sealed class PackageMetadataTests
{
    [Fact]
    public void HostProject_DefinesDotnetToolMetadata()
    {
        var projectPath = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "../../../../SqlSchemaMcp.csproj"));
        var document = XDocument.Load(projectPath);
        var properties = document.Descendants("PropertyGroup").Elements()
            .ToDictionary(element => element.Name.LocalName, element => element.Value);

        properties["PackAsTool"].Should().Be("true");
        properties["IsPackable"].Should().Be("true");
        properties["ToolCommandName"].Should().Be("sql-schema-mcp");
        properties["PackageId"].Should().Be("SqlSchemaMcp");
        properties["Description"].Should().Contain("MCP server");
        properties["Authors"].Should().NotBeNullOrWhiteSpace();
        properties["PackageReadmeFile"].Should().Be("README.md");
    }
}
