using System.Collections.Generic;
using FluentAssertions;
using SqlSchemaMcp.Configuration;
using SqlSchemaMcp.Security;
using Xunit;

namespace SqlSchemaMcp.Tests.Security;

public sealed class ReadOnlyStartupGateTests
{
    private static readonly SecurityOptions Strict = new() { VerifyLoginsAtStartup = true, AllowWritableLogin = false };

    [Fact]
    public void Evaluate_AllReadOnly_StartsWithNoErrors()
    {
        var probes = new List<LoginPermissionResult>
        {
            new("poc", Reachable: true, CanWrite: false, GrantedWrites: []),
            new("azure", Reachable: true, CanWrite: false, GrantedWrites: []),
        };

        var decision = ReadOnlyStartupGate.Evaluate(probes, Strict);

        decision.ShouldStart.Should().BeTrue();
        decision.Errors.Should().BeEmpty();
    }

    [Fact]
    public void Evaluate_WritableLogin_Strict_RefusesToStart()
    {
        var probes = new List<LoginPermissionResult>
        {
            new("poc", Reachable: true, CanWrite: true, GrantedWrites: ["db_datawriter"]),
        };

        var decision = ReadOnlyStartupGate.Evaluate(probes, Strict);

        decision.ShouldStart.Should().BeFalse();
        decision.Errors.Should().ContainSingle().Which.Should().Contain("poc").And.Contain("db_datawriter");
    }

    [Fact]
    public void Evaluate_WritableLogin_AllowOverride_StartsWithWarning()
    {
        var probes = new List<LoginPermissionResult>
        {
            new("poc", Reachable: true, CanWrite: true, GrantedWrites: ["db_owner"]),
        };
        var lenient = new SecurityOptions { VerifyLoginsAtStartup = true, AllowWritableLogin = true };

        var decision = ReadOnlyStartupGate.Evaluate(probes, lenient);

        decision.ShouldStart.Should().BeTrue();
        decision.Warnings.Should().ContainSingle().Which.Should().Contain("poc");
    }

    [Fact]
    public void Evaluate_UnreachableLogin_StartsWithWarning()
    {
        var probes = new List<LoginPermissionResult>
        {
            new("poc", Reachable: false, CanWrite: false, GrantedWrites: []),
        };

        var decision = ReadOnlyStartupGate.Evaluate(probes, Strict);

        decision.ShouldStart.Should().BeTrue();
        decision.Warnings.Should().ContainSingle().Which.Should().Contain("could not be verified");
    }

    [Fact]
    public void Evaluate_VerificationDisabled_AlwaysStarts()
    {
        var probes = new List<LoginPermissionResult>
        {
            new("poc", Reachable: true, CanWrite: true, GrantedWrites: ["db_owner"]),
        };
        var off = new SecurityOptions { VerifyLoginsAtStartup = false, AllowWritableLogin = false };

        var decision = ReadOnlyStartupGate.Evaluate(probes, off);

        decision.ShouldStart.Should().BeTrue();
    }
}
