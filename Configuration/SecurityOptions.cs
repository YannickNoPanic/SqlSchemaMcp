namespace SqlSchemaMcp.Configuration;

public sealed class SecurityOptions
{
    /// <summary>When true (default) the server probes every configured login at startup.</summary>
    public bool VerifyLoginsAtStartup { get; init; } = true;

    /// <summary>Escape hatch. When false (default) the server refuses to start if any reachable login can write.</summary>
    public bool AllowWritableLogin { get; init; } = false;
}
