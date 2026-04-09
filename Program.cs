using Microsoft.Extensions.Configuration;
using SqlSchemaMcp.Configuration;
using SqlSchemaMcp.Data;
using SqlSchemaMcp.Tools;

var builder = WebApplication.CreateBuilder(args);

builder.Configuration.SetBasePath(AppContext.BaseDirectory);
builder.Configuration.AddEnvironmentVariables(prefix: "SQLMCP_");

builder.Services.Configure<SqlServerOptions>(
    builder.Configuration.GetSection("SqlServer"));

builder.Services.AddSingleton<SchemaQueries>();
builder.Services.AddSingleton<AnalysisQueries>();
builder.Services.AddSingleton<PipelineQueries>();
builder.Services.AddSingleton<CompareQueries>();
builder.Services.AddSingleton<DiagnosticsQueries>();
builder.Services.AddSingleton<DataQueries>();
builder.Services.AddSingleton<SecurityQueries>();

builder.Services
    .AddMcpServer()
    .WithHttpTransport()
    .WithTools<SchemaTools>()
    .WithTools<AnalysisTools>()
    .WithTools<PipelineTools>()
    .WithTools<CompareTools>()
    .WithTools<ConstraintTools>()
    .WithTools<DiagnosticsTools>()
    .WithTools<DataTools>()
    .WithTools<SecurityTools>();

var port = builder.Configuration.GetValue<int>("Mcp:Port", 5101);
builder.WebHost.UseUrls($"http://localhost:{port}");

var app = builder.Build();

// Minimal OAuth server so Claude Code can complete its auth flow for local MCP connections.
// No tokens are validated — these endpoints exist only to satisfy the OAuth discovery dance.
app.MapGet("/.well-known/oauth-protected-resource", () => Results.Json(new
{
    resource = $"http://localhost:{port}",
    authorization_servers = new[] { $"http://localhost:{port}" }
}));

app.MapGet("/.well-known/oauth-authorization-server", () => Results.Json(new
{
    issuer = $"http://localhost:{port}",
    authorization_endpoint = $"http://localhost:{port}/oauth/authorize",
    token_endpoint = $"http://localhost:{port}/oauth/token",
    registration_endpoint = $"http://localhost:{port}/register",
    response_types_supported = new[] { "code" },
    grant_types_supported = new[] { "authorization_code" },
    code_challenge_methods_supported = new[] { "S256" }
}));

app.MapPost("/register", async (HttpRequest req) =>
{
    var body = await req.ReadFromJsonAsync<System.Text.Json.JsonElement>();
    var redirectUris = body.TryGetProperty("redirect_uris", out var uris)
        ? uris.EnumerateArray().Select(u => u.GetString()).ToArray()
        : [];
    return Results.Json(new
    {
        client_id = "local-mcp-client",
        client_id_issued_at = DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
        redirect_uris = redirectUris,
        grant_types = new[] { "authorization_code" },
        response_types = new[] { "code" },
        token_endpoint_auth_method = "none"
    }, statusCode: 201);
});

app.MapGet("/oauth/authorize", (string? redirect_uri, string? state) =>
{
    if (string.IsNullOrEmpty(redirect_uri))
        return Results.BadRequest("redirect_uri required");
    var sep = redirect_uri.Contains('?') ? '&' : '?';
    var location = $"{redirect_uri}{sep}code={Uri.EscapeDataString("local-dev-code")}";
    if (!string.IsNullOrEmpty(state))
        location += $"&state={Uri.EscapeDataString(state)}";
    return Results.Redirect(location);
});

app.MapPost("/oauth/token", () => Results.Json(new
{
    access_token = "local-dev-token",
    token_type = "Bearer",
    expires_in = 86400
}));

app.MapMcp();

await app.RunAsync();
