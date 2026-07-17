using System.Collections.Generic;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SqlSchemaMcp.Abstractions;
using SqlSchemaMcp.Abstractions.Security;
using SqlSchemaMcp.Auditing;
using SqlSchemaMcp.Configuration;
using SqlSchemaMcp.Data;
using SqlSchemaMcp.Engines;
using SqlSchemaMcp.MariaDb;
using SqlSchemaMcp.MariaDb.Configuration;
using SqlSchemaMcp.MariaDb.Data;
using SqlSchemaMcp.Postgres;
using SqlSchemaMcp.Postgres.Configuration;
using SqlSchemaMcp.Postgres.Data;
using SqlSchemaMcp.Security;
using SqlSchemaMcp.SqlServer;
using SqlSchemaMcp.SqlServer.Configuration;
using SqlSchemaMcp.SqlServer.Data;
using SqlSchemaMcp.SqlServer.Security;
using SqlSchemaMcp.Tools;

bool useSse = args.Contains("--sse");

if (useSse)
{
    var builder = WebApplication.CreateBuilder(args);

    builder.Configuration.SetBasePath(AppContext.BaseDirectory);
    builder.Configuration.AddEnvironmentVariables(prefix: "SQLMCP_");

    RegisterServices(builder.Configuration, builder.Services);

    var port = builder.Configuration.GetValue<int>("Mcp:Port", 5101);
    var bindAddress = builder.Configuration.GetValue<string>("Mcp:BindAddress", "localhost");
    builder.WebHost.UseUrls($"http://{bindAddress}:{port}");

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
        .WithTools<SecurityTools>()
        .WithTools<QueryTools>()
        .WithTools<RuntimeTools>();

    var app = builder.Build();

    if (!await RunStartupGateAsync(app.Services, CancellationToken.None))
    {
        Console.Error.WriteLine("[SqlSchemaMcp] Startup aborted: a configured login is not read-only.");
        Environment.Exit(1);
    }

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

    // Health check — plain GET / without SSE headers (e.g. from health check hooks).
    // MapMcp handles GET / for SSE streams; non-SSE GET returns 406, which blocks health checks.
    app.Use(async (context, next) =>
    {
        if (context.Request.Method == "GET"
            && context.Request.Path == "/"
            && !context.Request.Headers.Accept.ToString().Contains("text/event-stream"))
        {
            context.Response.StatusCode = 200;
            await context.Response.WriteAsJsonAsync(new { status = "ok", service = "SqlSchemaMcp" });
            return;
        }
        await next(context);
    });

    app.MapMcp();

    Console.Error.WriteLine($"[SqlSchemaMcp] HTTP mode — http://{bindAddress}:{port}/");
    await app.RunAsync();
}
else
{
    var builder = Host.CreateApplicationBuilder(args);

    builder.Configuration.SetBasePath(AppContext.BaseDirectory);
    builder.Configuration.AddEnvironmentVariables(prefix: "SQLMCP_");

    builder.Logging.AddConsole(opts => opts.LogToStandardErrorThreshold = LogLevel.Trace);

    RegisterServices(builder.Configuration, builder.Services);

    builder.Services
        .AddMcpServer()
        .WithStdioServerTransport()
        .WithTools<SchemaTools>()
        .WithTools<AnalysisTools>()
        .WithTools<PipelineTools>()
        .WithTools<CompareTools>()
        .WithTools<ConstraintTools>()
        .WithTools<DiagnosticsTools>()
        .WithTools<DataTools>()
        .WithTools<SecurityTools>()
        .WithTools<QueryTools>()
        .WithTools<RuntimeTools>();

    Console.Error.WriteLine("[SqlSchemaMcp] Stdio mode gestart");
    var host = builder.Build();

    if (!await RunStartupGateAsync(host.Services, CancellationToken.None))
    {
        Console.Error.WriteLine("[SqlSchemaMcp] Startup aborted: a configured login is not read-only.");
        Environment.Exit(1);
    }

    await host.RunAsync();
}

static void RegisterServices(IConfiguration configuration, IServiceCollection services)
{
    var databases = new ConfiguredDatabases(DatabaseConfigLoader.Load(configuration));
    services.AddSingleton(databases);
    services.AddOptions<SqlServerOptions>()
        .Configure(options =>
        {
            foreach (var (name, connectionString) in databases.SqlServerConnectionStrings)
                options.Databases[name] = connectionString;
        });
    services.AddOptions<SqlServerEngineOptions>()
        .Configure(engineOptions =>
        {
            foreach (var (name, connectionString) in databases.SqlServerConnectionStrings)
                engineOptions.Databases[name] = connectionString;
        });
    services.AddOptions<PostgresEngineOptions>()
        .Configure(engineOptions =>
        {
            foreach (var (name, connectionString) in databases.PostgresConnectionStrings)
                engineOptions.Databases[name] = connectionString;
        });
    services.AddOptions<MariaDbEngineOptions>()
        .Configure(engineOptions =>
        {
            foreach (var (name, connectionString) in databases.MariaDbConnectionStrings)
                engineOptions.Databases[name] = connectionString;
        });
    services.Configure<SecurityOptions>(configuration.GetSection("Security"));
    services.Configure<AuditOptions>(configuration.GetSection("Audit"));

    services.AddSingleton<SchemaQueries>();
    services.AddSingleton<AnalysisQueries>();
    services.AddSingleton<PipelineQueries>();
    services.AddSingleton<CompareQueries>();
    services.AddSingleton<DiagnosticsQueries>();
    services.AddSingleton<DataQueries>();
    services.AddSingleton<SecurityQueries>();
    services.AddSingleton<QueryQueries>();
    services.AddSingleton<RuntimeQueries>();
    services.AddSingleton<SqlServerQuery>();
    services.AddSingleton<SqlServerSchema>();
    services.AddSingleton<SqlServerSchemaExtras>();
    services.AddSingleton<SqlServerDataSampling>();
    services.AddSingleton<SqlServerDiagnostics>();
    services.AddSingleton<SqlServerPipeline>();
    services.AddSingleton<SqlServerSecurity>();
    services.AddSingleton<SqlServerAnalysis>();
    services.AddSingleton<SqlServerSchemaSnapshot>();
    services.AddSingleton<SqlServerCompareSupport>();
    services.AddSingleton<SqlServerEngine>();
    services.AddSingleton<PostgresSchema>();
    services.AddSingleton<PostgresSchemaSnapshot>();
    services.AddSingleton<PostgresEngine>();
    services.AddSingleton<MariaDbSchema>();
    services.AddSingleton<MariaDbSchemaSnapshot>();
    services.AddSingleton<MariaDbEngine>();
    services.AddSingleton<ICapabilityResolver>(sp =>
        new CapabilityResolver(
            databases.All,
            new Dictionary<DatabaseEngine, object>
            {
                [DatabaseEngine.SqlServer] = sp.GetRequiredService<SqlServerEngine>(),
                [DatabaseEngine.Postgres] = sp.GetRequiredService<PostgresEngine>(),
                [DatabaseEngine.MariaDb] = sp.GetRequiredService<MariaDbEngine>()
            }));
    services.AddSingleton<IPermissionProbe, SqlServerPermissionProbe>();
    services.AddSingleton<IAuditLog, FileAuditLog>();
}

static async Task<bool> RunStartupGateAsync(IServiceProvider services, CancellationToken ct)
{
    var options = services.GetRequiredService<IOptions<SqlServerOptions>>().Value;
    var security = services.GetRequiredService<IOptions<SecurityOptions>>().Value;
    var probe = services.GetRequiredService<IPermissionProbe>();

    var configurationErrors = options.GetConfigurationErrors();
    foreach (var error in configurationErrors)
        Console.Error.WriteLine($"[SqlSchemaMcp] CRITICAL: {error}");
    if (configurationErrors.Count > 0)
        return false;

    var results = new List<LoginPermissionResult>();
    foreach (var (name, connectionString) in options.Databases)
        results.Add(await probe.ProbeAsync(name, connectionString, ct));

    var decision = ReadOnlyStartupGate.Evaluate(results, security);

    foreach (var warning in decision.Warnings)
        Console.Error.WriteLine($"[SqlSchemaMcp] WARN: {warning}");
    foreach (var error in decision.Errors)
        Console.Error.WriteLine($"[SqlSchemaMcp] CRITICAL: {error}");

    return decision.ShouldStart;
}
