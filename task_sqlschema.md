# TASK: SqlSchemaMcp — Dual-mode transport (stdio + SSE)

## Doel
Voeg stdio transport toe als default naast de bestaande SSE mode. Stdio wordt
de primary mode voor teamleden (geen handmatige setup). SSE blijft beschikbaar
via `--sse` flag voor powerusers die meerdere Claude instanties tegelijk willen.

## Acceptatiecriteria
- [ ] `dotnet run` (zonder args) → start in stdio mode, werkt als MCP server via stdin/stdout
- [ ] `dotnet run -- --sse` → start SSE server op huidige port
- [ ] Connection string configureerbaar via env var `SQL_SCHEMA_CONNECTION`
- [ ] Alle logging gaat naar `stderr`, nooit naar `stdout`
- [ ] `.claude/settings.json` snippets gedocumenteerd in README.md

## Technische aanpak

### 1. Program.cs — transport switch
Voeg bovenaan de bestaande builder setup in:
```csharp
bool useSse = args.Contains("--sse");

if (useSse)
{
    builder.WebHost.UseUrls("http://localhost:port");
    builder.Services.AddMcpServer().WithHttpTransport();
    Console.Error.WriteLine("[SqlSchemaMcp] SSE mode — http://localhost:port/sse");
}
else
{
    builder.Services.AddMcpServer().WithStdioServerTransport();
    Console.Error.WriteLine("[SqlSchemaMcp] Stdio mode gestart");
}
```

### 2. Connection string via env var
Vervang de huidige hardcoded/appsettings connection string door:
```csharp
var connectionString = builder.Configuration["SqlSchema:ConnectionString"]
    ?? Environment.GetEnvironmentVariable("SQL_SCHEMA_CONNECTION")
    ?? throw new InvalidOperationException(
        "Geen connection string. Stel SQL_SCHEMA_CONNECTION in als env var.");
```

### 3. Stderr logging check
Scan de volledige codebase op `Console.WriteLine` — vervang elke aanroep
buiten MCP tool responses door `Console.Error.WriteLine`. Stdout mag alleen
door de MCP SDK zelf worden gebruikt.

### 4. README.md genereren
Na de implementatie: genereer README.md op basis van de code. Inhoud:
- Korte beschrijving van de MCP en beschikbare tools (naam + één zin)
- Setup voor teamleden: stdio, minimale stappen (clone → build → settings.json)
- Setup voor powerusers: SSE, `dotnet run -- --sse`
- Configuratietabel met `SQL_SCHEMA_CONNECTION` en eventuele andere env vars
- settings.json snippets voor beide modes
- Troubleshooting: stdout-conflict, ontbrekende connection string

## Niet in scope
- Nieuwe tools toevoegen
- Authenticatie / security op de SSE endpoint
- Live schema refresh
