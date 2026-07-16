using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;
using SqlSchemaMcp.Postgres.Configuration;

namespace SqlSchemaMcp.Postgres.Data;

public abstract class PostgresQueryBase
{
    protected readonly IReadOnlyDictionary<string, string> Databases;
    private readonly ILogger _logger;

    protected PostgresQueryBase(IOptions<PostgresEngineOptions> options, ILogger logger)
    {
        Databases = options.Value.Databases;
        _logger = logger;
    }

    protected string UnknownDatabase(string database) =>
        $"ERROR: Unknown database '{database}'. Available: {string.Join(", ", Databases.Keys)}";

    protected string SafeError(Exception ex, string? operation = null)
    {
        if (!string.IsNullOrWhiteSpace(operation))
            _logger.LogWarning(ex, "Postgres operation {Operation} failed", operation);
        else
            _logger.LogWarning(ex, "Postgres operation failed");

        return "ERROR: the query failed. Check the server log for details.";
    }

    protected async Task<(NpgsqlConnection? Connection, string? Error)> OpenConnection(string database, CancellationToken ct)
    {
        if (!Databases.TryGetValue(database, out var connectionString))
            return (null, UnknownDatabase(database));

        try
        {
            var conn = new NpgsqlConnection(connectionString);
            await conn.OpenAsync(ct);
            return (conn, null);
        }
        catch (Exception ex)
        {
            return (null, SafeError(ex, nameof(OpenConnection)));
        }
    }
}
