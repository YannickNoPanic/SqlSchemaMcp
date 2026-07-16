using SqlSchemaMcp.Abstractions;
using SqlSchemaMcp.Abstractions.Capabilities;
using SqlSchemaMcp.SqlServer;
using SqlSchemaMcp.SqlServer.Data;

namespace SqlSchemaMcp.Data;

public sealed class CompareQueries(ICapabilityResolver resolver)
{
    public async Task<HashSet<string>> GetTableNames(
        string database,
        CancellationToken cancellationToken = default)
    {
        var snapshot = await GetSnapshotOrNull(database, cancellationToken);
        return snapshot is null
            ? []
            : snapshot.Objects
                .Where(o => o.Type.Equals("TABLE", StringComparison.OrdinalIgnoreCase))
                .Select(o => $"{o.Schema}.{o.Name}")
                .ToHashSet(StringComparer.OrdinalIgnoreCase);
    }

    public async Task<HashSet<string>> GetProcNames(
        string database,
        CancellationToken cancellationToken = default)
    {
        var snapshot = await GetSnapshotOrNull(database, cancellationToken);
        return snapshot is null
            ? []
            : snapshot.Objects
                .Where(o => o.Type.Equals("PROCEDURE", StringComparison.OrdinalIgnoreCase))
                .Select(o => $"{o.Schema}.{o.Name}")
                .ToHashSet(StringComparer.OrdinalIgnoreCase);
    }

    public async Task<HashSet<string>> GetViewNames(
        string database,
        CancellationToken cancellationToken = default)
    {
        var snapshot = await GetSnapshotOrNull(database, cancellationToken);
        return snapshot is null
            ? []
            : snapshot.Objects
                .Where(o => o.Type.Equals("VIEW", StringComparison.OrdinalIgnoreCase))
                .Select(o => $"{o.Schema}.{o.Name}")
                .ToHashSet(StringComparer.OrdinalIgnoreCase);
    }

    public async Task<List<ColumnInfo>> GetTableColumns(
        string database,
        string tableName,
        CancellationToken cancellationToken = default)
    {
        var snapshot = await GetSnapshotOrNull(database, cancellationToken);
        if (snapshot is null)
            return [];

        var (schema, table) = ParseSchemaTable(tableName);
        return snapshot.Columns
            .Where(c => c.Schema.Equals(schema, StringComparison.OrdinalIgnoreCase)
                && c.Table.Equals(table, StringComparison.OrdinalIgnoreCase))
            .Select(c => new ColumnInfo(c.Column, c.FormattedType, c.Nullable))
            .ToList();
    }

    public Task<(int LineCount, List<string> TablesReferenced)> GetProcStats(
        string database,
        string procName,
        CancellationToken cancellationToken = default)
    {
        if (resolver.TryResolve<ISchemaSnapshotCapability>(database, out var engine, out _) && engine == DatabaseEngine.SqlServer)
            return ResolveSqlServerSupport(database, support => support.GetProcStats(database, procName, cancellationToken));

        return Task.FromResult((0, new List<string>()));
    }

    public Task<(int LineCount, List<string> TablesReferenced)> GetViewStats(
        string database,
        string viewName,
        CancellationToken cancellationToken = default)
    {
        if (resolver.TryResolve<ISchemaSnapshotCapability>(database, out var engine, out _) && engine == DatabaseEngine.SqlServer)
            return ResolveSqlServerSupport(database, support => support.GetViewStats(database, viewName, cancellationToken));

        return Task.FromResult((0, new List<string>()));
    }

    private async Task<SchemaSnapshot?> GetSnapshotOrNull(string database, CancellationToken ct)
    {
        if (!resolver.TryResolve<ISchemaSnapshotCapability>(database, out _, out var capability) || capability is null)
            return null;

        try
        {
            return await capability.GetSchemaSnapshot(database, ct);
        }
        catch
        {
            return null;
        }
    }

    private Task<T> ResolveSqlServerSupport<T>(string database, Func<SqlServerCompareSupport, Task<T>> execute)
    {
        if (resolver.TryResolve<ISchemaSnapshotCapability>(database, out _, out var capability)
            && capability is SqlServerEngine engine)
        {
            return execute(GetCompareSupport(engine));
        }

        return Task.FromResult(default(T)!);
    }

    private static SqlServerCompareSupport GetCompareSupport(SqlServerEngine engine) => engine.CompareSupport;

    private static (string Schema, string Table) ParseSchemaTable(string tableName)
    {
        var trimmed = tableName.Trim();

        if (trimmed.StartsWith('['))
        {
            int closingBracket = trimmed.IndexOf(']');
            if (closingBracket > 0)
            {
                string firstPart = trimmed[1..closingBracket];
                string remainder = trimmed[(closingBracket + 1)..].Trim();

                if (remainder.StartsWith('.'))
                {
                    string rest = remainder[1..].Trim().Trim('[', ']');
                    return (firstPart, rest);
                }

                return ("dbo", firstPart);
            }
        }

        if (trimmed.Contains('.'))
        {
            var parts = trimmed.Split('.', 2);
            return (parts[0].Trim('[', ']'), parts[1].Trim('[', ']'));
        }

        return ("dbo", trimmed.Trim('[', ']'));
    }
}

public sealed record ColumnInfo(string Name, string Type, string Nullable);
