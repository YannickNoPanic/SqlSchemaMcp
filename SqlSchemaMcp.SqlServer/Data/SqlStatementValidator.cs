using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace SqlSchemaMcp.SqlServer.Data;

/// <summary>
/// Allowlist validator for the execute_query tool. Parses the statement with the
/// T-SQL parser and permits ONLY a single read-only SELECT (optionally a CTE).
/// Everything else — writes, DDL, EXEC, OPENQUERY/OPENROWSET/OPENDATASOURCE,
/// WAITFOR, SELECT INTO, or multiple statements — is rejected.
///
/// This is the code-level defence. The primary defence is the read-only database
/// login verified by the startup gate; see docs/security-posture.md.
/// </summary>
public static class SqlStatementValidator
{
    public static SqlValidationResult Validate(string sql)
    {
        if (string.IsNullOrWhiteSpace(sql))
            return SqlValidationResult.Reject("Empty query is not permitted.");

        var parser = new TSql160Parser(initialQuotedIdentifiers: true);
        using var reader = new StringReader(sql);
        var fragment = parser.Parse(reader, out IList<ParseError> errors);

        if (errors.Count > 0)
            return SqlValidationResult.Reject($"Query failed to parse: {errors[0].Message}");

        if (fragment is not TSqlScript script)
            return SqlValidationResult.Reject("Query could not be interpreted as a T-SQL script.");

        var statements = script.Batches.SelectMany(b => b.Statements).ToList();
        if (statements.Count == 0)
            return SqlValidationResult.Reject("No statement found.");
        if (statements.Count > 1)
            return SqlValidationResult.Reject("Only a single SELECT statement is permitted.");

        if (statements[0] is not SelectStatement select)
            return SqlValidationResult.Reject("Only SELECT statements are permitted. This server is read-only.");

        // Block SELECT ... INTO (creates a table — a write via DDL).
        if (select.Into is not null)
            return SqlValidationResult.Reject("SELECT ... INTO is not permitted (it creates a table).");

        var visitor = new ForbiddenConstructVisitor();
        select.Accept(visitor);
        if (visitor.Rejection is not null)
            return SqlValidationResult.Reject(visitor.Rejection);

        return SqlValidationResult.Allow();
    }

    private sealed class ForbiddenConstructVisitor : TSqlFragmentVisitor
    {
        public string? Rejection { get; private set; }

        // Note: SELECT ... INTO is only syntactically valid as the outermost statement in
        // T-SQL (not inside subqueries or CTEs), so the top-level SelectStatement.Into check
        // in Validate() above already covers it; QuerySpecification has no Into member in
        // this ScriptDom version to visit separately.

        public override void Visit(OpenQueryTableReference node)
        {
            Rejection ??= "OPENQUERY is not permitted.";
            base.Visit(node);
        }

        public override void Visit(OpenRowsetTableReference node)
        {
            Rejection ??= "OPENROWSET is not permitted.";
            base.Visit(node);
        }

        public override void Visit(InternalOpenRowset node)
        {
            Rejection ??= "OPENROWSET is not permitted.";
            base.Visit(node);
        }

        public override void Visit(OpenXmlTableReference node)
        {
            Rejection ??= "OPENXML is not permitted.";
            base.Visit(node);
        }

        // OPENDATASOURCE appears as a four-part SchemaObjectFunctionTableReference /
        // AdHocTableReference; block ad-hoc data source references explicitly.
        public override void Visit(AdHocTableReference node)
        {
            Rejection ??= "OPENDATASOURCE / ad-hoc remote table references are not permitted.";
            base.Visit(node);
        }
    }
}

public readonly record struct SqlValidationResult(bool IsAllowed, string? Reason)
{
    public static SqlValidationResult Allow() => new(true, null);
    public static SqlValidationResult Reject(string reason) => new(false, reason);
}
