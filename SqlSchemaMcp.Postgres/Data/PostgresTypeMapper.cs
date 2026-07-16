using SqlSchemaMcp.Abstractions;

namespace SqlSchemaMcp.Postgres.Data;

public static class PostgresTypeMapper
{
    public static ColumnTypeCategory ToCategory(string dataType) =>
        dataType.ToLowerInvariant() switch
        {
            "smallint" or "integer" or "bigint" => ColumnTypeCategory.Integer,
            "uuid" => ColumnTypeCategory.Guid,
            "text" or "varchar" or "character varying" or "char" or "character" => ColumnTypeCategory.Text,
            "boolean" => ColumnTypeCategory.Boolean,
            "date" or "timestamp" or "timestamp without time zone" or "timestamp with time zone"
                or "timestamptz" or "time" or "time without time zone" or "time with time zone"
                or "timetz" => ColumnTypeCategory.Temporal,
            "numeric" or "decimal" or "real" or "double precision" => ColumnTypeCategory.Decimal,
            _ => ColumnTypeCategory.Other
        };

    public static string FormatColumnType(string dataType, int? maxLength, int? precision, int? scale)
    {
        if ((dataType.Equals("character varying", StringComparison.OrdinalIgnoreCase)
                || dataType.Equals("varchar", StringComparison.OrdinalIgnoreCase)
                || dataType.Equals("character", StringComparison.OrdinalIgnoreCase)
                || dataType.Equals("char", StringComparison.OrdinalIgnoreCase))
            && maxLength is > 0)
        {
            return $"{dataType}({maxLength.Value})";
        }

        if ((dataType.Equals("numeric", StringComparison.OrdinalIgnoreCase)
                || dataType.Equals("decimal", StringComparison.OrdinalIgnoreCase))
            && precision is > 0)
        {
            return scale is > 0
                ? $"{dataType}({precision.Value},{scale.Value})"
                : $"{dataType}({precision.Value})";
        }

        return dataType;
    }
}
