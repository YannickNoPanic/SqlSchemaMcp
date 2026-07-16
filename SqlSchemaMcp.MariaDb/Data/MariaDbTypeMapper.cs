using SqlSchemaMcp.Abstractions;

namespace SqlSchemaMcp.MariaDb.Data;

public static class MariaDbTypeMapper
{
    public static ColumnTypeCategory ToCategory(string dataType) =>
        dataType.ToLowerInvariant() switch
        {
            "tinyint" => ColumnTypeCategory.Boolean,
            "smallint" or "mediumint" or "int" or "integer" or "bigint" => ColumnTypeCategory.Integer,
            "char" when true => ColumnTypeCategory.Guid,
            "varchar" or "text" or "tinytext" or "mediumtext" or "longtext" => ColumnTypeCategory.Text,
            "date" or "datetime" or "timestamp" or "time" or "year" => ColumnTypeCategory.Temporal,
            "decimal" or "numeric" or "float" or "double" or "real" => ColumnTypeCategory.Decimal,
            _ => ColumnTypeCategory.Other
        };

    public static string FormatColumnType(string dataType, long? maxLength, long? precision, long? scale)
    {
        if ((dataType.Equals("varchar", StringComparison.OrdinalIgnoreCase)
                || dataType.Equals("char", StringComparison.OrdinalIgnoreCase))
            && maxLength is > 0)
        {
            return $"{dataType}({maxLength.Value})";
        }

        if ((dataType.Equals("decimal", StringComparison.OrdinalIgnoreCase)
                || dataType.Equals("numeric", StringComparison.OrdinalIgnoreCase))
            && precision is > 0)
        {
            return scale is > 0
                ? $"{dataType}({precision.Value},{scale.Value})"
                : $"{dataType}({precision.Value})";
        }

        return dataType;
    }
}
