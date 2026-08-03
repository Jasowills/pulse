using System.Collections;
using System.Globalization;
using System.Text;
using Pulse.Abstractions;

namespace Pulse.SqlServer;

/// <summary>
/// Translates the provider-neutral <see cref="FilterExpr"/> tree into a SQL Server WHERE
/// clause over a table (not a JSON document, unlike the Postgres provider). The first path
/// segment is a real column (<c>_id</c> maps to the primary key column); any remaining
/// segments navigate JSON inside that column via <c>JSON_VALUE</c>, so dotted paths like
/// <c>customer.address.city</c> work on <c>nvarchar</c> JSON columns. Semantics mirror
/// <see cref="DictionaryFilterMatcher"/>: <c>exists</c> means the field is present and
/// non-null, and range comparisons on JSON paths are done numerically when the filter value
/// is numeric. Filter values are bound as parameters, never interpolated.
/// </summary>
public static class SqlServerFilterTranslator
{
    public static string Translate(
        FilterExpr expr,
        string? primaryKeyColumn,
        IDictionary<string, object> parameters)
    {
        ArgumentNullException.ThrowIfNull(expr);
        ArgumentNullException.ThrowIfNull(parameters);
        return TranslateCore(expr, primaryKeyColumn, parameters);
    }

    private static string TranslateCore(
        FilterExpr expr,
        string? primaryKeyColumn,
        IDictionary<string, object> parameters)
    {
        return expr switch
        {
            And andClause => andClause.Clauses.Count == 0
                ? "(1 = 1)"
                : $"({string.Join(" AND ", andClause.Clauses.Select(c => TranslateCore(c, primaryKeyColumn, parameters)))})",
            Or orClause => orClause.Clauses.Count == 0
                ? "(1 = 0)"
                : $"({string.Join(" OR ", orClause.Clauses.Select(c => TranslateCore(c, primaryKeyColumn, parameters)))})",
            Not notClause => $"(NOT {TranslateCore(notClause.Clause, primaryKeyColumn, parameters)})",
            FieldCompare compare => TranslateCompare(compare, primaryKeyColumn, parameters),
            _ => throw new NotSupportedException($"Unsupported filter expression '{expr.GetType().Name}'."),
        };
    }

    private static string TranslateCompare(
        FieldCompare compare,
        string? primaryKeyColumn,
        IDictionary<string, object> parameters)
    {
        if (string.IsNullOrWhiteSpace(compare.Field))
        {
            throw new ArgumentException("Filter field must be a non-empty path.", nameof(compare));
        }

        var (column, path) = ResolvePath(compare.Field, primaryKeyColumn);
        // The ISJSON guards are required: JSON_VALUE throws on input that is not valid JSON,
        // so a non-JSON column must evaluate to NULL (never match) instead of erroring.
        string TextRef() => path.Length == 0
            ? QuoteIdent(column)
            : $"CASE WHEN ISJSON({QuoteIdent(column)}) = 1 THEN JSON_VALUE({QuoteIdent(column)}, '{JsonPath(path)}') ELSE NULL END";
        string NumericRef() => path.Length == 0
            ? QuoteIdent(column)
            : $"CASE WHEN ISJSON({QuoteIdent(column)}) = 1 THEN TRY_CONVERT(decimal(38, 18), JSON_VALUE({QuoteIdent(column)}, '{JsonPath(path)}')) ELSE NULL END";

        switch (compare.Op)
        {
            case CompareOp.Exists:
                return $"{TextRef()} IS NOT NULL";

            case CompareOp.Eq:
            case CompareOp.Ne:
                var eqOp = compare.Op == CompareOp.Eq ? "=" : "<>";
                if (compare.Value is null)
                {
                    return $"{TextRef()} IS {(compare.Op == CompareOp.Eq ? "NULL" : "NOT NULL")}";
                }

                if (IsNumeric(compare.Value))
                {
                    return $"{NumericRef()} {eqOp} {Param(parameters, Convert.ToDecimal(compare.Value, CultureInfo.InvariantCulture))}";
                }

                return $"{TextRef()} {eqOp} {Param(parameters, compare.Value)}";

            case CompareOp.In:
            case CompareOp.NotIn:
                return TranslateMembership(compare, parameters, TextRef, NumericRef);

            case CompareOp.Gt:
            case CompareOp.Gte:
            case CompareOp.Lt:
            case CompareOp.Lte:
                if (compare.Value is null)
                {
                    return "(1 = 0)";
                }

                var sqlOp = compare.Op switch
                {
                    CompareOp.Gt => ">",
                    CompareOp.Gte => ">=",
                    CompareOp.Lt => "<",
                    CompareOp.Lte => "<=",
                    _ => throw new InvalidOperationException(),
                };

                if (IsNumeric(compare.Value))
                {
                    return $"{NumericRef()} {sqlOp} {Param(parameters, Convert.ToDecimal(compare.Value, CultureInfo.InvariantCulture))}";
                }

                if (compare.Value is string)
                {
                    return $"{TextRef()} {sqlOp} {Param(parameters, compare.Value)}";
                }

                throw new NotSupportedException(
                    $"Range comparisons require a numeric or string filter value; got '{compare.Value.GetType().Name}'.");

            default:
                throw new NotSupportedException($"Unsupported comparison operator '{compare.Op}'.");
        }
    }

    private static string TranslateMembership(
        FieldCompare compare,
        IDictionary<string, object> parameters,
        Func<string> textRef,
        Func<string> numericRef)
    {
        var enumerable = compare.Value as IEnumerable;
        if (enumerable is null || compare.Value is string)
        {
            enumerable = new[] { compare.Value };
        }

        var items = new List<object?>();
        foreach (var item in enumerable)
        {
            items.Add(item);
        }

        if (items.Count == 0)
        {
            return compare.Op == CompareOp.NotIn ? "(1 = 1)" : "(1 = 0)";
        }

        var useNumeric = items.All(i => i is not null && IsNumeric(i));
        var placeholder = new List<string>(items.Count);
        foreach (var item in items)
        {
            object value = item is null ? DBNull.Value
                : useNumeric ? Convert.ToDecimal(item, CultureInfo.InvariantCulture)
                : item;
            placeholder.Add(Param(parameters, value));
        }

        var list = string.Join(", ", placeholder);
        var target = useNumeric ? numericRef() : textRef();
        return compare.Op == CompareOp.NotIn
            ? $"{target} NOT IN ({list})"
            : $"{target} IN ({list})";
    }

    private static (string Column, string[] Path) ResolvePath(string field, string? primaryKeyColumn)
    {
        var segments = field.Split('.');
        if (segments.Length == 0 || segments[0].Length == 0)
        {
            throw new ArgumentException("Filter field must be a non-empty path.", nameof(field));
        }

        var column = segments[0];
        if (string.Equals(column, "_id", StringComparison.Ordinal))
        {
            column = primaryKeyColumn ?? "_id";
        }

        return (column, segments.Skip(1).ToArray());
    }

    private static string JsonPath(string[] path)
    {
        var sb = new StringBuilder("$");
        foreach (var segment in path)
        {
            if (segment.Length > 0 && segment.All(static c => char.IsLetterOrDigit(c) || c == '_'))
            {
                sb.Append('.').Append(segment);
            }
            else
            {
                sb.Append("[\"").Append(segment.Replace("\"", "\\\"", StringComparison.Ordinal)).Append("\"]");
            }
        }

        return sb.ToString();
    }

    private static string Param(IDictionary<string, object> parameters, object value)
    {
        var name = $"p{parameters.Count}";
        parameters[name] = value;
        return "@" + name;
    }

    private static string QuoteIdent(string name)
        => "[" + name.Replace("]", "]]", StringComparison.Ordinal) + "]";

    private static bool IsNumeric(object value)
        => value is byte or sbyte or short or ushort or int or uint or long or ulong or float or double or decimal;
}
