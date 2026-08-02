using System.Collections;
using System.Globalization;
using System.Text.Json;
using Pulse.Abstractions;

namespace Pulse.Postgres;

/// <summary>
/// Translates the provider-neutral <see cref="FilterExpr"/> tree into a SQL WHERE clause
/// over a CTE alias named <c>doc</c> (a <c>jsonb</c> row with the primary key renamed to
/// <c>_id</c>, matching the delivered document shape). Dot paths map onto jsonb navigation
/// via <c>#&gt;</c>/<c>#&gt;&gt;</c> (numeric array indexes like <c>items.0.name</c> work too).
/// Semantics mirror <see cref="DictionaryFilterMatcher"/>: <c>exists</c> means key present
/// and non-null, and comparisons preserve JSON type (a number field never equals a string
/// filter value). Filter values are bound as parameters, never interpolated.
/// </summary>
public static class PostgresFilterTranslator
{
    public static string Translate(FilterExpr expr, IDictionary<string, object> parameters)
    {
        ArgumentNullException.ThrowIfNull(expr);
        ArgumentNullException.ThrowIfNull(parameters);
        return TranslateCore(expr, parameters);
    }

    private static string TranslateCore(FilterExpr expr, IDictionary<string, object> parameters)
    {
        return expr switch
        {
            And andClause => andClause.Clauses.Count == 0
                ? "TRUE"
                : $"({string.Join(" AND ", andClause.Clauses.Select(c => TranslateCore(c, parameters)))})",
            Or orClause => orClause.Clauses.Count == 0
                ? "FALSE"
                : $"({string.Join(" OR ", orClause.Clauses.Select(c => TranslateCore(c, parameters)))})",
            Not notClause => $"(NOT {TranslateCore(notClause.Clause, parameters)})",
            FieldCompare compare => TranslateCompare(compare, parameters),
            _ => throw new NotSupportedException($"Unsupported filter expression '{expr.GetType().Name}'."),
        };
    }

    private static string TranslateCompare(FieldCompare compare, IDictionary<string, object> parameters)
    {
        if (string.IsNullOrWhiteSpace(compare.Field))
        {
            throw new ArgumentException("Filter field must be a non-empty path.", nameof(compare));
        }

        var path = compare.Field.Split('.');
        string JsonPath() => string.Join(", ", path.Select(static p => "'" + p.Replace("'", "''", StringComparison.Ordinal) + "'"));
        string E() => $"doc #> ARRAY[{JsonPath()}]";
        string V() => $"doc #>> ARRAY[{JsonPath()}]";

        switch (compare.Op)
        {
            case CompareOp.Exists:
                return $"{E()} IS NOT NULL";

            case CompareOp.Eq:
                return $"COALESCE({E()}, 'null'::jsonb) = {Param(parameters, ToJson(compare.Value))}::jsonb";

            case CompareOp.Ne:
                return $"COALESCE({E()}, 'null'::jsonb) <> {Param(parameters, ToJson(compare.Value))}::jsonb";

            case CompareOp.In:
            case CompareOp.NotIn:
                return TranslateMembership(compare, parameters, E, negate: compare.Op == CompareOp.NotIn);

            case CompareOp.Gt:
            case CompareOp.Gte:
            case CompareOp.Lt:
            case CompareOp.Lte:
                if (compare.Value is null)
                {
                    return "FALSE";
                }

                var sqlOp = compare.Op switch
                {
                    CompareOp.Gt => ">",
                    CompareOp.Gte => ">=",
                    CompareOp.Lt => "<",
                    CompareOp.Lte => "<=",
                    _ => throw new InvalidOperationException(),
                };

                return compare.Value switch
                {
                    string text => $"{V()} {sqlOp} {Param(parameters, text)}",
                    _ when IsNumeric(compare.Value) =>
                        $"CASE WHEN jsonb_typeof({E()}) = 'number' THEN ({V()})::numeric END {sqlOp} {Param(parameters, Convert.ToDecimal(compare.Value, CultureInfo.InvariantCulture))}",
                    _ => throw new NotSupportedException(
                        $"Range comparisons require a numeric or string filter value; got '{compare.Value.GetType().Name}'."),
                };

            default:
                throw new NotSupportedException($"Unsupported comparison operator '{compare.Op}'.");
        }
    }

    private static string TranslateMembership(
        FieldCompare compare,
        IDictionary<string, object> parameters,
        Func<string> jsonPath,
        bool negate)
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
            return negate ? "TRUE" : "FALSE";
        }

        var elements = items.Select(item => $"{Param(parameters, ToJson(item))}::jsonb");
        var array = $"ARRAY[{string.Join(", ", elements)}]";
        return negate
            ? $"COALESCE({jsonPath()}, 'null'::jsonb) <> ALL({array})"
            : $"COALESCE({jsonPath()}, 'null'::jsonb) = ANY({array})";
    }

    private static string Param(IDictionary<string, object> parameters, object value)
    {
        var name = $"p{parameters.Count}";
        parameters[name] = value;
        return "@" + name;
    }

    private static string ToJson(object? value)
        => value is null ? "null" : JsonSerializer.Serialize(value);

    private static bool IsNumeric(object value)
        => value is byte or sbyte or short or ushort or int or uint or long or ulong or float or double or decimal;
}
