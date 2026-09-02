using System.Collections;
using System.Globalization;
using System.Text.Json;
using Pulse.Abstractions;
using Pulse.Abstractions.Filtering;

namespace Pulse.Postgres;

public static class PostgresFilterTranslator
{
    public static string Translate(FilterExpr expr, IDictionary<string, object> parameters)
    {
        ArgumentNullException.ThrowIfNull(expr);
        ArgumentNullException.ThrowIfNull(parameters);
        return new Translator(parameters).Translate(expr);
    }

    private sealed class Translator : FilterTranslatorBase<string>
    {
        private readonly IDictionary<string, object> _parameters;
        public Translator(IDictionary<string, object> parameters) => _parameters = parameters;

        protected override string EmptyAnd() => "TRUE";
        protected override string EmptyOr() => "FALSE";
        protected override string CombineAnd(IEnumerable<string> clauses) => $"({string.Join(" AND ", clauses)})";
        protected override string CombineOr(IEnumerable<string> clauses) => $"({string.Join(" OR ", clauses)})";
        protected override string Negate(string clause) => $"(NOT {clause})";

        protected override string TranslateCompare(FieldCompare compare)
        {
            if (string.IsNullOrWhiteSpace(compare.Field))
                throw new ArgumentException("Filter field must be a non-empty path.", nameof(compare));

            var path = compare.Field.Split('.');
            string JsonPath() => string.Join(", ", path.Select(static p => "'" + p.Replace("'", "''", StringComparison.Ordinal) + "'"));
            string E() => $"doc #> ARRAY[{JsonPath()}]";
            string V() => $"doc #>> ARRAY[{JsonPath()}]";

            switch (compare.Op)
            {
                case CompareOp.Exists:
                    return $"{E()} IS NOT NULL";
                case CompareOp.Eq:
                    return $"COALESCE({E()}, 'null'::jsonb) = {Param(ToJson(compare.Value))}::jsonb";
                case CompareOp.Ne:
                    return $"COALESCE({E()}, 'null'::jsonb) <> {Param(ToJson(compare.Value))}::jsonb";
                case CompareOp.In:
                case CompareOp.NotIn:
                    return TranslateMembership(compare, E, negate: compare.Op == CompareOp.NotIn);
                case CompareOp.Gt:
                case CompareOp.Gte:
                case CompareOp.Lt:
                case CompareOp.Lte:
                    if (compare.Value is null) return "FALSE";
                    var sqlOp = compare.Op switch { CompareOp.Gt => ">", CompareOp.Gte => ">=", CompareOp.Lt => "<", CompareOp.Lte => "<=", _ => throw new InvalidOperationException() };
                    return compare.Value switch
                    {
                        string text => $"{V()} {sqlOp} {Param(text)}",
                        _ when FilterValueHelpers.IsNumeric(compare.Value) => $"CASE WHEN jsonb_typeof({E()}) = 'number' THEN ({V()})::numeric END {sqlOp} {Param(Convert.ToDecimal(compare.Value, CultureInfo.InvariantCulture))}",
                        _ => throw new NotSupportedException($"Range comparisons require a numeric or string filter value; got '{compare.Value.GetType().Name}'."),
                    };
                default:
                    throw new NotSupportedException($"Unsupported comparison operator '{compare.Op}'.");
            }
        }

        private string TranslateMembership(FieldCompare compare, Func<string> jsonPath, bool negate)
        {
            var enumerable = compare.Value as IEnumerable;
            if (enumerable is null || compare.Value is string) enumerable = new[] { compare.Value };
            var items = new List<object?>();
            foreach (var item in enumerable) items.Add(item);
            if (items.Count == 0) return negate ? "TRUE" : "FALSE";
            var elements = items.Select(item => $"{Param(ToJson(item))}::jsonb");
            var array = $"ARRAY[{string.Join(", ", elements)}]";
            return negate ? $"COALESCE({jsonPath()}, 'null'::jsonb) <> ALL({array})" : $"COALESCE({jsonPath()}, 'null'::jsonb) = ANY({array})";
        }

        private string Param(object value)
        {
            var name = $"p{_parameters.Count}";
            _parameters[name] = value;
            return "@" + name;
        }

        private static string ToJson(object? value) => value is null ? "null" : JsonSerializer.Serialize(value);
    }
}
