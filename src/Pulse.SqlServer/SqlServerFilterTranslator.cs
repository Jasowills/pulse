using System.Collections;
using System.Globalization;
using System.Text;
using Pulse.Abstractions;
using Pulse.Abstractions.Filtering;

namespace Pulse.SqlServer;

public static class SqlServerFilterTranslator
{
    public static string Translate(FilterExpr expr, string? primaryKeyColumn, IDictionary<string, object> parameters)
    {
        ArgumentNullException.ThrowIfNull(expr);
        ArgumentNullException.ThrowIfNull(parameters);
        return new Translator(primaryKeyColumn, parameters).Translate(expr);
    }

    private sealed class Translator : FilterTranslatorBase<string>
    {
        private readonly string? _pk;
        private readonly IDictionary<string, object> _parameters;
        public Translator(string? pk, IDictionary<string, object> p) { _pk = pk; _parameters = p; }
        protected override string EmptyAnd() => "(1 = 1)";
        protected override string EmptyOr() => "(1 = 0)";
        protected override string CombineAnd(IEnumerable<string> c) => $"({string.Join(" AND ", c)})";
        protected override string CombineOr(IEnumerable<string> c) => $"({string.Join(" OR ", c)})";
        protected override string Negate(string c) => $"(NOT {c})";
        protected override string TranslateCompare(FieldCompare compare)
        {
            if (string.IsNullOrWhiteSpace(compare.Field)) throw new ArgumentException("Filter field must be a non-empty path.", nameof(compare));
            var (column, path) = ResolvePath(compare.Field, _pk);
            string TextRef() => path.Length == 0 ? QuoteIdent(column) : $"CASE WHEN ISJSON({QuoteIdent(column)}) = 1 THEN JSON_VALUE({QuoteIdent(column)}, '{JsonPath(path)}') ELSE NULL END";
            string NumericRef() => path.Length == 0 ? QuoteIdent(column) : $"CASE WHEN ISJSON({QuoteIdent(column)}) = 1 THEN TRY_CONVERT(decimal(38, 18), JSON_VALUE({QuoteIdent(column)}, '{JsonPath(path)}')) ELSE NULL END";
            switch (compare.Op)
            {
                case CompareOp.Exists: return $"{TextRef()} IS NOT NULL";
                case CompareOp.Eq:
                case CompareOp.Ne:
                    var eqOp = compare.Op == CompareOp.Eq ? "=" : "<>";
                    if (compare.Value is null) return $"{TextRef()} IS {(compare.Op == CompareOp.Eq ? "NULL" : "NOT NULL")}";
                    if (FilterValueHelpers.IsNumeric(compare.Value)) return $"{NumericRef()} {eqOp} {Param(Convert.ToDecimal(compare.Value, CultureInfo.InvariantCulture))}";
                    return $"{TextRef()} {eqOp} {Param(compare.Value)}";
                case CompareOp.In:
                case CompareOp.NotIn:
                    return TranslateMembership(compare, TextRef, NumericRef);
                case CompareOp.Gt:
                case CompareOp.Gte:
                case CompareOp.Lt:
                case CompareOp.Lte:
                    if (compare.Value is null) return "(1 = 0)";
                    var sqlOp = compare.Op switch { CompareOp.Gt => ">", CompareOp.Gte => ">=", CompareOp.Lt => "<", CompareOp.Lte => "<=", _ => throw new InvalidOperationException() };
                    if (FilterValueHelpers.IsNumeric(compare.Value)) return $"{NumericRef()} {sqlOp} {Param(Convert.ToDecimal(compare.Value, CultureInfo.InvariantCulture))}";
                    if (compare.Value is string) return $"{TextRef()} {sqlOp} {Param(compare.Value)}";
                    throw new NotSupportedException($"Range comparisons require a numeric or string filter value; got '{compare.Value.GetType().Name}'.");
                default: throw new NotSupportedException($"Unsupported comparison operator '{compare.Op}'.");
            }
        }
        private string TranslateMembership(FieldCompare compare, Func<string> textRef, Func<string> numericRef)
        {
            var enumerable = compare.Value as IEnumerable;
            if (enumerable is null || compare.Value is string) enumerable = new[] { compare.Value };
            var items = new List<object?>(); foreach (var i in enumerable) items.Add(i);
            if (items.Count == 0) return compare.Op == CompareOp.NotIn ? "(1 = 1)" : "(1 = 0)";
            var useNumeric = items.All(i => i is not null && FilterValueHelpers.IsNumeric(i));
            var placeholder = new List<string>(items.Count);
            foreach (var item in items) { object v = item is null ? DBNull.Value : useNumeric ? Convert.ToDecimal(item, CultureInfo.InvariantCulture) : item; placeholder.Add(Param(v)); }
            var list = string.Join(", ", placeholder);
            var target = useNumeric ? numericRef() : textRef();
            return compare.Op == CompareOp.NotIn ? $"{target} NOT IN ({list})" : $"{target} IN ({list})";
        }
        private static (string Column, string[] Path) ResolvePath(string field, string? pk)
        {
            var segs = field.Split('.'); if (segs.Length == 0 || segs[0].Length == 0) throw new ArgumentException("Filter field must be a non-empty path.", nameof(field));
            var col = segs[0]; if (string.Equals(col, "_id", StringComparison.Ordinal)) col = pk ?? "_id";
            return (col, segs.Skip(1).ToArray());
        }
        private static string JsonPath(string[] path) { var sb = new StringBuilder("$"); foreach (var s in path) { if (s.Length > 0 && s.All(static c => char.IsLetterOrDigit(c) || c == '_')) sb.Append('.').Append(s); else sb.Append("[\"").Append(s.Replace("\"", "\\\"", StringComparison.Ordinal)).Append("\"]"); } return sb.ToString(); }
        private string Param(object value) { var n = $"p{_parameters.Count}"; _parameters[n] = value; return "@" + n; }
        private static string QuoteIdent(string n) => "[" + n.Replace("]", "]]", StringComparison.Ordinal) + "]";
    }
}
