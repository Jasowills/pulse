using System.Collections;

namespace Pulse.Abstractions;

public enum CompareOp
{
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,
    In,
    NotIn,
    Exists,
}

/// <summary>
/// Provider-neutral subscription filter expression. Deliberately not raw Mongo query
/// syntax, so it maps cleanly onto both a BSON document (v0.1) and a SQL row (later).
/// </summary>
[System.Text.Json.Serialization.JsonConverter(typeof(Pulse.Abstractions.Json.FilterExprJsonConverterFactory))]
public abstract record FilterExpr;

/// <summary>A field comparison against a document, e.g. <c>status == "pending"</c>.</summary>
[System.Text.Json.Serialization.JsonConverter(typeof(Pulse.Abstractions.Json.FilterExprJsonConverterFactory))]
public sealed record FieldCompare(string Field, CompareOp Op, object? Value) : FilterExpr
{
    public bool Equals(FieldCompare? other)
        => other is not null
           && string.Equals(Field, other.Field, StringComparison.Ordinal)
           && Op == other.Op
           && FilterValueHelpers.Equal(Value, other.Value);

    public override int GetHashCode()
        => HashCode.Combine(Field, Op, FilterValueHelpers.Hash(Value));
}

/// <summary>All clauses must match.</summary>
[System.Text.Json.Serialization.JsonConverter(typeof(Pulse.Abstractions.Json.FilterExprJsonConverterFactory))]
public sealed record And(IReadOnlyList<FilterExpr> Clauses) : FilterExpr
{
    public bool Equals(And? other)
        => other is not null && Clauses.SequenceEqual(other.Clauses);

    public override int GetHashCode()
    {
        var hash = new HashCode();
        foreach (var clause in Clauses)
        {
            hash.Add(clause);
        }

        return hash.ToHashCode();
    }
}

/// <summary>At least one clause must match.</summary>
[System.Text.Json.Serialization.JsonConverter(typeof(Pulse.Abstractions.Json.FilterExprJsonConverterFactory))]
public sealed record Or(IReadOnlyList<FilterExpr> Clauses) : FilterExpr
{
    public bool Equals(Or? other)
        => other is not null && Clauses.SequenceEqual(other.Clauses);

    public override int GetHashCode()
    {
        var hash = new HashCode();
        foreach (var clause in Clauses)
        {
            hash.Add(clause);
        }

        return hash.ToHashCode();
    }
}

/// <summary>The clause must not match.</summary>
[System.Text.Json.Serialization.JsonConverter(typeof(Pulse.Abstractions.Json.FilterExprJsonConverterFactory))]
public sealed record Not(FilterExpr Clause) : FilterExpr;

/// <summary>
/// Value comparison helpers implementing Pulse's explicit coercion semantics: numeric
/// values compare numerically across CLR numeric types, strings compare by value, lists
/// compare structurally. Used by filter expression equality and by filter matchers.
/// </summary>
public static class FilterValueHelpers
{
    public static bool Equal(object? a, object? b)
    {
        if (ReferenceEquals(a, b))
        {
            return true;
        }

        if (a is null || b is null)
        {
            return false;
        }

        if (a is string || b is string)
        {
            return a.Equals(b);
        }

        // Numeric coercion: a BSON int field vs a JSON long filter value (etc.) must compare
        // numerically, not by runtime type — the spec calls this out as the #1 source of
        // "filter silently doesn't match" bugs (see README).
        if (IsNumeric(a) && IsNumeric(b))
        {
            return CompareNumeric(a, b) == 0;
        }

        if (a is IEnumerable ea && b is IEnumerable eb)
        {
            var e1 = ea.GetEnumerator();
            var e2 = eb.GetEnumerator();
            while (true)
            {
                var m1 = e1.MoveNext();
                var m2 = e2.MoveNext();
                if (m1 != m2)
                {
                    return false;
                }

                if (!m1)
                {
                    return true;
                }

                if (!Equal(e1.Current, e2.Current))
                {
                    return false;
                }
            }
        }

        return a.Equals(b);
    }

    /// <summary>Ordered comparison of two values, or null when the values aren't comparable.</summary>
    public static int? Compare(object? a, object? b)
    {
        if (a is null || b is null)
        {
            return null;
        }

        if (IsNumeric(a) && IsNumeric(b))
        {
            return CompareNumeric(a, b);
        }

        if (a is string sa && b is string sb)
        {
            return string.CompareOrdinal(sa, sb);
        }

        if (a is DateTimeOffset dtoA && b is DateTimeOffset dtoB)
        {
            return dtoA.CompareTo(dtoB);
        }

        if (a is DateTime dtA && b is DateTime dtB)
        {
            return dtA.CompareTo(dtB);
        }

        if (a is IComparable comparable && a.GetType() == b?.GetType())
        {
            return comparable.CompareTo(b);
        }

        return null;
    }

    public static bool IsNumeric(object value)
        => value is byte or sbyte or short or ushort or int or uint or long or ulong or float or double or decimal;

    private static int CompareNumeric(object a, object b)
    {
        try
        {
            return Convert.ToDecimal(a, System.Globalization.CultureInfo.InvariantCulture)
                .CompareTo(Convert.ToDecimal(b, System.Globalization.CultureInfo.InvariantCulture));
        }
        catch (Exception)
        {
            return -1; // NaN/Infinity and similar: treat as less-than so ordering stays total.
        }
    }

    public static int Hash(object? value)
    {
        if (value is null)
        {
            return 0;
        }

        if (value is IEnumerable e && value is not string)
        {
            var hash = new HashCode();
            foreach (var item in e)
            {
                hash.Add(Hash(item));
            }

            return hash.ToHashCode();
        }

        return value.GetHashCode();
    }
}
