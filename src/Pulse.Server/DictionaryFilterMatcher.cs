using System.Collections;
using Pulse.Abstractions;

namespace Pulse.Server;

/// <summary>
/// Provider-neutral <see cref="IFilterMatcher"/> that evaluates <see cref="FilterExpr"/>
/// against plain dictionaries — the same shape every <c>IChangeSource</c> produces, which is
/// why live events need no provider-specific matching (only snapshot queries do).
///
/// Path semantics: dot notation descends through nested dictionaries; a numeric segment
/// indexes into a list; a non-numeric segment on a list matches when any element matches
/// the remaining path (Mongo-like). Comparison semantics follow <see cref="FilterValueHelpers"/>
/// (numeric coercion, structural equality for lists).
/// </summary>
public sealed class DictionaryFilterMatcher : IFilterMatcher
{
    public static readonly DictionaryFilterMatcher Instance = new();

    public bool Matches(IReadOnlyDictionary<string, object?> document, SubscriptionFilter filter)
    {
        ArgumentNullException.ThrowIfNull(document);
        ArgumentNullException.ThrowIfNull(filter);

        return filter.Where is null || MatchesExpr(document, filter.Where);
    }

    private static bool MatchesExpr(IReadOnlyDictionary<string, object?> document, FilterExpr expr)
    {
        return expr switch
        {
            FieldCompare compare => MatchesCompare(document, compare),
            And and => and.Clauses.All(clause => MatchesExpr(document, clause)),
            Or or => or.Clauses.Any(clause => MatchesExpr(document, clause)),
            Not not => !MatchesExpr(document, not.Clause),
            _ => throw new NotSupportedException($"Unsupported filter expression '{expr.GetType().Name}'."),
        };
    }

    private static bool MatchesCompare(IReadOnlyDictionary<string, object?> document, FieldCompare compare)
    {
        var resolved = ResolvePath(document, compare.Field);
        var present = resolved.Resolved;
        var values = resolved.Values;

        return compare.Op switch
        {
            CompareOp.Eq => present && values.Any(v => FilterValueHelpers.Equal(v, compare.Value)),
            CompareOp.Ne => !present || !values.Any(v => FilterValueHelpers.Equal(v, compare.Value)),
            CompareOp.Gt => present && values.Any(v => OrderingHolds(v, compare.Value, static c => c > 0)),
            CompareOp.Gte => present && values.Any(v => OrderingHolds(v, compare.Value, static c => c >= 0)),
            CompareOp.Lt => present && values.Any(v => OrderingHolds(v, compare.Value, static c => c < 0)),
            CompareOp.Lte => present && values.Any(v => OrderingHolds(v, compare.Value, static c => c <= 0)),
            CompareOp.In => present && values.Any(v => AnyEqualAny(v, compare.Value)),
            CompareOp.NotIn => !present || !values.Any(v => AnyEqualAny(v, compare.Value)),
            CompareOp.Exists => present && values.Any(static v => v is not null),
            _ => throw new NotSupportedException($"Unsupported comparison operator '{compare.Op}'."),
        };
    }

    private static bool OrderingHolds(object? fieldValue, object? filterValue, Func<int, bool> holds)
    {
        var comparison = FilterValueHelpers.Compare(fieldValue, filterValue);
        return comparison is not null && holds(comparison.Value);
    }

    private static bool AnyEqualAny(object? fieldValue, object? filterValue)
    {
        var candidates = filterValue is IEnumerable filterList and not string
            ? filterList.Cast<object?>()
            : new[] { filterValue };

        var fieldValues = fieldValue is IEnumerable fieldList and not string
            ? fieldList.Cast<object?>()
            : new[] { fieldValue };

        foreach (var field in fieldValues)
        {
            foreach (var candidate in candidates)
            {
                if (FilterValueHelpers.Equal(field, candidate))
                {
                    return true;
                }
            }
        }

        return false;
    }

    private static (bool Resolved, object?[] Values) ResolvePath(
        IReadOnlyDictionary<string, object?> document,
        string path)
    {
        var dot = path.IndexOf('.');
        var segment = dot < 0 ? path : path[..dot];
        var remaining = dot < 0 ? null : path[(dot + 1)..];

        if (!document.TryGetValue(segment, out var value))
        {
            return (false, Array.Empty<object?>());
        }

        if (remaining is null)
        {
            return (true, new[] { value });
        }

        return ResolveValue(value, remaining);
    }

    private static (bool Resolved, object?[] Values) ResolveValue(object? value, string path)
    {
        switch (value)
        {
            case IReadOnlyDictionary<string, object?> nested:
                return ResolvePath(nested, path);

            case IReadOnlyList<object?> list:
                var dot = path.IndexOf('.');
                var segment = dot < 0 ? path : path[..dot];
                var remaining = dot < 0 ? null : path[(dot + 1)..];

                if (int.TryParse(segment, out var index))
                {
                    if (index < 0 || index >= list.Count)
                    {
                        return (false, Array.Empty<object?>());
                    }

                    return remaining is null
                        ? (true, new[] { list[index] })
                        : ResolveValue(list[index], remaining);
                }

                // Element-wise match: any element matching the remaining path counts.
                var matches = new List<object?>();
                foreach (var element in list)
                {
                    if (element is IReadOnlyDictionary<string, object?> elementDoc)
                    {
                        var result = ResolvePath(elementDoc, path);
                        if (result.Resolved)
                        {
                            matches.AddRange(result.Values);
                        }
                    }
                    else if (remaining is null)
                    {
                        matches.Add(element);
                    }
                }

                return matches.Count > 0
                    ? (true, matches.ToArray())
                    : (false, Array.Empty<object?>());

            default:
                return (false, Array.Empty<object?>());
        }
    }
}
