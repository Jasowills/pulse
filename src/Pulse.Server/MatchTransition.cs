using Pulse.Abstractions;

namespace Pulse.Server;

/// <summary>
/// Pure match-transition logic — no I/O, no locks, no SignalR.
/// Decides what a filtered subscription sees for a single <see cref="ChangeEvent"/>.
/// Extracted from <see cref="SubscriptionRegistry"/> so it is testable without a hub.
/// </summary>
public static class MatchTransition
{
    /// <param name="trackedIds">Ids currently matching the subscription (mutated in place, caller holds lock).</param>
    public static ChangeEvent? DecideDelivery(
        ChangeEvent change,
        FilterExpr? where,
        HashSet<string> trackedIds,
        IFilterMatcher matcher,
        string source)
    {
        if (where is null) return change;

        switch (change.Kind)
        {
            case ChangeKind.Delete:
                trackedIds.Remove(change.DocumentId);
                return change;

            case ChangeKind.Insert:
                if (change.FullDocument is null || matcher.Matches(change.FullDocument, new SubscriptionFilter(source, where)))
                {
                    trackedIds.Add(change.DocumentId);
                    return change;
                }
                return null;

            case ChangeKind.Update:
            case ChangeKind.Replace:
                if (change.FullDocument is null) return change;
                if (matcher.Matches(change.FullDocument, new SubscriptionFilter(source, where)))
                {
                    return trackedIds.Add(change.DocumentId) ? change with { Kind = ChangeKind.Insert } : change;
                }
                return trackedIds.Remove(change.DocumentId) ? change with { Kind = ChangeKind.Delete, FullDocument = null } : null;

            default:
                return change;
        }
    }
}
