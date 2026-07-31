namespace Pulse.Abstractions;

/// <summary>Decides whether a document matches a <see cref="SubscriptionFilter"/>.</summary>
public interface IFilterMatcher
{
    bool Matches(IReadOnlyDictionary<string, object?> document, SubscriptionFilter filter);
}
