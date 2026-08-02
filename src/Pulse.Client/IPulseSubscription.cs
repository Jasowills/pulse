using Pulse.Abstractions;

namespace Pulse.Client;

/// <summary>
/// A live subscription to a source. <see cref="Current"/> is a maintained local cache of
/// matching documents keyed by <c>_id</c>; raw events are still exposed via
/// <see cref="OnSnapshot"/> and <see cref="OnChange"/>.
/// </summary>
public interface IPulseSubscription<T>
{
    string Id { get; }

    string Source { get; }

    /// <summary>All currently matching documents, keyed by <c>_id</c>.</summary>
    IReadOnlyList<T> Current { get; }

    /// <summary>Raised once when the initial matching snapshot arrives (and on client reconnect).</summary>
    event Action<IReadOnlyList<T>>? OnSnapshot;

    /// <summary>Raised for every live change after the snapshot.</summary>
    event Action<PulseChange<T>>? OnChange;

    Task UnsubscribeAsync(CancellationToken cancellationToken = default);
}
