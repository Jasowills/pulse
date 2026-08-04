using System.Collections.Concurrent;
using Pulse.Abstractions;
using Pulse.Client;
using Pulse.TestApp.Core;

namespace Pulse.TestApp.Harness;

/// <summary>
/// A client-side mirror of a subscription's current document set, keyed by document
/// id, that keeps a running list of observed changes (for coalescing/volume stats)
/// and signals when the first snapshot has been applied. Used by the harness the same
/// way the MAUI app's list/detail screens use <c>Current</c>.
/// </summary>
public sealed class ListModel
{
    private readonly object _gate = new();
    private readonly ConcurrentDictionary<string, Order> _items = new(StringComparer.Ordinal);
    private readonly ConcurrentQueue<PulseChange<Order>> _changes = new();
    private readonly TaskCompletionSource _firstSnapshot = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public Order[] Snapshot()
    {
        lock (_gate) return _items.Values.OrderBy(o => o.Id, StringComparer.Ordinal).ToArray();
    }

    public Task FirstSnapshotTask => _firstSnapshot.Task;

    public int ChangeCount => _changes.Count;

    public IReadOnlyList<PulseChange<Order>> Changes => _changes.ToArray();

    public Order? Get(string id)
    {
        return _items.TryGetValue(id, out var order) ? order : null;
    }

    public bool Contains(string id) => _items.ContainsKey(id);

    public bool Matches(Order expected)
    {
        return _items.TryGetValue(expected.Id, out var actual)
            && actual.Status == expected.Status
            && actual.Region == expected.Region
            && actual.Total == expected.Total;
    }

    public void Bind(IPulseSubscription<Order> subscription)
    {
        subscription.OnSnapshot += OnSnapshot;
        subscription.OnChange += OnChange;

        // The subscription processes messages on a background task, so a snapshot can be applied
        // (Current populated, OnSnapshot already fired) before Bind attaches — sync from Current so
        // the model can never miss the initial snapshot.
        var current = subscription.Current;
        if (current.Count > 0)
        {
            lock (_gate)
            {
                _items.Clear();
                foreach (var document in current)
                {
                    _items[document.Id] = document;
                }
            }

            _firstSnapshot.TrySetResult();
        }
    }

    public void Unbind(IPulseSubscription<Order> subscription)
    {
        subscription.OnSnapshot -= OnSnapshot;
        subscription.OnChange -= OnChange;
    }

    private void OnSnapshot(IReadOnlyList<Order> documents)
    {
        lock (_gate)
        {
            _items.Clear();
            foreach (var document in documents)
            {
                _items[document.Id] = document;
            }
        }

        _firstSnapshot.TrySetResult();
    }

    private void OnChange(PulseChange<Order> change)
    {
        _changes.Enqueue(change);

        lock (_gate)
        {
            switch (change.Kind)
            {
                case ChangeKind.Delete:
                    _items.TryRemove(change.DocumentId, out _);
                    break;
                case ChangeKind.Insert:
                case ChangeKind.Update:
                case ChangeKind.Replace:
                    if (change.Document is not null)
                    {
                        _items[change.DocumentId] = change.Document;
                    }
                    break;
            }
        }
    }
}
