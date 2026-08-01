using Pulse.Abstractions;

namespace Pulse.TestSupport;

/// <summary>
/// In-memory <see cref="IChangeSource"/> for tests: watches receive events published
/// through <see cref="PublishAsync"/>. Tracks active watch counts per source so tests
/// can assert that watches are disposed when the last subscriber leaves.
/// </summary>
public sealed class FakeChangeSource : IChangeSource
{
    private readonly object _sync = new();
    private readonly Dictionary<string, List<Func<ChangeEvent, Task>>> _watches = new(StringComparer.Ordinal);

    /// <summary>When set, <see cref="WatchAsync"/> throws this exception (simulates provider failure at start).</summary>
    public Exception? StartException { get; set; }

    /// <summary>
    /// When set, <see cref="GetSnapshotAsync"/> delegates to this delegate; otherwise it
    /// returns an empty document set. Tests use this to control snapshot contents and delay.
    /// </summary>
    public Func<string, SubscriptionFilter, CancellationToken, Task<(IReadOnlyList<IReadOnlyDictionary<string, object?>> Documents, ResumeToken AsOf)>>?
        SnapshotProvider { get; set; }

    /// <summary>Number of <see cref="GetSnapshotAsync"/> calls so far.</summary>
    public int SnapshotCallCount { get; private set; }

    /// <summary>The filter of the most recent <see cref="GetSnapshotAsync"/> call.</summary>
    public SubscriptionFilter? LastSnapshotFilter { get; private set; }

    public string ProviderIdFor(string source) => $"fake:{source}";

    public Task<IAsyncDisposable> WatchAsync(
        string source,
        Func<ChangeEvent, Task> onChange,
        ResumeToken? resumeFrom,
        CancellationToken cancellationToken)
    {
        if (resumeFrom is not null)
        {
            throw new ResumeTokenInvalidException("FakeChangeSource does not support resume tokens.");
        }

        if (StartException is not null)
        {
            throw StartException;
        }

        lock (_sync)
        {
            if (!_watches.TryGetValue(source, out var callbacks))
            {
                callbacks = new List<Func<ChangeEvent, Task>>();
                _watches[source] = callbacks;
            }

            callbacks.Add(onChange);
        }

        return Task.FromResult<IAsyncDisposable>(new FakeWatchHandle(this, source, onChange));
    }

    public async Task<(IReadOnlyList<IReadOnlyDictionary<string, object?>> Documents, ResumeToken AsOf)>
        GetSnapshotAsync(string source, SubscriptionFilter filter, CancellationToken cancellationToken)
    {
        SnapshotCallCount++;
        LastSnapshotFilter = filter;
        if (SnapshotProvider is not null)
        {
            return await SnapshotProvider(source, filter, cancellationToken).ConfigureAwait(false);
        }

        return (
            Array.Empty<IReadOnlyDictionary<string, object?>>(),
            new ResumeToken(ProviderIdFor(source), Array.Empty<byte>()));
    }

    /// <summary>Delivers a change to every active watch on <paramref name="change"/>.Source.</summary>
    public Task PublishAsync(ChangeEvent change)
    {
        Func<ChangeEvent, Task>[] callbacks;
        lock (_sync)
        {
            if (!_watches.TryGetValue(change.Source, out var list))
            {
                return Task.CompletedTask;
            }

            callbacks = list.ToArray();
        }

        return Task.WhenAll(callbacks.Select(c => c(change)));
    }

    public int ActiveWatchCount(string source)
    {
        lock (_sync)
        {
            return _watches.TryGetValue(source, out var list) ? list.Count : 0;
        }
    }

    private sealed class FakeWatchHandle : IAsyncDisposable
    {
        private readonly FakeChangeSource _owner;
        private readonly string _source;
        private readonly Func<ChangeEvent, Task> _onChange;
        private bool _disposed;

        public FakeWatchHandle(FakeChangeSource owner, string source, Func<ChangeEvent, Task> onChange)
        {
            _owner = owner;
            _source = source;
            _onChange = onChange;
        }

        public ValueTask DisposeAsync()
        {
            if (_disposed)
            {
                return ValueTask.CompletedTask;
            }

            _disposed = true;
            lock (_owner._sync)
            {
                if (_owner._watches.TryGetValue(_source, out var callbacks))
                {
                    callbacks.RemoveAll(c => ReferenceEquals(c, _onChange));
                    if (callbacks.Count == 0)
                    {
                        _owner._watches.Remove(_source);
                    }
                }
            }

            return ValueTask.CompletedTask;
        }
    }
}
