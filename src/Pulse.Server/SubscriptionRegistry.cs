using System.Threading.Channels;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Logging;
using Pulse.Abstractions;

namespace Pulse.Server;

/// <summary>
/// Owns the live watch for each source and fans out change events to every subscription on it.
/// One registry is registered per change-source provider (see the AddXxxSource extensions):
/// the hub routes a subscription to the first registry whose <see cref="CanHandle"/> accepts
/// the source. Subscriptions are delivered over SignalR as <see cref="PulseChangeMessage"/>.
/// </summary>
public class SubscriptionRegistry
{
    private static int _nextSubscriptionId;

    private readonly IChangeSource _changeSource;
    private readonly IHubContext<PulseHub> _hubContext;
    private readonly IFilterMatcher _matcher;
    private readonly IResumeTokenStore _resumeTokenStore;
    private readonly ILogger<SubscriptionRegistry> _logger;
    private readonly object _sync = new();
    private readonly Dictionary<string, SourceState> _sources = new(StringComparer.Ordinal);
    private readonly Dictionary<string, (string Source, SourceState State)> _subscriptions = new(StringComparer.Ordinal);

    public SubscriptionRegistry(
        IChangeSource changeSource,
        IHubContext<PulseHub> hubContext,
        ILogger<SubscriptionRegistry> logger,
        IResumeTokenStore? resumeTokenStore = null,
        IFilterMatcher? matcher = null)
    {
        _changeSource = changeSource ?? throw new ArgumentNullException(nameof(changeSource));
        _hubContext = hubContext ?? throw new ArgumentNullException(nameof(hubContext));
        _matcher = matcher ?? DictionaryFilterMatcher.Instance;
        _resumeTokenStore = resumeTokenStore ?? new InMemoryResumeTokenStore();
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <summary>
    /// Whether this registry can serve subscriptions for the given source. Defaults to true;
    /// providers that must scope sources override this.
    /// </summary>
    public virtual bool CanHandle(string source) => true;

    /// <summary>Registers a subscription and starts (or shares) the watch for the source.</summary>
    public async Task<string> SubscribeAsync(
        string connectionId,
        string source,
        FilterExpr? where,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(connectionId))
        {
            throw new ArgumentException("Connection id must not be empty.", nameof(connectionId));
        }

        if (string.IsNullOrWhiteSpace(source))
        {
            throw new ArgumentException("Source must be a non-empty name.", nameof(source));
        }

        string subscriptionId;
        SourceState state;
        Task startTask;
        Subscription subscription;
        lock (_sync)
        {
            subscriptionId = $"sub-{Interlocked.Increment(ref _nextSubscriptionId)}";
            if (!_sources.TryGetValue(source, out state!))
            {
                state = new SourceState(source, _changeSource.ProviderIdFor(source), _resumeTokenStore, _logger);
                _sources[source] = state;
            }

            subscription = new Subscription(
                subscriptionId,
                connectionId,
                where,
                Channel.CreateUnbounded<QueuedChange>());
            state.Subscriptions[subscriptionId] = subscription;
            _subscriptions[subscriptionId] = (source, state);
            startTask = state.EnsureStartedAsync(_changeSource, FanOutAsync);
        }

        try
        {
            await startTask.ConfigureAwait(false);
        }
        catch
        {
            await RemoveAndDisposeAsync(subscriptionId).ConfigureAwait(false);
            throw;
        }

        // Cut point for gapless delivery: every change emitted by the watch before the
        // snapshot query was requested is included in the snapshot, so it is dropped;
        // everything after is replayed after the snapshot and supersedes it.
        long cut;
        lock (_sync)
        {
            cut = state.EmittedCount;
        }

        (IReadOnlyList<IReadOnlyDictionary<string, object?>> Documents, ResumeToken AsOf) snapshot;
        try
        {
            snapshot = await _changeSource
                .GetSnapshotAsync(source, new SubscriptionFilter(source, where), cancellationToken)
                .ConfigureAwait(false);
        }
        catch
        {
            await RemoveAndDisposeAsync(subscriptionId).ConfigureAwait(false);
            throw;
        }

        try
        {
            await _hubContext.Clients.Client(connectionId)
                .SendAsync(
                    "PulseSnapshot",
                    new PulseSnapshotMessage(subscriptionId, snapshot.Documents),
                    CancellationToken.None)
                .ConfigureAwait(false);
        }
        catch
        {
            await RemoveAndDisposeAsync(subscriptionId).ConfigureAwait(false);
            throw;
        }

        // Drain from the channel in order; the writer is completed on unsubscribe/dispose.
        _ = Task.Run(() => DeliverLoopAsync(subscription, cut));

        return subscriptionId;
    }

    public async Task UnsubscribeAsync(string subscriptionId)
    {
        SourceState? toDispose;
        lock (_sync)
        {
            toDispose = RemoveSubscriptionLocked(subscriptionId);
        }

        if (toDispose is not null)
        {
            await toDispose.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>Removes every subscription owned by a connection (called on disconnect).</summary>
    public async Task RemoveConnectionAsync(string connectionId)
    {
        List<SourceState> toDispose = new();
        lock (_sync)
        {
            foreach (var (id, entry) in _subscriptions.ToList())
            {
                if (!entry.State.Subscriptions.TryGetValue(id, out var subscription)
                    || !string.Equals(subscription.ConnectionId, connectionId, StringComparison.Ordinal))
                {
                    continue;
                }

                var removed = RemoveSubscriptionLocked(id);
                if (removed is not null && !toDispose.Contains(removed))
                {
                    toDispose.Add(removed);
                }
            }
        }

        foreach (var state in toDispose)
        {
            await state.DisposeAsync().ConfigureAwait(false);
        }
    }

    private SourceState? RemoveSubscriptionLocked(string subscriptionId)
    {
        if (!_subscriptions.TryGetValue(subscriptionId, out var entry))
        {
            return null;
        }

        _subscriptions.Remove(subscriptionId);
        entry.State.Subscriptions.Remove(subscriptionId, out var removed);
        removed?.Queue.Writer.TryComplete();
        if (entry.State.Subscriptions.Count == 0
            && _sources.TryGetValue(entry.Source, out var current)
            && ReferenceEquals(current, entry.State))
        {
            _sources.Remove(entry.Source);
            return entry.State;
        }

        return null;
    }

    /// <summary>Removes a subscription and disposes its source watch when it was the last one.</summary>
    private async Task RemoveAndDisposeAsync(string subscriptionId)
    {
        SourceState? toDispose;
        lock (_sync)
        {
            toDispose = RemoveSubscriptionLocked(subscriptionId);
        }

        if (toDispose is not null)
        {
            await toDispose.DisposeAsync().ConfigureAwait(false);
        }
    }

    private async Task FanOutAsync(string source, ChangeEvent change)
    {
        Subscription[] subscriptions;
        SourceState state;
        long sequence;
        lock (_sync)
        {
            if (!_sources.TryGetValue(source, out state!))
            {
                return;
            }

            sequence = ++state.EmittedCount;
            subscriptions = state.Subscriptions.Values.ToArray();
        }

        foreach (var subscription in subscriptions)
        {
            if (!ShouldDeliver(change, subscription))
            {
                continue;
            }

            if (!subscription.Queue.Writer.TryWrite(new QueuedChange(sequence, change)))
            {
                _logger.LogWarning(
                    "Failed to enqueue change for source '{Source}' to subscription '{SubscriptionId}'.",
                    source, subscription.Id);
            }
        }

        // Persist the resume point after delivery enqueue so a restart replays at most
        // the events since the last save (at-least-once, never a silent gap).
        try
        {
            await _resumeTokenStore
                .SaveAsync(state.ResumeKey, change.Token, CancellationToken.None)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to persist resume token for source '{Source}'.", source);
        }
    }

    /// <summary>
    /// Sends queued changes in order, dropping everything at or before the subscription's
    /// snapshot cut point (those changes are already reflected in the snapshot).
    /// </summary>
    private async Task DeliverLoopAsync(Subscription subscription, long cut)
    {
        try
        {
            await foreach (var queued in subscription.Queue.Reader.ReadAllAsync().ConfigureAwait(false))
            {
                if (queued.Sequence <= cut)
                {
                    continue;
                }

                await DeliverChangeAsync(subscription, queued.Change).ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Delivery loop for subscription '{SubscriptionId}' failed.", subscription.Id);
        }
    }

    private async Task DeliverChangeAsync(Subscription subscription, ChangeEvent change)
    {
        try
        {
            await _hubContext.Clients.Client(subscription.ConnectionId)
                .SendAsync("PulseChange", PulseChangeMessage.FromChangeEvent(change, subscription.Id), CancellationToken.None)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Failed to deliver change for source '{Source}' to connection '{ConnectionId}'.",
                change.Source, subscription.ConnectionId);
        }
    }

    /// <summary>
    /// Applies the subscription's filter to document changes. Deletes are always delivered
    /// (their FullDocument is null, so they can't be evaluated — clients need the removal
    /// either way); changes with no document body are delivered rather than silently dropped.
    /// </summary>
    private bool ShouldDeliver(ChangeEvent change, Subscription subscription)
    {
        if (subscription.Where is null || change.Kind == ChangeKind.Delete || change.FullDocument is null)
        {
            return true;
        }

        return _matcher.Matches(change.FullDocument, new SubscriptionFilter(change.Source, subscription.Where));
    }

    private readonly record struct QueuedChange(long Sequence, ChangeEvent Change);

    private sealed record Subscription(
        string Id,
        string ConnectionId,
        FilterExpr? Where,
        Channel<QueuedChange> Queue);

    private sealed class SourceState
    {
        public readonly string Source;
        public readonly string ResumeKey;
        public readonly Dictionary<string, Subscription> Subscriptions = new(StringComparer.Ordinal);

        /// <summary>Number of changes the watch has emitted for this source (guarded by the registry lock).</summary>
        public long EmittedCount;

        private readonly IResumeTokenStore _store;
        private readonly ILogger<SubscriptionRegistry> _logger;
        private readonly object _sync = new();
        private CancellationTokenSource? _cts;
        private IAsyncDisposable? _handle;
        private Task? _startTask;
        private bool _disposed;

        public SourceState(
            string source,
            string resumeKey,
            IResumeTokenStore store,
            ILogger<SubscriptionRegistry> logger)
        {
            Source = source;
            ResumeKey = resumeKey;
            _store = store;
            _logger = logger;
        }

        /// <summary>Returns a task that completes when the underlying watch is open (or start failed).</summary>
        public Task EnsureStartedAsync(IChangeSource changeSource, Func<string, ChangeEvent, Task> onEvent)
        {
            lock (_sync)
            {
                if (_startTask is null)
                {
                    _cts = new CancellationTokenSource();
                    _startTask = StartCoreAsync(changeSource, onEvent, _cts);
                }

                return _startTask;
            }
        }

        private async Task StartCoreAsync(
            IChangeSource changeSource,
            Func<string, ChangeEvent, Task> onEvent,
            CancellationTokenSource cts)
        {
            try
            {
                // Resume from the persisted point when one exists; if it turned stale/invalid,
                // fall back to a fresh watch — the subscribe-time snapshot covers the gap
                // (the token is logged and deleted, never silently ignored).
                var resumeFrom = await _store.GetAsync(ResumeKey, cts.Token).ConfigureAwait(false);
                var handle = await OpenWatchAsync(changeSource, onEvent, resumeFrom, cts).ConfigureAwait(false);

                IAsyncDisposable? staleHandle = null;
                lock (_sync)
                {
                    if (_disposed)
                    {
                        staleHandle = handle;
                    }
                    else
                    {
                        _handle = handle;
                    }
                }

                if (staleHandle is not null)
                {
                    await staleHandle.DisposeAsync().ConfigureAwait(false);
                }
            }
            catch (Exception)
            {
                lock (_sync)
                {
                    _startTask = null;
                }

                throw;
            }
        }

        private async Task<IAsyncDisposable> OpenWatchAsync(
            IChangeSource changeSource,
            Func<string, ChangeEvent, Task> onEvent,
            ResumeToken? resumeFrom,
            CancellationTokenSource cts)
        {
            try
            {
                return await changeSource
                    .WatchAsync(Source, change => onEvent(Source, change), resumeFrom, cts.Token)
                    .ConfigureAwait(false);
            }
            catch (ResumeTokenInvalidException ex) when (resumeFrom is not null)
            {
                _logger.LogWarning(ex,
                    "Stored resume token for source '{Source}' is stale or invalid; resyncing from a fresh watch.",
                    Source);
                try
                {
                    await _store.DeleteAsync(ResumeKey, CancellationToken.None).ConfigureAwait(false);
                }
                catch (Exception deleteEx)
                {
                    _logger.LogError(deleteEx, "Failed to delete invalid resume token for source '{Source}'.", Source);
                }

                return await changeSource
                    .WatchAsync(Source, change => onEvent(Source, change), null, cts.Token)
                    .ConfigureAwait(false);
            }
        }

        public async Task DisposeAsync()
        {
            IAsyncDisposable? handle;
            CancellationTokenSource? cts;
            lock (_sync)
            {
                _disposed = true;
                handle = _handle;
                _handle = null;
                cts = _cts;
                _cts = null;
            }

            cts?.Cancel();
            if (handle is not null)
            {
                try
                {
                    await handle.DisposeAsync().ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    // Disposal must not throw into hub lifecycle paths; caller logs if it cares.
                    System.Diagnostics.Debug.WriteLine(ex);
                }
            }

            cts?.Dispose();
        }
    }
}
