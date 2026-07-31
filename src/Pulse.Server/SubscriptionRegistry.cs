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
    private readonly ILogger<SubscriptionRegistry> _logger;
    private readonly object _sync = new();
    private readonly Dictionary<string, SourceState> _sources = new(StringComparer.Ordinal);
    private readonly Dictionary<string, (string Source, SourceState State)> _subscriptions = new(StringComparer.Ordinal);

    public SubscriptionRegistry(
        IChangeSource changeSource,
        IHubContext<PulseHub> hubContext,
        ILogger<SubscriptionRegistry> logger)
    {
        _changeSource = changeSource ?? throw new ArgumentNullException(nameof(changeSource));
        _hubContext = hubContext ?? throw new ArgumentNullException(nameof(hubContext));
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
        lock (_sync)
        {
            subscriptionId = $"sub-{Interlocked.Increment(ref _nextSubscriptionId)}";
            if (!_sources.TryGetValue(source, out state!))
            {
                state = new SourceState(source);
                _sources[source] = state;
            }

            state.Subscriptions[subscriptionId] = new Subscription(subscriptionId, connectionId, where);
            _subscriptions[subscriptionId] = (source, state);
            startTask = state.EnsureStartedAsync(_changeSource, FanOutAsync);
        }

        try
        {
            await startTask.ConfigureAwait(false);
        }
        catch
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

            throw;
        }

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
        entry.State.Subscriptions.Remove(subscriptionId);
        if (entry.State.Subscriptions.Count == 0
            && _sources.TryGetValue(entry.Source, out var current)
            && ReferenceEquals(current, entry.State))
        {
            _sources.Remove(entry.Source);
            return entry.State;
        }

        return null;
    }

    private async Task FanOutAsync(string source, ChangeEvent change)
    {
        Subscription[] subscriptions;
        lock (_sync)
        {
            if (!_sources.TryGetValue(source, out var state))
            {
                return;
            }

            subscriptions = state.Subscriptions.Values.ToArray();
        }

        foreach (var subscription in subscriptions)
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
                    source, subscription.ConnectionId);
            }
        }
    }

    private sealed record Subscription(string Id, string ConnectionId, FilterExpr? Where);

    private sealed class SourceState
    {
        public readonly string Source;
        public readonly Dictionary<string, Subscription> Subscriptions = new(StringComparer.Ordinal);

        private readonly object _sync = new();
        private CancellationTokenSource? _cts;
        private IAsyncDisposable? _handle;
        private Task? _startTask;
        private bool _disposed;

        public SourceState(string source)
        {
            Source = source;
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
                var handle = await changeSource
                    .WatchAsync(Source, change => onEvent(Source, change), null, cts.Token)
                    .ConfigureAwait(false);

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
