using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Pulse.Abstractions;

namespace Pulse.Server;

/// <summary>
/// Deep module that owns shared-watch fanout, capped exponential backoff, and
/// handle lifecycle. Providers supply <see cref="IChangePollAdapter"/> so this
/// module stays provider-agnostic. Pruning floor notification stays provider-private
/// via <see cref="IChangePollAdapter.OnFloorAdvanced"/>.
/// </summary>
public sealed class SharedWatchCoordinator : IAsyncDisposable
{
    private readonly IChangePollAdapter _adapter;
    private readonly SharedWatchCoordinatorOptions _options;
    private readonly ILogger _logger;
    private readonly object _sync = new();
    private readonly Dictionary<string, SharedWatch> _sharedWatches = new(StringComparer.Ordinal);

    public SharedWatchCoordinator(
        IChangePollAdapter adapter,
        SharedWatchCoordinatorOptions? options = null,
        ILogger<SharedWatchCoordinator>? logger = null)
    {
        _adapter = adapter ?? throw new ArgumentNullException(nameof(adapter));
        _options = options ?? new SharedWatchCoordinatorOptions();
        _logger = logger ?? NullLogger<SharedWatchCoordinator>.Instance;
    }

    public string ProviderIdFor(string resolvedSource) => _adapter.ProviderIdFor(resolvedSource);

    /// <summary>Shared per-source watch — multiplexed, resilient with backoff.</summary>
    public async ValueTask<IAsyncDisposable> SubscribeAsync(
        string resolvedSource,
        Func<ChangeEvent, Task> onChange,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(resolvedSource);
        ArgumentNullException.ThrowIfNull(onChange);

        SharedWatch shared;
        lock (_sync)
        {
            if (!_sharedWatches.TryGetValue(resolvedSource, out shared!))
            {
                shared = new SharedWatch(this, resolvedSource);
                _sharedWatches[resolvedSource] = shared;
            }

            shared.AddSubscriber(onChange);
        }

        try
        {
            await shared.EnsureStartedAsync().ConfigureAwait(false);
        }
        catch
        {
            lock (_sync)
            {
                if (shared.RemoveSubscriber(onChange) == 0
                    && _sharedWatches.TryGetValue(resolvedSource, out var current)
                    && ReferenceEquals(current, shared))
                {
                    _sharedWatches.Remove(resolvedSource);
                    shared.DisposeCore();
                }
            }

            throw;
        }

        return new SharedSubscriptionHandle(this, resolvedSource, shared, onChange, cancellationToken);
    }

    /// <summary>Private watch resumed from an explicit token — not shareable, fails fast on stale.</summary>
    public async Task<IAsyncDisposable> SubscribeResumedAsync(
        string resolvedSource,
        ResumeToken resumeFrom,
        Func<ChangeEvent, Task> onChange,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(resolvedSource);
        ArgumentNullException.ThrowIfNull(resumeFrom);
        ArgumentNullException.ThrowIfNull(onChange);

        ValidateProviderId(resolvedSource, resumeFrom);

        var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var loop = PumpPrivateAsync(resolvedSource, onChange, resumeFrom, cts.Token);
        return new PrivateWatchHandle(cts, loop);
    }

    private void ValidateProviderId(string resolvedSource, ResumeToken token)
    {
        var expected = _adapter.ProviderIdFor(resolvedSource);
        if (!string.Equals(token.ProviderId, expected, StringComparison.Ordinal))
        {
            throw new ResumeTokenInvalidException(
                $"Resume token was issued by '{token.ProviderId}', but watching '{expected}'. Refusing to misinterpret the token.");
        }
    }

    private async Task PumpPrivateAsync(
        string resolvedSource,
        Func<ChangeEvent, Task> onChange,
        ResumeToken from,
        CancellationToken cancellationToken)
    {
        var current = from;
        while (!cancellationToken.IsCancellationRequested)
        {
            PollBatch batch;
            try
            {
                batch = await _adapter.PollAsync(resolvedSource, current, cancellationToken).ConfigureAwait(false);
            }
            catch (ResumeTokenInvalidException)
            {
                throw;
            }

            foreach (var change in batch.Changes)
            {
                await onChange(change).ConfigureAwait(false);
            }

            current = batch.NewPosition;

            await _adapter.WaitAsync(resolvedSource, cancellationToken).ConfigureAwait(false);
        }
    }

    private void RemoveSharedIfEmpty(string resolvedSource, SharedWatch shared, Func<ChangeEvent, Task> onChange)
    {
        lock (_sync)
        {
            if (shared.RemoveSubscriber(onChange) == 0
                && _sharedWatches.TryGetValue(resolvedSource, out var current)
                && ReferenceEquals(current, shared))
            {
                _sharedWatches.Remove(resolvedSource);
                shared.DisposeCore();
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        List<SharedWatch> watches;
        lock (_sync)
        {
            watches = new List<SharedWatch>(_sharedWatches.Values);
            _sharedWatches.Clear();
        }

        foreach (var w in watches)
        {
            w.DisposeCore();
        }

        await Task.CompletedTask;
    }

    private sealed class SharedWatch
    {
        private readonly SharedWatchCoordinator _owner;
        private readonly string _resolvedSource;
        private readonly List<Func<ChangeEvent, Task>> _subscribers = new();
        private readonly ILogger _logger;
        private CancellationTokenSource? _cts;
        private Task? _startTask;
        private ResumeToken? _lastPosition;
        private int _consecutiveFailures;
        private int _staleRetries;

        public SharedWatch(SharedWatchCoordinator owner, string resolvedSource)
        {
            _owner = owner;
            _resolvedSource = resolvedSource;
            _logger = owner._logger;
        }

        public void AddSubscriber(Func<ChangeEvent, Task> onChange) => _subscribers.Add(onChange);

        public int RemoveSubscriber(Func<ChangeEvent, Task> onChange)
            => _subscribers.RemoveAll(s => ReferenceEquals(s, onChange));

        public Task EnsureStartedAsync()
        {
            lock (_owner._sync)
            {
                if (_startTask is null)
                {
                    _cts = new CancellationTokenSource();
                    _startTask = StartCoreAsync(_cts);
                }

                return _startTask;
            }
        }

        private async Task StartCoreAsync(CancellationTokenSource cts)
        {
            try
            {
                _lastPosition = await _owner._adapter.GetCurrentPositionAsync(_resolvedSource, cts.Token).ConfigureAwait(false);
                _ = Task.Run(() => RunSupervisedAsync(cts), cts.Token);
            }
            catch (Exception)
            {
                lock (_owner._sync)
                {
                    _startTask = null;
                }

                throw;
            }
        }

        private async Task RunSupervisedAsync(CancellationTokenSource cts)
        {
            while (!cts.IsCancellationRequested)
            {
                try
                {
                    var batch = await _owner._adapter.PollAsync(_resolvedSource, _lastPosition!, cts.Token).ConfigureAwait(false);

                    if (batch.Changes.Count > 0)
                    {
                        await FanOutAsync(batch.Changes).ConfigureAwait(false);
                        _consecutiveFailures = 0;
                        _staleRetries = 0;
                        lock (_owner._sync)
                        {
                            _lastPosition = batch.NewPosition;
                        }

                        _owner._adapter.OnFloorAdvanced(_resolvedSource, batch.NewPosition);
                    }
                    else
                    {
                        _consecutiveFailures = 0;
                    }

                    await _owner._adapter.WaitAsync(_resolvedSource, cts.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
                catch (ResumeTokenInvalidException)
                {
                    if (++_staleRetries >= _owner._options.MaxStaleRetries)
                    {
                        _logger.LogWarning(
                            "Change watch for source '{Source}' cannot be resumed from its stored token; restarting from current position. Events in the gap may be missed.",
                            _resolvedSource);
                        try
                        {
                            _lastPosition = await _owner._adapter.GetCurrentPositionAsync(_resolvedSource, cts.Token).ConfigureAwait(false);
                            _owner._adapter.OnFloorAdvanced(_resolvedSource, _lastPosition);
                        }
                        catch (OperationCanceledException)
                        {
                            return;
                        }

                        _staleRetries = 0;
                    }

                    if (!await DelayAsync(BackoffDelay(_consecutiveFailures++), cts.Token).ConfigureAwait(false))
                    {
                        return;
                    }
                }
                catch (Exception ex)
                {
                    var delay = BackoffDelay(_consecutiveFailures++);
                    _logger.LogError(ex, "Change watch for source '{Source}' failed; retrying in {Delay} ms.", _resolvedSource, (int)delay.TotalMilliseconds);
                    if (!await DelayAsync(delay, cts.Token).ConfigureAwait(false))
                    {
                        return;
                    }
                }
            }
        }

        private static async Task<bool> DelayAsync(TimeSpan delay, CancellationToken token)
        {
            try
            {
                await Task.Delay(delay, token).ConfigureAwait(false);
                return true;
            }
            catch (OperationCanceledException)
            {
                return false;
            }
        }

        private TimeSpan BackoffDelay(int failures)
            => SharedWatchCoordinatorOptions.BackoffDelay(_owner._options.PollInterval, _owner._options.MaxBackoff, failures);

        private async Task FanOutAsync(IReadOnlyList<ChangeEvent> changes)
        {
            Func<ChangeEvent, Task>[] subscribers;
            lock (_owner._sync)
            {
                subscribers = _subscribers.ToArray();
            }

            foreach (var change in changes)
            {
                foreach (var subscriber in subscribers)
                {
                    try
                    {
                        await subscriber(change).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Subscriber callback failed for source '{Source}'.", _resolvedSource);
                    }
                }
            }
        }

        public void DisposeCore()
        {
            var cts = _cts;
            if (cts is null)
            {
                return;
            }

            _cts = null;
            cts.Cancel();
            cts.Dispose();
        }
    }

    private sealed class SharedSubscriptionHandle : IAsyncDisposable
    {
        private readonly SharedWatchCoordinator _owner;
        private readonly string _resolvedSource;
        private readonly SharedWatch _shared;
        private readonly Func<ChangeEvent, Task> _onChange;
        private readonly CancellationTokenRegistration _registration;
        private bool _disposed;

        public SharedSubscriptionHandle(
            SharedWatchCoordinator owner,
            string resolvedSource,
            SharedWatch shared,
            Func<ChangeEvent, Task> onChange,
            CancellationToken cancellationToken)
        {
            _owner = owner;
            _resolvedSource = resolvedSource;
            _shared = shared;
            _onChange = onChange;
            _registration = cancellationToken.Register(UnsubscribeSync);
        }

        private void UnsubscribeSync()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            _registration.Unregister();
            _owner.RemoveSharedIfEmpty(_resolvedSource, _shared, _onChange);
        }

        public ValueTask DisposeAsync()
        {
            UnsubscribeSync();
            return ValueTask.CompletedTask;
        }
    }

    private sealed class PrivateWatchHandle : IAsyncDisposable
    {
        private readonly CancellationTokenSource _cts;
        private readonly Task _loop;
        private bool _disposed;

        public PrivateWatchHandle(CancellationTokenSource cts, Task loop)
        {
            _cts = cts;
            _loop = loop;
        }

        public Task Completion => _loop;

        public async ValueTask DisposeAsync()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            _cts.Cancel();
            try
            {
                await _loop.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
            catch (ResumeTokenInvalidException)
            {
            }

            _cts.Dispose();
        }
    }
}
