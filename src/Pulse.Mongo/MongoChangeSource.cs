using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using MongoDB.Bson;
using MongoDB.Driver;
using Pulse.Abstractions;

namespace Pulse.Mongo;

/// <summary>
/// <see cref="IChangeSource"/> backed by MongoDB change streams.
/// Watches each distinct collection at most once: subscriptions on the same source
/// (without a resume token) share one underlying change stream and are fanned out
/// internally. Watch calls that carry a resume token open a private watch, since a
/// cursor resumed from an arbitrary point cannot be shared.
/// </summary>
public sealed class MongoChangeSource : IChangeSource
{
    private static readonly PipelineDefinition<ChangeStreamDocument<BsonDocument>, ChangeStreamDocument<BsonDocument>> Pipeline =
        new EmptyPipelineDefinition<ChangeStreamDocument<BsonDocument>>().Match(
            Builders<ChangeStreamDocument<BsonDocument>>.Filter.In(
                change => change.OperationType,
                new[]
                {
                    ChangeStreamOperationType.Insert,
                    ChangeStreamOperationType.Update,
                    ChangeStreamOperationType.Replace,
                    ChangeStreamOperationType.Delete,
                }));

    private readonly object _sync = new();
    private readonly Dictionary<string, SharedWatch> _sharedWatches = new(StringComparer.Ordinal);
    private readonly IMongoDatabase _database;
    private readonly ILogger _logger;

    public MongoChangeSource(IMongoDatabase database, ILogger<MongoChangeSource>? logger = null)
    {
        _database = database ?? throw new ArgumentNullException(nameof(database));
        _logger = logger ?? NullLogger<MongoChangeSource>.Instance;
    }

    public string ProviderIdFor(string source)
        => $"mongo:{_database.DatabaseNamespace.DatabaseName}.{source}";

    public async Task<IAsyncDisposable> WatchAsync(
        string source,
        Func<ChangeEvent, Task> onChange,
        ResumeToken? resumeFrom,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(source))
        {
            throw new ArgumentException("Source must be a non-empty collection name.", nameof(source));
        }

        if (onChange is null)
        {
            throw new ArgumentNullException(nameof(onChange));
        }

        if (resumeFrom is not null)
        {
            ValidateProviderId(source, resumeFrom);
            return await OpenPrivateWatchAsync(source, onChange, resumeFrom, cancellationToken).ConfigureAwait(false);
        }

        return await RegisterSharedAsync(source, onChange, cancellationToken).ConfigureAwait(false);
    }

    public async Task<(IReadOnlyList<IReadOnlyDictionary<string, object?>> Documents, ResumeToken AsOf)>
        GetSnapshotAsync(string source, SubscriptionFilter filter, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(source))
        {
            throw new ArgumentException("Source must be a non-empty collection name.", nameof(source));
        }

        if (filter is null)
        {
            throw new ArgumentNullException(nameof(filter));
        }

        if (filter.Where is not null && !string.Equals(filter.Source, source, StringComparison.Ordinal))
        {
            throw new ArgumentException(
                $"Filter was built for source '{filter.Source}' but the snapshot targets '{source}'.",
                nameof(filter));
        }

        var collection = _database.GetCollection<BsonDocument>(source);

        // Watch-first, then snapshot (see README "Resume tokens and gapless delivery"): the
        // as-of token is captured before the snapshot query runs, so changes at or after it
        // supersede the snapshot and the caller can deliver without a gap or duplicate.
        var asOf = await CaptureAsOfTokenAsync(collection, source, cancellationToken).ConfigureAwait(false);

        var where = filter.Where is null
            ? Builders<BsonDocument>.Filter.Empty
            : MongoFilterTranslator.Translate(filter.Where);

        var documents = new List<IReadOnlyDictionary<string, object?>>();
        using var cursor = await collection
            .FindAsync(where, cancellationToken: cancellationToken)
            .ConfigureAwait(false);
        while (await cursor.MoveNextAsync(cancellationToken).ConfigureAwait(false))
        {
            foreach (var document in cursor.Current)
            {
                documents.Add(BsonValueConverter.ToDictionary(document));
            }
        }

        return (documents, new ResumeToken(ProviderIdFor(source), asOf));
    }

    /// <summary>
    /// Captures the change stream's current resume token without consuming events: the
    /// initial <see cref="IMongoCollection{TDocument}.WatchAsync"/> response carries the
    /// cursor's start position, exposed via <see cref="IChangeStreamCursor{TDocument}.GetResumeToken"/>.
    /// </summary>
    private async Task<byte[]> CaptureAsOfTokenAsync(
        IMongoCollection<BsonDocument> collection,
        string source,
        CancellationToken cancellationToken)
    {
        using var cursor = await collection.WatchAsync(Pipeline, cancellationToken: cancellationToken).ConfigureAwait(false);
        while (true)
        {
            if (TryGetResumeToken(cursor) is { } token)
            {
                return MongoResumeTokenCodec.Encode(token);
            }

            if (!await cursor.MoveNextAsync(cancellationToken).ConfigureAwait(false))
            {
                break;
            }
        }

        throw new InvalidOperationException(
            $"Change stream for '{ProviderIdFor(source)}' closed without yielding a resume token.");
    }

    /// <summary>Returns the cursor's resume token, or null when the initial response has not arrived yet.</summary>
    private static BsonDocument? TryGetResumeToken(
        IChangeStreamCursor<ChangeStreamDocument<BsonDocument>> cursor)
    {
        try
        {
            return cursor.GetResumeToken();
        }
        catch (InvalidOperationException)
        {
            return null;
        }
    }

    private void ValidateProviderId(string source, ResumeToken resumeFrom)
    {
        var expected = ProviderIdFor(source);
        if (!string.Equals(resumeFrom.ProviderId, expected, StringComparison.Ordinal))
        {
            throw new ResumeTokenInvalidException(
                $"Resume token was issued by '{resumeFrom.ProviderId}', but watching '{expected}'. Refusing to misinterpret the token.");
        }
    }

    /// <summary>Registers a callback on a shared per-source watch, creating it on first use.</summary>
    private async Task<IAsyncDisposable> RegisterSharedAsync(
        string source,
        Func<ChangeEvent, Task> onChange,
        CancellationToken cancellationToken)
    {
        SharedWatch shared;
        lock (_sync)
        {
            if (!_sharedWatches.TryGetValue(source, out shared!))
            {
                shared = new SharedWatch(this, source);
                _sharedWatches[source] = shared;
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
                    && _sharedWatches.TryGetValue(source, out var current)
                    && ReferenceEquals(current, shared))
                {
                    _sharedWatches.Remove(source);
                    shared.DisposeCore();
                }
            }

            throw;
        }

        return new SharedSubscriptionHandle(this, source, shared, onChange, cancellationToken);
    }

    /// <summary>Opens a dedicated watch resumed from the given token.</summary>
    private async Task<IAsyncDisposable> OpenPrivateWatchAsync(
        string source,
        Func<ChangeEvent, Task> onChange,
        ResumeToken resumeFrom,
        CancellationToken cancellationToken)
    {
        var cursor = await OpenCursorAsync(source, resumeFrom, cancellationToken).ConfigureAwait(false);
        var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var loop = PumpAsync(cursor, source, onChange, onResumeToken: null, cts.Token);
        return new PrivateWatchHandle(cursor, cts, loop);
    }

    private async Task<IChangeStreamCursor<ChangeStreamDocument<BsonDocument>>> OpenCursorAsync(
        string source,
        ResumeToken? resumeFrom,
        CancellationToken cancellationToken)
    {
        var collection = _database.GetCollection<BsonDocument>(source);
        var options = new ChangeStreamOptions
        {
            FullDocument = ChangeStreamFullDocumentOption.UpdateLookup,
            ResumeAfter = resumeFrom is null ? null : MongoResumeTokenCodec.Decode(resumeFrom.Opaque),
        };

        try
        {
            return await collection.WatchAsync(Pipeline, options, cancellationToken).ConfigureAwait(false);
        }
        catch (MongoException ex) when (MongoErrors.IsResumeInvalid(ex))
        {
            throw new ResumeTokenInvalidException(
                $"Resume token for '{ProviderIdFor(source)}' is stale or invalid: {ex.Message}", ex);
        }
    }

    private async Task PumpAsync(
        IChangeStreamCursor<ChangeStreamDocument<BsonDocument>> cursor,
        string source,
        Func<ChangeEvent, Task> onChange,
        Action<BsonDocument>? onResumeToken,
        CancellationToken cancellationToken)
    {
        try
        {
            while (await cursor.MoveNextAsync(cancellationToken).ConfigureAwait(false))
            {
                foreach (var change in cursor.Current)
                {
                    await onChange(ToChangeEvent(change, source)).ConfigureAwait(false);
                }

                if (onResumeToken is not null && TryGetResumeToken(cursor) is { } token)
                {
                    onResumeToken(token);
                }
            }
        }
        catch (OperationCanceledException)
        {
        }
        catch (MongoException ex) when (MongoErrors.IsResumeInvalid(ex))
        {
            throw new ResumeTokenInvalidException(
                $"Resume token for '{ProviderIdFor(source)}' became stale or invalid while watching: {ex.Message}", ex);
        }
        finally
        {
            cursor.Dispose();
        }
    }

    private ChangeEvent ToChangeEvent(ChangeStreamDocument<BsonDocument> change, string source)
    {
        var kind = change.OperationType switch
        {
            ChangeStreamOperationType.Insert => ChangeKind.Insert,
            ChangeStreamOperationType.Update => ChangeKind.Update,
            ChangeStreamOperationType.Replace => ChangeKind.Replace,
            ChangeStreamOperationType.Delete => ChangeKind.Delete,
            _ => throw new InvalidOperationException(
                $"Unsupported change stream operation type '{change.OperationType}'."),
        };

        var timestamp = change.WallTime is { } wallTime
            ? new DateTimeOffset(DateTime.SpecifyKind(wallTime, DateTimeKind.Utc))
            : DateTimeOffset.UtcNow;

        return new ChangeEvent(
            Source: source,
            Kind: kind,
            DocumentId: FormatDocumentId(change.DocumentKey?["_id"]),
            FullDocument: change.FullDocument is null ? null : BsonValueConverter.ToDictionary(change.FullDocument),
            UpdatedFields: kind == ChangeKind.Update && change.UpdateDescription?.UpdatedFields is { } updatedFields
                ? BsonValueConverter.ToDictionary(updatedFields)
                : null,
            Token: new ResumeToken(ProviderIdFor(source), MongoResumeTokenCodec.Encode(change.ResumeToken)),
            Timestamp: timestamp);
    }

    private static string FormatDocumentId(BsonValue? id)
    {
        return id switch
        {
            null => string.Empty,
            BsonNull => string.Empty,
            BsonObjectId objectId => objectId.ToString(),
            BsonString str => str.ToString(),
            _ => id.ToJson(),
        };
    }

    /// <summary>A shared per-source watch that fans out to multiple subscribers.</summary>
    private sealed class SharedWatch
    {
        private readonly MongoChangeSource _owner;
        private readonly string _source;
        private readonly List<Func<ChangeEvent, Task>> _subscribers = new();
        private readonly ILogger _logger;
        private CancellationTokenSource? _cts;
        private Task? _startTask;
        private byte[]? _lastResumeToken;
        private int _consecutiveFailures;
        private int _staleRetries;

        private const int MaxStaleRetries = 3;

        public SharedWatch(MongoChangeSource owner, string source)
        {
            _owner = owner;
            _source = source;
            _logger = owner._logger;
        }

        public void AddSubscriber(Func<ChangeEvent, Task> onChange) => _subscribers.Add(onChange);

        public int RemoveSubscriber(Func<ChangeEvent, Task> onChange)
            => _subscribers.RemoveAll(s => ReferenceEquals(s, onChange));

        /// <summary>Returns a task that completes when the underlying cursor is open (or start failed).</summary>
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
                var cursor = await _owner.OpenCursorAsync(_source, ResumeFrom(), cts.Token).ConfigureAwait(false);
                _ = Task.Run(() => RunSupervisedAsync(cursor, cts));
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

        /// <summary>
        /// Runs the change stream and restarts it after a failure with capped exponential
        /// backoff, resuming from the last seen resume token. A cursor that cannot resume from
        /// its token (the oplog rolled off) falls back to a fresh stream after a few retries,
        /// logging the gap, so transient outages don't permanently kill the shared watch.
        /// </summary>
        private async Task RunSupervisedAsync(
            IChangeStreamCursor<ChangeStreamDocument<BsonDocument>>? initialCursor,
            CancellationTokenSource cts)
        {
            var cursor = initialCursor;
            while (!cts.IsCancellationRequested)
            {
                try
                {
                    if (cursor is null)
                    {
                        cursor = await _owner.OpenCursorAsync(_source, ResumeFrom(), cts.Token).ConfigureAwait(false);
                    }

                    await _owner.PumpAsync(cursor, _source, FanOutAsync, OnResumeToken, cts.Token).ConfigureAwait(false);
                    return; // stream ended normally (e.g. an invalidate on collection drop)
                }
                catch (OperationCanceledException)
                {
                    return;
                }
                catch (ResumeTokenInvalidException)
                {
                    cursor = null;
                    if (++_staleRetries >= MaxStaleRetries)
                    {
                        _logger.LogWarning(
                            "Change stream for source '{Source}' cannot be resumed from its stored token; restarting from the current position. Events in the gap may be missed.",
                            _source);
                        lock (_owner._sync)
                        {
                            _lastResumeToken = null;
                        }
                    }

                    if (!await DelayAsync(BackoffDelay(_consecutiveFailures++), cts.Token))
                    {
                        return;
                    }
                }
                catch (Exception ex)
                {
                    cursor = null;
                    if (!await DelayAsync(BackoffDelay(_consecutiveFailures++), cts.Token))
                    {
                        return;
                    }

                    _logger.LogError(ex, "Change stream for source '{Source}' failed; retrying.", _source);
                }
            }
        }

        private void OnResumeToken(BsonDocument token)
        {
            _consecutiveFailures = 0;
            _staleRetries = 0;
            lock (_owner._sync)
            {
                _lastResumeToken = MongoResumeTokenCodec.Encode(token);
            }
        }

        private ResumeToken? ResumeFrom()
        {
            lock (_owner._sync)
            {
                return _lastResumeToken is { } opaque
                    ? new ResumeToken(_owner.ProviderIdFor(_source), opaque)
                    : null;
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

        private static TimeSpan BackoffDelay(int failures)
        {
            var capped = Math.Min(failures, 6);
            var ms = 250 * Math.Pow(2, capped);
            return TimeSpan.FromMilliseconds(Math.Min(ms, 30000));
        }

        private async Task FanOutAsync(ChangeEvent change)
        {
            Func<ChangeEvent, Task>[] subscribers;
            lock (_owner._sync)
            {
                subscribers = _subscribers.ToArray();
            }

            foreach (var subscriber in subscribers)
            {
                try
                {
                    await subscriber(change).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _owner._logger.LogError(ex, "Subscriber callback failed for source '{Source}'.", _source);
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
        private readonly MongoChangeSource _owner;
        private readonly string _source;
        private readonly SharedWatch _shared;
        private readonly Func<ChangeEvent, Task> _onChange;
        private readonly CancellationTokenRegistration _registration;
        private bool _disposed;

        public SharedSubscriptionHandle(
            MongoChangeSource owner,
            string source,
            SharedWatch shared,
            Func<ChangeEvent, Task> onChange,
            CancellationToken cancellationToken)
        {
            _owner = owner;
            _source = source;
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
            lock (_owner._sync)
            {
                if (_shared.RemoveSubscriber(_onChange) == 0
                    && _owner._sharedWatches.TryGetValue(_source, out var current)
                    && ReferenceEquals(current, _shared))
                {
                    _owner._sharedWatches.Remove(_source);
                    _shared.DisposeCore();
                }
            }
        }

        public ValueTask DisposeAsync()
        {
            UnsubscribeSync();
            return ValueTask.CompletedTask;
        }
    }

    private sealed class PrivateWatchHandle : IAsyncDisposable
    {
        private readonly IChangeStreamCursor<ChangeStreamDocument<BsonDocument>> _cursor;
        private readonly CancellationTokenSource _cts;
        private readonly Task _loop;
        private bool _disposed;

        public PrivateWatchHandle(
            IChangeStreamCursor<ChangeStreamDocument<BsonDocument>> cursor,
            CancellationTokenSource cts,
            Task loop)
        {
            _cursor = cursor;
            _cts = cts;
            _loop = loop;
        }

        /// <summary>Faults with <see cref="ResumeTokenInvalidException"/> if the token is lost while watching.</summary>
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
                // Surfaced via Completion; disposal is not the place to rethrow.
            }

            _cursor.Dispose();
            _cts.Dispose();
        }
    }
}
