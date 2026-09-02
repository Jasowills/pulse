using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using MongoDB.Bson;
using MongoDB.Driver;
using Pulse.Abstractions;
using Pulse.Server;

namespace Pulse.Mongo;

/// <summary>
/// <see cref="IChangeSource"/> backed by MongoDB change streams.
/// Watches each distinct collection at most once: subscriptions on the same source
/// (without a resume token) share one underlying change stream and are fanned out
/// internally. Watch calls that carry a resume token open a private watch, since a
/// cursor resumed from an arbitrary point cannot be shared.
/// </summary>
public sealed class MongoChangeSource : IChangeSource, IChangePollAdapter
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

    private readonly IMongoDatabase _database;
    private readonly ILogger _logger;
    private readonly SharedWatchCoordinator _coordinator;

    public MongoChangeSource(IMongoDatabase database, ILogger<MongoChangeSource>? logger = null)
    {
        _database = database ?? throw new ArgumentNullException(nameof(database));
        _logger = logger ?? NullLogger<MongoChangeSource>.Instance;
        _coordinator = new SharedWatchCoordinator(this, new SharedWatchCoordinatorOptions());
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
            return await _coordinator.SubscribeResumedAsync(source, resumeFrom, onChange, cancellationToken).ConfigureAwait(false);
        }

        return await _coordinator.SubscribeAsync(source, onChange, cancellationToken).ConfigureAwait(false);
    }

    // IChangePollAdapter
    public async Task<ResumeToken> GetCurrentPositionAsync(string resolvedSource, CancellationToken cancellationToken)
    {
        var collection = _database.GetCollection<BsonDocument>(resolvedSource);
        var opaque = await CaptureAsOfTokenAsync(collection, resolvedSource, cancellationToken).ConfigureAwait(false);
        return new ResumeToken(ProviderIdFor(resolvedSource), opaque);
    }

    public async Task<PollBatch> PollAsync(string resolvedSource, ResumeToken after, CancellationToken cancellationToken)
    {
        var cursor = await OpenCursorAsync(resolvedSource, after, cancellationToken).ConfigureAwait(false);
        try
        {
            if (!await cursor.MoveNextAsync(cancellationToken).ConfigureAwait(false))
            {
                return new PollBatch(Array.Empty<ChangeEvent>(), after);
            }

            var batch = new List<ChangeEvent>();
            foreach (var change in cursor.Current)
            {
                batch.Add(ToChangeEvent(change, resolvedSource));
            }

            if (batch.Count == 0)
            {
                return new PollBatch(Array.Empty<ChangeEvent>(), after);
            }

            return new PollBatch(batch, batch[^1].Token);
        }
        finally
        {
            cursor.Dispose();
        }
    }

    public Task WaitAsync(string resolvedSource, CancellationToken cancellationToken)
        => Task.CompletedTask;

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

    private void ValidateProviderId(string source, ResumeToken resumeFrom) => resumeFrom.EnsureProvider(ProviderIdFor(source));

    /// <summary>Registers a callback on a shared per-source watch, creating it on first use.</summary>

    /// <summary>Opens a dedicated watch resumed from the given token.</summary>

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


}
