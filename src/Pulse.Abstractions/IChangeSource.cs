namespace Pulse.Abstractions;

/// <summary>Abstracts a database change source (Mongo for v0.1, Postgres/SQL Server later).</summary>
public interface IChangeSource
{
    /// <summary>
    /// Begin watching a logical source (collection/table). Invokes <paramref name="onChange"/>
    /// for every matching change event. Returns a disposable that stops watching when disposed.
    /// If <paramref name="resumeFrom"/> is provided and valid, watching resumes from that point;
    /// if it is stale/invalid, implementations must throw <see cref="ResumeTokenInvalidException"/>
    /// so callers can decide to resync from a fresh snapshot rather than silently skipping events.
    /// </summary>
    Task<IAsyncDisposable> WatchAsync(
        string source,
        Func<ChangeEvent, Task> onChange,
        ResumeToken? resumeFrom,
        CancellationToken cancellationToken);

    /// <summary>
    /// Fetch an initial snapshot of documents matching a filter, for subscribe-time sync.
    /// Returns the matching documents plus a <see cref="ResumeToken"/> marking the "as of" point,
    /// so live watching can pick up immediately after without a gap or duplicate.
    /// </summary>
    Task<(IReadOnlyList<IReadOnlyDictionary<string, object?>> Documents, ResumeToken AsOf)>
        GetSnapshotAsync(string source, SubscriptionFilter filter, CancellationToken cancellationToken);
}
