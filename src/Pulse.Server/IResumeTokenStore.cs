using Pulse.Abstractions;

namespace Pulse.Server;

/// <summary>
/// Persists the per-source resume point so watching can continue after a restart without
/// silently dropping events. Keys are provider-qualified source ids (see
/// <see cref="Abstractions.IChangeSource.ProviderIdFor"/>), so stores never mix tokens
/// from different providers or databases.
/// </summary>
public interface IResumeTokenStore
{
    /// <summary>Returns the stored resume token for a source, or null when none is stored.</summary>
    Task<ResumeToken?> GetAsync(string key, CancellationToken cancellationToken);

    /// <summary>Persists the latest resume token for a source (at-least-once semantics).</summary>
    Task SaveAsync(string key, ResumeToken token, CancellationToken cancellationToken);

    /// <summary>Drops the stored resume token (used when it turned out to be stale/invalid).</summary>
    Task DeleteAsync(string key, CancellationToken cancellationToken);
}
