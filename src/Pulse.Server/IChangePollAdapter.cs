namespace Pulse.Server;

using Pulse.Abstractions;

/// <summary>
/// Port at the <see cref="SharedWatchCoordinator"/> seam. Each provider implements this
/// adapter so the coordinator can remain provider-agnostic while owning fanout,
/// backoff, and handle lifecycle. Pruning floor stays provider-private via
/// <see cref="OnFloorAdvanced"/>.
/// </summary>
public interface IChangePollAdapter
{
    string ProviderIdFor(string resolvedSource);

    Task<ResumeToken> GetCurrentPositionAsync(string resolvedSource, CancellationToken cancellationToken);

    /// <summary>Fetch changes strictly after <paramref name="after"/>, oldest first.</summary>
    Task<PollBatch> PollAsync(string resolvedSource, ResumeToken after, CancellationToken cancellationToken);

    /// <summary>Wait for a signal (e.g. LISTEN notify) or until <see cref="SharedWatchCoordinatorOptions.PollInterval"/> elapses.</summary>
    Task WaitAsync(string resolvedSource, CancellationToken cancellationToken);

    /// <summary>Notify that the shared watch's floor advanced. Default no-op; Postgres overrides to drive prune.</summary>
    void OnFloorAdvanced(string resolvedSource, ResumeToken floor) { }
}

/// <summary>Batched poll result. Coordinator delivers <see cref="Changes"/> in order and advances to <see cref="NewPosition"/>.</summary>
public sealed record PollBatch(IReadOnlyList<ChangeEvent> Changes, ResumeToken NewPosition);
