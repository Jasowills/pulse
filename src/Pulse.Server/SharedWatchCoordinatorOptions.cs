namespace Pulse.Server;

public sealed class SharedWatchCoordinatorOptions
{
    /// <summary>Base poll interval for WaitAsync fallback and backoff scaling. Default 250 ms.</summary>
    public TimeSpan PollInterval { get; init; } = TimeSpan.FromMilliseconds(250);

    /// <summary>Maximum backoff delay after consecutive failures. Default 30 s.</summary>
    public TimeSpan MaxBackoff { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>Retries of a stale shared-watch token before wiping to current position with a warning. Default 3.</summary>
    public int MaxStaleRetries { get; init; } = 3;

    internal static TimeSpan BackoffDelay(TimeSpan pollInterval, TimeSpan maxBackoff, int failures)
    {
        var capped = Math.Min(failures, 6);
        var ms = pollInterval.TotalMilliseconds * Math.Pow(2, capped);
        return TimeSpan.FromMilliseconds(Math.Min(ms, maxBackoff.TotalMilliseconds));
    }
}
