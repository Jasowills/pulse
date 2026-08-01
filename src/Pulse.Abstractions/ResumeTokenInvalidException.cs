namespace Pulse.Abstractions;

/// <summary>
/// Thrown by an <see cref="IChangeSource"/> when a supplied <see cref="ResumeToken"/>
/// is stale or invalid (e.g. the Mongo oplog has rolled off), so callers can resync
/// from a fresh snapshot rather than silently skipping events.
/// </summary>
public sealed class ResumeTokenInvalidException : Exception
{
    public ResumeTokenInvalidException(string message) : base(message)
    {
    }

    public ResumeTokenInvalidException(string message, Exception? innerException) : base(message, innerException)
    {
    }
}
