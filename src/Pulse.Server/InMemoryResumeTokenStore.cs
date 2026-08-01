using System.Collections.Concurrent;
using Pulse.Abstractions;

namespace Pulse.Server;

/// <summary>
/// Process-local <see cref="IResumeTokenStore"/>. Correct within a running server, but does
/// NOT survive a restart — see README caveats. Register <see cref="FileResumeTokenStore"/>
/// instead for restart durability.
/// </summary>
public sealed class InMemoryResumeTokenStore : IResumeTokenStore
{
    private readonly ConcurrentDictionary<string, ResumeToken> _tokens = new(StringComparer.Ordinal);

    public Task<ResumeToken?> GetAsync(string key, CancellationToken cancellationToken)
        => Task.FromResult(_tokens.TryGetValue(key, out var token) ? token : null);

    public Task SaveAsync(string key, ResumeToken token, CancellationToken cancellationToken)
    {
        _tokens[key] = token;
        return Task.CompletedTask;
    }

    public Task DeleteAsync(string key, CancellationToken cancellationToken)
    {
        _tokens.TryRemove(key, out _);
        return Task.CompletedTask;
    }
}
