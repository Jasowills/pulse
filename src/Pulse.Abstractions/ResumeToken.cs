namespace Pulse.Abstractions;

/// <summary>
/// Opaque provider-specific resume point. <see cref="ProviderId"/> lets a registry
/// validate that a token was issued by the same provider/instance (e.g.
/// "mongo:orders-db") so we fail loudly instead of silently misinterpreting a token
/// from a different provider or collection.
/// </summary>
public sealed record ResumeToken(string ProviderId, byte[] Opaque)
{
    public bool Equals(ResumeToken? other)
        => other is not null
           && string.Equals(ProviderId, other.ProviderId, StringComparison.Ordinal)
           && Opaque.AsSpan().SequenceEqual(other.Opaque);

    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(ProviderId, StringComparer.Ordinal);
        foreach (var b in Opaque)
        {
            hash.Add(b);
        }

        return hash.ToHashCode();
    }
}
