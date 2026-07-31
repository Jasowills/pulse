namespace Pulse.Abstractions;

/// <summary>
/// Opaque provider-specific resume point. <see cref="ProviderId"/> lets a registry
/// validate that a token was issued by the same provider/instance (e.g.
/// "mongo:orders-db") so we fail loudly instead of silently misinterpreting a token
/// from a different provider or collection.
/// </summary>
public sealed record ResumeToken(string ProviderId, byte[] Opaque);
