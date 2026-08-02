using Pulse.Abstractions;

namespace Pulse.Client;

/// <summary>
/// A single document change delivered to a subscription, with the post-change document
/// deserialized to the subscription's type argument.
/// </summary>
public sealed record PulseChange<T>(
    ChangeKind Kind,
    string DocumentId,
    T? Document,
    IReadOnlyDictionary<string, object?>? UpdatedFields,
    DateTimeOffset Timestamp);
