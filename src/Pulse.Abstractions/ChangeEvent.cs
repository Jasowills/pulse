namespace Pulse.Abstractions;

/// <summary>A single state change to a document in a logical source.</summary>
public sealed record ChangeEvent(
    string Source,
    ChangeKind Kind,
    string DocumentId,
    IReadOnlyDictionary<string, object?>? FullDocument,
    IReadOnlyDictionary<string, object?>? UpdatedFields,
    ResumeToken Token,
    DateTimeOffset Timestamp);
