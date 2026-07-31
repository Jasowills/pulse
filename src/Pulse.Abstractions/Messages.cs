using System.Text.Json.Serialization;

namespace Pulse.Abstractions;

/// <summary>Server-to-client notification that a watched document changed (wire: <c>PulseChange</c>).</summary>
public sealed record PulseChangeMessage(
    [property: JsonPropertyName("subscriptionId")] string SubscriptionId,
    [property: JsonPropertyName("kind")] ChangeKind Kind,
    [property: JsonPropertyName("documentId")] string DocumentId,
    [property: JsonPropertyName("document")] IReadOnlyDictionary<string, object?>? Document,
    [property: JsonPropertyName("updatedFields")] IReadOnlyDictionary<string, object?>? UpdatedFields,
    [property: JsonPropertyName("timestamp")] DateTimeOffset Timestamp)
{
    /// <summary>Builds the client-facing message for a change, dropping provider-only fields.</summary>
    public static PulseChangeMessage FromChangeEvent(ChangeEvent change, string subscriptionId)
    {
        return new PulseChangeMessage(
            SubscriptionId: subscriptionId,
            Kind: change.Kind,
            DocumentId: change.DocumentId,
            Document: change.Kind == ChangeKind.Delete ? null : change.FullDocument,
            UpdatedFields: change.Kind == ChangeKind.Update ? change.UpdatedFields : null,
            Timestamp: change.Timestamp);
    }
}

/// <summary>Server-to-client initial snapshot for a subscription (wire: <c>PulseSnapshot</c>).</summary>
public sealed record PulseSnapshotMessage(
    [property: JsonPropertyName("subscriptionId")] string SubscriptionId,
    [property: JsonPropertyName("documents")] IReadOnlyList<IReadOnlyDictionary<string, object?>> Documents);

/// <summary>Server-to-client error notification (wire: <c>PulseError</c>).</summary>
public sealed record PulseErrorMessage(
    [property: JsonPropertyName("subscriptionId")] string? SubscriptionId,
    [property: JsonPropertyName("message")] string Message);
