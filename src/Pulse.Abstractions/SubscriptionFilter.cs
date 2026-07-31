using System.Text.Json.Serialization;

namespace Pulse.Abstractions;

/// <summary>
/// A subscription to a logical source, optionally filtered.
/// <c>Where == null</c> means "match everything in this source".
/// Serializes to the wire shape <c>{"source": "...", "where": {...}}</c>.
/// </summary>
public sealed record SubscriptionFilter(
    [property: JsonPropertyName("source")] string Source,
    [property: JsonPropertyName("where")] FilterExpr? Where);
