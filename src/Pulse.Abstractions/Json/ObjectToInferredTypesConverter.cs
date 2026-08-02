using System.Text.Json;
using System.Text.Json.Serialization;

namespace Pulse.Abstractions.Json;

/// <summary>
/// Deserializes object-typed JSON values into concrete CLR types (long/double/string/bool,
/// nested dictionaries/lists) instead of <see cref="JsonElement"/>, matching the shapes
/// produced by <c>IChangeSource</c> implementations. Applied to the SignalR JSON protocol
/// so both the server and the SDK deserialize document payloads identically.
/// </summary>
public sealed class ObjectToInferredTypesConverter : JsonConverter<object>
{
    public override object? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        switch (reader.TokenType)
        {
            case JsonTokenType.True:
                return true;
            case JsonTokenType.False:
                return false;
            case JsonTokenType.Number:
                return reader.TryGetInt64(out var integral)
                    ? integral
                    : (object?)reader.GetDouble();
            case JsonTokenType.String:
                return reader.GetString();
            case JsonTokenType.StartObject:
                using (var document = JsonDocument.ParseValue(ref reader))
                {
                    return document.RootElement.EnumerateObject()
                        .ToDictionary(p => p.Name, p => ReadElement(p.Value));
                }

            case JsonTokenType.StartArray:
                using (var document = JsonDocument.ParseValue(ref reader))
                {
                    return document.RootElement.EnumerateArray().Select(ReadElement).ToList();
                }

            default:
                return JsonSerializer.Deserialize(ref reader, typeof(object), options);
        }
    }

    public override void Write(Utf8JsonWriter writer, object value, JsonSerializerOptions options)
        => JsonSerializer.Serialize(writer, value, value.GetType(), options);

    private static object? ReadElement(JsonElement element)
    {
        return element.ValueKind switch
        {
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.Number => element.TryGetInt64(out var integral)
                ? integral
                : (object?)element.GetDouble(),
            JsonValueKind.String => element.GetString(),
            JsonValueKind.Object => element.EnumerateObject()
                .ToDictionary(p => p.Name, p => ReadElement(p.Value)),
            JsonValueKind.Array => element.EnumerateArray().Select(ReadElement).ToList(),
            _ => null,
        };
    }
}
