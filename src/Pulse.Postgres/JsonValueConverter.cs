using System.Text.Json;

namespace Pulse.Postgres;

/// <summary>
/// Converts Postgres <c>jsonb</c> values to plain CLR values for the provider-neutral
/// wire types, mirroring the Mongo provider's <c>BsonValueConverter</c>: numbers arrive
/// as <see cref="long"/> when they fit and <see cref="double"/> otherwise, so client-side
/// numeric assertions and the <c>ObjectToInferredTypesConverter</c> agree.
/// </summary>
internal static class JsonValueConverter
{
    public static IReadOnlyDictionary<string, object?> ToDictionary(JsonElement element)
    {
        var dict = new Dictionary<string, object?>(StringComparer.Ordinal);
        foreach (var property in element.EnumerateObject())
        {
            dict[property.Name] = ToClrValue(property.Value);
        }

        return dict;
    }

    public static object? ToClrValue(JsonElement element)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.Null:
                return null;
            case JsonValueKind.String:
                return element.GetString();
            case JsonValueKind.Number:
                if (element.TryGetInt64(out var integral))
                {
                    return integral;
                }

                return element.GetDouble();
            case JsonValueKind.True:
            case JsonValueKind.False:
                return element.GetBoolean();
            case JsonValueKind.Object:
                var dict = new Dictionary<string, object?>(StringComparer.Ordinal);
                foreach (var property in element.EnumerateObject())
                {
                    dict[property.Name] = ToClrValue(property.Value);
                }

                return dict;
            case JsonValueKind.Array:
                var list = new List<object?>(element.GetArrayLength());
                foreach (var item in element.EnumerateArray())
                {
                    list.Add(ToClrValue(item));
                }

                return list;
            default:
                return null;
        }
    }
}
