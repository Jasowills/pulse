using System.Text.Json;

namespace Pulse.Abstractions.Json;

/// <summary>
/// Single place that decides how a JSON number becomes a CLR value.
/// Wire contract: integral numbers that fit in Int64 become <see cref="long"/>,
/// otherwise <see cref="double"/>. Keeps <c>ObjectToInferredTypesConverter</c>,
/// provider <c>JsonValueConverter</c>s, and <c>FilterValueHelpers</c> in agreement.
/// </summary>
public static class ClrValueCoercer
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
        return element.ValueKind switch
        {
            JsonValueKind.Null => null,
            JsonValueKind.String => element.GetString(),
            JsonValueKind.Number => element.TryGetInt64(out var i64) ? (object)i64
                : element.TryGetUInt64(out var u64) ? (object)u64
                : element.GetDouble(),
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.Object => element.EnumerateObject()
                .ToDictionary(p => p.Name, p => ToClrValue(p.Value)),
            JsonValueKind.Array => element.EnumerateArray().Select(ToClrValue).ToList(),
            _ => null,
        };
    }
}
