using System.Text.Json;
using System.Text.Json.Serialization;

namespace Pulse.Abstractions;

/// <summary>
/// Serializes <see cref="ChangeKind"/> as lowercase wire names
/// (<c>insert</c>/<c>update</c>/<c>replace</c>/<c>delete</c>), matching the lowercase
/// operator style used by <see cref="FilterExpr"/>.
/// </summary>
public sealed class ChangeKindJsonConverter : JsonConverter<ChangeKind>
{
    public override ChangeKind Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        var token = reader.GetString();
        return token?.ToLowerInvariant() switch
        {
            "insert" => ChangeKind.Insert,
            "update" => ChangeKind.Update,
            "replace" => ChangeKind.Replace,
            "delete" => ChangeKind.Delete,
            _ => throw new JsonException($"Unknown ChangeKind '{token}'."),
        };
    }

    public override void Write(Utf8JsonWriter writer, ChangeKind value, JsonSerializerOptions options)
        => writer.WriteStringValue(value.ToString().ToLowerInvariant());
}
