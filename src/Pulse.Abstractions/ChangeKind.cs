using System.Text.Json.Serialization;

namespace Pulse.Abstractions;

[JsonConverter(typeof(ChangeKindJsonConverter))]
public enum ChangeKind
{
    Insert,
    Update,
    Replace,
    Delete,
}
