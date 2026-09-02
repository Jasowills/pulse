using System.Text.Json;
using Pulse.Abstractions.Json;

namespace Pulse.Postgres;

/// <summary>Thin adapter over <see cref="ClrValueCoercer"/> — keeps provider import local.</summary>
internal static class JsonValueConverter
{
    public static IReadOnlyDictionary<string, object?> ToDictionary(JsonElement element) => ClrValueCoercer.ToDictionary(element);
    public static object? ToClrValue(JsonElement element) => ClrValueCoercer.ToClrValue(element);
}
