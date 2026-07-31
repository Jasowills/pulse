using System.Collections;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Pulse.Abstractions.Json;

/// <summary>
/// Factory that supplies an exact-typed <see cref="JsonConverter{T}"/> for every
/// <see cref="FilterExpr"/> node type, so both base-typed and concrete-typed
/// serialization resolve the same wire format:
/// <c>{ "field", "op", "value" }</c> for a compare, <c>{ "and" | "or": [...] }</c>
/// for a composite, <c>{ "not": {...} }</c> for negation.
/// Values are normalized to plain CLR types on read (integral JSON numbers become
/// <see cref="long"/>, non-integral become <see cref="double"/>, objects become
/// <see cref="Dictionary{TKey,TValue}"/>, arrays become <see cref="List{T}"/>).
/// </summary>
public sealed class FilterExprJsonConverterFactory : JsonConverterFactory
{
    public override bool CanConvert(Type typeToConvert)
        => typeof(FilterExpr).IsAssignableFrom(typeToConvert);

    public override JsonConverter CreateConverter(Type typeToConvert, JsonSerializerOptions options)
    {
        var converterType = typeof(FilterExprConverter<>).MakeGenericType(typeToConvert);
        return (JsonConverter)Activator.CreateInstance(converterType)!;
    }
}

internal sealed class FilterExprConverter<T> : JsonConverter<T>
    where T : FilterExpr
{
    public override T? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        => (T?)FilterExprJsonCore.Read(ref reader);

    public override void Write(Utf8JsonWriter writer, T value, JsonSerializerOptions options)
        => FilterExprJsonCore.Write(writer, value);
}

internal static class FilterExprJsonCore
{
    public static FilterExpr? Read(ref Utf8JsonReader reader)
    {
        if (reader.TokenType == JsonTokenType.Null)
        {
            return null;
        }

        using var doc = JsonDocument.ParseValue(ref reader);
        return ParseExpr(doc.RootElement);
    }

    public static void Write(Utf8JsonWriter writer, FilterExpr value)
    {
        switch (value)
        {
            case FieldCompare compare:
                writer.WriteStartObject();
                writer.WriteString("field", compare.Field);
                writer.WriteString("op", OpToString(compare.Op));
                writer.WritePropertyName("value");
                WriteValue(writer, compare.Value);
                writer.WriteEndObject();
                break;

            case And and:
                writer.WriteStartObject();
                writer.WritePropertyName("and");
                writer.WriteStartArray();
                foreach (var clause in and.Clauses)
                {
                    Write(writer, clause);
                }

                writer.WriteEndArray();
                writer.WriteEndObject();
                break;

            case Or or:
                writer.WriteStartObject();
                writer.WritePropertyName("or");
                writer.WriteStartArray();
                foreach (var clause in or.Clauses)
                {
                    Write(writer, clause);
                }

                writer.WriteEndArray();
                writer.WriteEndObject();
                break;

            case Not not:
                writer.WriteStartObject();
                writer.WritePropertyName("not");
                Write(writer, not.Clause);
                writer.WriteEndObject();
                break;

            default:
                throw new JsonException($"Unsupported FilterExpr type '{value.GetType().FullName}'.");
        }
    }

    private static FilterExpr ParseExpr(JsonElement element)
    {
        if (element.ValueKind != JsonValueKind.Object)
        {
            throw new JsonException("A FilterExpr must be a JSON object.");
        }

        if (element.TryGetProperty("field", out var fieldElement))
        {
            var field = fieldElement.GetString();
            if (field is null)
            {
                throw new JsonException("'field' must be a string.");
            }

            if (!element.TryGetProperty("op", out var opElement))
            {
                throw new JsonException("A field compare requires an 'op' property.");
            }

            object? value = element.TryGetProperty("value", out var valueElement)
                ? JsonValueNormalizer.ToClrValue(valueElement)
                : null;

            return new FieldCompare(field, ReadOp(opElement), value);
        }

        if (element.TryGetProperty("and", out var andElement))
        {
            return new And(ReadClauses(andElement));
        }

        if (element.TryGetProperty("or", out var orElement))
        {
            return new Or(ReadClauses(orElement));
        }

        if (element.TryGetProperty("not", out var notElement))
        {
            return new Not(ParseExpr(notElement));
        }

        throw new JsonException("A FilterExpr object must have 'field', 'and', 'or', or 'not'.");
    }

    private static IReadOnlyList<FilterExpr> ReadClauses(JsonElement element)
    {
        if (element.ValueKind != JsonValueKind.Array)
        {
            throw new JsonException("'and'/'or' must be a JSON array.");
        }

        var clauses = new List<FilterExpr>();
        foreach (var item in element.EnumerateArray())
        {
            clauses.Add(ParseExpr(item));
        }

        return clauses;
    }

    private static CompareOp ReadOp(JsonElement element)
    {
        var text = element.GetString();
        if (text is not null && Enum.TryParse<CompareOp>(text, ignoreCase: true, out var op))
        {
            return op;
        }

        throw new JsonException($"Unknown CompareOp '{text}'.");
    }

    private static string OpToString(CompareOp op)
    {
        var name = op.ToString();
        return char.ToLowerInvariant(name[0]) + name[1..];
    }

    private static void WriteValue(Utf8JsonWriter writer, object? value)
    {
        switch (value)
        {
            case null:
                writer.WriteNullValue();
                break;
            case bool b:
                writer.WriteBooleanValue(b);
                break;
            case string s:
                writer.WriteStringValue(s);
                break;
            case char c:
                writer.WriteStringValue(c.ToString());
                break;
            case byte b:
                writer.WriteNumberValue(b);
                break;
            case short s:
                writer.WriteNumberValue(s);
                break;
            case int i:
                writer.WriteNumberValue(i);
                break;
            case long l:
                writer.WriteNumberValue(l);
                break;
            case float f:
                writer.WriteNumberValue(f);
                break;
            case double d:
                writer.WriteNumberValue(d);
                break;
            case decimal m:
                writer.WriteNumberValue(m);
                break;
            case DateTime dt:
                writer.WriteStringValue(dt);
                break;
            case DateTimeOffset dto:
                writer.WriteStringValue(dto);
                break;
            case Guid g:
                writer.WriteStringValue(g);
                break;
            case JsonElement je:
                je.WriteTo(writer);
                break;
            case IReadOnlyDictionary<string, object?> dict:
                writer.WriteStartObject();
                foreach (var (key, item) in dict)
                {
                    writer.WritePropertyName(key);
                    WriteValue(writer, item);
                }

                writer.WriteEndObject();
                break;
            case IEnumerable enumerable:
                writer.WriteStartArray();
                foreach (var item in enumerable)
                {
                    WriteValue(writer, item);
                }

                writer.WriteEndArray();
                break;
            default:
                throw new JsonException($"Unsupported filter value type '{value.GetType().FullName}'.");
        }
    }
}

internal static class JsonValueNormalizer
{
    /// <summary>Converts a <see cref="JsonElement"/> to plain CLR values.</summary>
    public static object? ToClrValue(JsonElement element)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.Null:
                return null;
            case JsonValueKind.True:
                return true;
            case JsonValueKind.False:
                return false;
            case JsonValueKind.String:
                return element.GetString();
            case JsonValueKind.Number:
                return element.TryGetInt64(out var integral) ? (object)integral : element.GetDouble();
            case JsonValueKind.Array:
                var list = new List<object?>();
                foreach (var item in element.EnumerateArray())
                {
                    list.Add(ToClrValue(item));
                }

                return list;
            case JsonValueKind.Object:
                var dict = new Dictionary<string, object?>(StringComparer.Ordinal);
                foreach (var property in element.EnumerateObject())
                {
                    dict[property.Name] = ToClrValue(property.Value);
                }

                return dict;
            default:
                throw new JsonException($"Unsupported JSON value kind '{element.ValueKind}'.");
        }
    }
}
