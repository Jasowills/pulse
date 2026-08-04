using System.Collections;
using MongoDB.Bson;
using MongoDB.Driver;
using Pulse.Abstractions;

namespace Pulse.Mongo;

/// <summary>
/// Translates the provider-neutral <see cref="FilterExpr"/> tree into a Mongo
/// <see cref="FilterDefinition{TDocument}"/> for snapshot queries. Dot paths map
/// directly onto Mongo dotted paths (including numeric array indexes like
/// <c>items.0.name</c>). Semantics mirror <see cref="DictionaryFilterMatcher"/>:
/// <c>exists</c> means key present and non-null.
/// </summary>
public static class MongoFilterTranslator
{
    public static FilterDefinition<BsonDocument> Translate(FilterExpr expr)
    {
        if (expr is null)
        {
            throw new ArgumentNullException(nameof(expr));
        }

        switch (expr)
        {
            case FieldCompare compare:
                return TranslateCompare(compare);

            case And and:
                return Builders<BsonDocument>.Filter.And(
                    and.Clauses.Select(Translate).ToArray());

            case Or or:
                return Builders<BsonDocument>.Filter.Or(
                    or.Clauses.Select(Translate).ToArray());

            case Not not:
                return Builders<BsonDocument>.Filter.Not(Translate(not.Clause));

            default:
                throw new NotSupportedException($"Unsupported filter expression '{expr.GetType().Name}'.");
        }
    }

    private static FilterDefinition<BsonDocument> TranslateCompare(FieldCompare compare)
    {
        if (string.IsNullOrWhiteSpace(compare.Field))
        {
            throw new ArgumentException("Filter field must be a non-empty path.", nameof(compare));
        }

        var field = compare.Field;
        var isId = field.Equals("_id", StringComparison.Ordinal);
        var value = isId ? NormalizeId(ToBsonValue(compare.Value)) : ToBsonValue(compare.Value);

        return compare.Op switch
        {
            CompareOp.Eq => Builders<BsonDocument>.Filter.Eq(field, value),
            CompareOp.Ne => Builders<BsonDocument>.Filter.Ne(field, value),
            CompareOp.Gt => Builders<BsonDocument>.Filter.Gt(field, value),
            CompareOp.Gte => Builders<BsonDocument>.Filter.Gte(field, value),
            CompareOp.Lt => Builders<BsonDocument>.Filter.Lt(field, value),
            CompareOp.Lte => Builders<BsonDocument>.Filter.Lte(field, value),
            CompareOp.In => Builders<BsonDocument>.Filter.In(field, AsArray(value)),
            CompareOp.NotIn => Builders<BsonDocument>.Filter.Nin(field, AsArray(value)),
            // Pulse's `exists` is key present AND non-null (see README).
            CompareOp.Exists => Builders<BsonDocument>.Filter.And(
                Builders<BsonDocument>.Filter.Exists(field, true),
                Builders<BsonDocument>.Filter.Ne(field, BsonNull.Value)),
            _ => throw new NotSupportedException($"Unsupported comparison operator '{compare.Op}'."),
        };
    }

    /// <summary>
    /// _id filters arrive as strings (Pulse surfaces ObjectIds as hex), but the stored value is
    /// an ObjectId. Parse valid hex back to ObjectId so Eq/In on _id actually match.
    /// </summary>
    private static BsonValue NormalizeId(BsonValue value)
        => value is BsonArray array
            ? new BsonArray(array.Select(AsObjectId))
            : AsObjectId(value);

    private static BsonValue AsObjectId(BsonValue value)
        => value is BsonString { Value: { } text } && ObjectId.TryParse(text, out var oid)
            ? new BsonObjectId(oid)
            : value;

    private static IEnumerable<BsonValue> AsArray(BsonValue value)
        => value is BsonArray array
            ? array
            : new[] { value };

    /// <summary>Converts a CLR filter value (from JSON) to BSON, mirroring the wire types.</summary>
    public static BsonValue ToBsonValue(object? value)
    {
        switch (value)
        {
            case null:
                return BsonNull.Value;
            case string s:
                return new BsonString(s);
            case bool b:
                return new BsonBoolean(b);
            case byte or sbyte or short or ushort or int or uint or long:
                return new BsonInt64(Convert.ToInt64(value));
            case ulong ul:
                return new BsonInt64(checked((long)ul));
            case float or double:
                return new BsonDouble(Convert.ToDouble(value));
            case decimal m:
                return new BsonDecimal128(m);
            case DateTime dt:
                return new BsonDateTime(dt);
            case DateTimeOffset dto:
                return new BsonDateTime(dto.UtcDateTime);
            case IReadOnlyDictionary<string, object?> dict:
                var document = new BsonDocument();
                foreach (var (key, item) in dict)
                {
                    document[key] = ToBsonValue(item);
                }

                return document;
            case IEnumerable enumerable and not string:
                var array = new BsonArray();
                foreach (var item in enumerable)
                {
                    array.Add(ToBsonValue(item));
                }

                return array;
            default:
                throw new NotSupportedException(
                    $"Filter values of type '{value.GetType().Name}' are not supported on the Mongo provider.");
        }
    }
}
