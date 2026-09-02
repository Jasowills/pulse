using System.Collections;
using MongoDB.Bson;
using MongoDB.Driver;
using Pulse.Abstractions;
using Pulse.Abstractions.Filtering;

namespace Pulse.Mongo;

public static class MongoFilterTranslator
{
    public static FilterDefinition<BsonDocument> Translate(FilterExpr expr)
    {
        ArgumentNullException.ThrowIfNull(expr);
        return new Translator().Translate(expr);
    }

    private sealed class Translator : FilterTranslatorBase<FilterDefinition<BsonDocument>>
    {
        protected override FilterDefinition<BsonDocument> EmptyAnd() => Builders<BsonDocument>.Filter.Empty;
        protected override FilterDefinition<BsonDocument> EmptyOr() => Builders<BsonDocument>.Filter.Empty;
        protected override FilterDefinition<BsonDocument> CombineAnd(IEnumerable<FilterDefinition<BsonDocument>> c) => Builders<BsonDocument>.Filter.And(c.ToArray());
        protected override FilterDefinition<BsonDocument> CombineOr(IEnumerable<FilterDefinition<BsonDocument>> c) => Builders<BsonDocument>.Filter.Or(c.ToArray());
        protected override FilterDefinition<BsonDocument> Negate(FilterDefinition<BsonDocument> c) => Builders<BsonDocument>.Filter.Not(c);

        protected override FilterDefinition<BsonDocument> TranslateCompare(FieldCompare compare)
        {
            if (string.IsNullOrWhiteSpace(compare.Field)) throw new ArgumentException("Filter field must be a non-empty path.", nameof(compare));
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
                CompareOp.Exists => Builders<BsonDocument>.Filter.And(Builders<BsonDocument>.Filter.Exists(field, true), Builders<BsonDocument>.Filter.Ne(field, BsonNull.Value)),
                _ => throw new NotSupportedException($"Unsupported comparison operator '{compare.Op}'."),
            };
        }

        private static BsonValue NormalizeId(BsonValue v) => v is BsonArray a ? new BsonArray(a.Select(AsObjectId)) : AsObjectId(v);
        private static BsonValue AsObjectId(BsonValue v) => v is BsonString { Value: { } t } && ObjectId.TryParse(t, out var oid) ? new BsonObjectId(oid) : v;
        private static IEnumerable<BsonValue> AsArray(BsonValue v) => v is BsonArray a ? a : new[] { v };

        public static BsonValue ToBsonValue(object? value)
        {
            switch (value)
            {
                case null: return BsonNull.Value;
                case string s: return new BsonString(s);
                case bool b: return new BsonBoolean(b);
                case byte or sbyte or short or ushort or int or uint or long: return new BsonInt64(Convert.ToInt64(value));
                case ulong ul: return new BsonInt64(checked((long)ul));
                case float or double: return new BsonDouble(Convert.ToDouble(value));
                case decimal m: return new BsonDecimal128(m);
                case DateTime dt: return new BsonDateTime(dt);
                case DateTimeOffset dto: return new BsonDateTime(dto.UtcDateTime);
                case IReadOnlyDictionary<string, object?> dict: var d = new BsonDocument(); foreach (var (k, v) in dict) d[k] = ToBsonValue(v); return d;
                case IEnumerable e and not string: var arr = new BsonArray(); foreach (var i in e) arr.Add(ToBsonValue(i)); return arr;
                default: throw new NotSupportedException($"Filter values of type '{value.GetType().Name}' are not supported on the Mongo provider.");
            }
        }
    }

    // Keep public ToBsonValue for callers that use it directly
    public static BsonValue ToBsonValue(object? value) => Translator.ToBsonValue(value);
}
