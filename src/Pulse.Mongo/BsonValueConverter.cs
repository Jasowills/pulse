using MongoDB.Bson;
using MongoDB.Bson.Serialization;

namespace Pulse.Mongo;

/// <summary>Converts BSON values to plain CLR values for the provider-neutral wire types.</summary>
internal static class BsonValueConverter
{
    public static IReadOnlyDictionary<string, object?> ToDictionary(BsonDocument document)
    {
        var dict = new Dictionary<string, object?>(document.ElementCount, StringComparer.Ordinal);
        foreach (var element in document)
        {
            dict[element.Name] = ToClrValue(element.Value);
        }

        return dict;
    }

    public static object? ToClrValue(BsonValue value)
    {
        switch (value.BsonType)
        {
            case BsonType.Null:
                return null;
            case BsonType.ObjectId:
                // ObjectIds surface to Pulse as their hex string (matching the id format the
                // change pipeline already uses for DocumentId), so _id is a plain string on the wire.
                return value.AsObjectId.ToString();
            case BsonType.Decimal128:
                // MapToDotNetValue returns the Decimal128 struct here, which serializes as an empty
                // object on the wire; project to a plain decimal so the client can bind it.
                return MongoDB.Bson.Decimal128.ToDecimal(value.AsDecimal128);
            case BsonType.Document:
                return ToDictionary(value.AsBsonDocument);
            case BsonType.Array:
                var list = new List<object?>(value.AsBsonArray.Count);
                foreach (var item in value.AsBsonArray)
                {
                    list.Add(ToClrValue(item));
                }

                return list;
            default:
                return BsonTypeMapper.MapToDotNetValue(value);
        }
    }
}
