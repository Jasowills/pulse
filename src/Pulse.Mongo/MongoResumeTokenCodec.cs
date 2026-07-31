using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using Pulse.Abstractions;

namespace Pulse.Mongo;

/// <summary>
/// Codec for <see cref="ResumeToken"/>: the opaque payload is the raw BSON bytes
/// of Mongo's resume token document (<see cref="BsonDocument.ToBson()"/>).
/// </summary>
internal static class MongoResumeTokenCodec
{
    public static byte[] Encode(BsonDocument resumeToken) => resumeToken.ToBson();

    public static BsonDocument Decode(byte[] opaque)
    {
        try
        {
            return BsonSerializer.Deserialize<BsonDocument>(opaque);
        }
        catch (BsonSerializationException ex)
        {
            throw new ResumeTokenInvalidException("Resume token bytes are not a valid Mongo resume token.", ex);
        }
    }
}
