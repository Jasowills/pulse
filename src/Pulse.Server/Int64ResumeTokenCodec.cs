using Pulse.Abstractions;

namespace Pulse.Server;

public static class Int64ResumeTokenCodec
{
    public static byte[] Encode(long value) => BitConverter.GetBytes(value);

    public static long Decode(byte[] opaque)
    {
        if (opaque is null || opaque.Length != sizeof(long))
            throw new ResumeTokenInvalidException("Resume token must be 8 bytes.");
        return BitConverter.ToInt64(opaque, 0);
    }
}
