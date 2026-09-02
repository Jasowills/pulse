namespace Pulse.Abstractions;

public static class ResumeTokenExtensions
{
    public static void EnsureProvider(this ResumeToken token, string expectedProviderId)
    {
        if (!string.Equals(token.ProviderId, expectedProviderId, StringComparison.Ordinal))
            throw new ResumeTokenInvalidException($"Resume token was issued by '{token.ProviderId}', but watching '{expectedProviderId}'. Refusing to misinterpret the token.");
    }
}
