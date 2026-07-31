using System.Security.Claims;

namespace Pulse.Server;

/// <summary>
/// Authorization seam for subscriptions. v0.1 ships <see cref="AllowAllAuthorizer"/> only;
/// the default allows everything (see README caveats) until a real policy exists.
/// </summary>
public interface IPulseAuthorizer
{
    ValueTask<bool> AuthorizeAsync(string source, ClaimsPrincipal? principal);
}

/// <summary>Allows every subscription. Not safe for production; see README caveats.</summary>
public sealed class AllowAllAuthorizer : IPulseAuthorizer
{
    public ValueTask<bool> AuthorizeAsync(string source, ClaimsPrincipal? principal)
        => ValueTask.FromResult(true);
}
