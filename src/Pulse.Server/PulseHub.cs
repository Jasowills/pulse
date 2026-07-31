using System.Text.Json;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Logging;
using Pulse.Abstractions;

namespace Pulse.Server;

/// <summary>
/// SignalR hub exposing Pulse subscriptions. Wire protocol (see README):
/// <c>Subscribe { source, where }</c> returns a subscriptionId; changes are delivered as
/// <c>PulseChange</c> invocations; errors surface as <c>HubException</c> (and later as
/// <c>PulseError</c> for async failures).
/// </summary>
public sealed class PulseHub : Hub
{
    private readonly IEnumerable<SubscriptionRegistry> _registries;
    private readonly IPulseAuthorizer _authorizer;
    private readonly ILogger<PulseHub> _logger;

    public PulseHub(
        IEnumerable<SubscriptionRegistry> registries,
        IPulseAuthorizer authorizer,
        ILogger<PulseHub> logger)
    {
        _registries = registries ?? throw new ArgumentNullException(nameof(registries));
        _authorizer = authorizer ?? throw new ArgumentNullException(nameof(authorizer));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task<string> Subscribe(string source, string? where)
    {
        if (string.IsNullOrWhiteSpace(source))
        {
            throw new HubException("Source must be a non-empty name.");
        }

        if (!await _authorizer.AuthorizeAsync(source, Context.User).ConfigureAwait(false))
        {
            throw new HubException($"Not authorized to subscribe to source '{source}'.");
        }

        FilterExpr? filter = null;
        if (!string.IsNullOrWhiteSpace(where))
        {
            try
            {
                filter = JsonSerializer.Deserialize<FilterExpr>(where);
            }
            catch (JsonException ex)
            {
                throw new HubException($"Invalid filter JSON for source '{source}': {ex.Message}", ex);
            }
        }

        foreach (var registry in _registries)
        {
            if (!registry.CanHandle(source))
            {
                continue;
            }

            try
            {
                return await registry
                    .SubscribeAsync(Context.ConnectionId, source, filter, Context.ConnectionAborted)
                    .ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not HubException)
            {
                _logger.LogError(ex, "Failed to subscribe connection '{ConnectionId}' to source '{Source}'.",
                    Context.ConnectionId, source);
                throw new HubException($"Failed to subscribe to source '{source}': {ex.Message}", ex);
            }
        }

        throw new HubException(
            $"No registered provider can serve source '{source}'. Did you forget an AddXxxSource() call?");
    }

    public async Task Unsubscribe(string subscriptionId)
    {
        foreach (var registry in _registries)
        {
            await registry.UnsubscribeAsync(subscriptionId).ConfigureAwait(false);
        }
    }

    public override async Task OnDisconnectedAsync(Exception? exception)
    {
        foreach (var registry in _registries)
        {
            await registry.RemoveConnectionAsync(Context.ConnectionId).ConfigureAwait(false);
        }

        await base.OnDisconnectedAsync(exception).ConfigureAwait(false);
    }
}
