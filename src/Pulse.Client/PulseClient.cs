using System.Text.Json;
using Microsoft.AspNetCore.Http.Connections.Client;
using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.Extensions.DependencyInjection;
using Pulse.Abstractions;
using Pulse.Abstractions.Json;

namespace Pulse.Client;

/// <summary>
/// SignalR-based client for Pulse hubs. All subscriptions share one connection; wire
/// messages are routed to the subscription whose id they carry. The connection uses
/// <c>WithAutomaticReconnect()</c>; automatic resubscribe after a reconnect is a later
/// step — see <c>IPulseSubscription&lt;T&gt;</c>.
/// </summary>
public sealed class PulseClient : IAsyncDisposable
{
    private readonly HubConnection _connection;
    private readonly object _sync = new();
    private readonly Dictionary<string, IPulseSubscriptionHost> _subscriptions = new(StringComparer.Ordinal);
    private readonly Dictionary<string, List<object>> _pending = new(StringComparer.Ordinal);
    private readonly JsonSerializerOptions _json;
    private bool _disposed;

    public JsonSerializerOptions JsonOptions => _json;

    /// <summary>Current connection state (useful for surfacing reconnect status to the UI).</summary>
    public HubConnectionState State => _connection.State;

    /// <summary>Raised when the connection has fully closed (final failure or manual stop).</summary>
    public event Func<Exception?, Task>? OnDisconnected;

    public PulseClient(
        string hubUrl,
        Action<HttpConnectionOptions>? configureHttpConnection = null,
        Action<JsonHubProtocolOptions>? configureJson = null)
    {
        _json = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };

        var builder = new HubConnectionBuilder()
            .WithUrl(hubUrl, configureHttpConnection ?? (_ => { }))
            .WithAutomaticReconnect()
            .AddJsonProtocol(options =>
            {
                options.PayloadSerializerOptions.Converters.Add(new ObjectToInferredTypesConverter());
                configureJson?.Invoke(options);
            });

        _connection = builder.Build();
        _connection.On<PulseSnapshotMessage>("PulseSnapshot", Dispatch);
        _connection.On<PulseChangeMessage>("PulseChange", Dispatch);
        _connection.Closed += OnClosedAsync;
        _connection.Reconnected += OnReconnectedAsync;
    }

    public async Task ConnectAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        await _connection.StartAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Subscribes to a source. Returns once the server has acknowledged the subscription;
    /// the initial <see cref="IPulseSubscription{T}.OnSnapshot"/> arrives asynchronously.
    /// </summary>
    public async Task<IPulseSubscription<T>> Subscribe<T>(
        string source,
        FilterExpr? where = null,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        if (_connection.State != HubConnectionState.Connected)
        {
            throw new InvalidOperationException("PulseClient is not connected. Call ConnectAsync first.");
        }

        var whereJson = where is null ? null : JsonSerializer.Serialize(where);
        var subscriptionId = await _connection
            .InvokeAsync<string>("Subscribe", source, whereJson, cancellationToken)
            .ConfigureAwait(false);

        var subscription = new PulseSubscription<T>(subscriptionId, source, where, _json, id => UnsubscribeAsync(id));
        lock (_sync)
        {
            _subscriptions[subscriptionId] = subscription;
            if (_pending.Remove(subscriptionId, out var buffered))
            {
                // The server can send the snapshot before Subscribe returns; replay any
                // buffered messages in wire order before any newly arriving message.
                foreach (var message in buffered)
                {
                    subscription.Enqueue(message);
                }
            }
        }

        return subscription;
    }

    public async Task UnsubscribeAsync(string subscriptionId, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        lock (_sync)
        {
            if (_subscriptions.Remove(subscriptionId, out var subscription))
            {
                subscription.Close();
            }

            _pending.Remove(subscriptionId);
        }

        if (_connection.State == HubConnectionState.Connected)
        {
            await _connection.InvokeAsync("Unsubscribe", subscriptionId, cancellationToken).ConfigureAwait(false);
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        lock (_sync)
        {
            foreach (var subscription in _subscriptions.Values)
            {
                subscription.Close();
            }

            _subscriptions.Clear();
            _pending.Clear();
        }

        await _connection.DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Routes a wire message to its subscription, buffering it if the subscription has not
    /// yet been registered (the snapshot can beat <c>Subscribe</c>'s return).
    /// </summary>
    private void Dispatch(object message)
    {
        var subscriptionId = message switch
        {
            PulseSnapshotMessage snapshot => snapshot.SubscriptionId,
            PulseChangeMessage change => change.SubscriptionId,
            _ => throw new InvalidOperationException($"Unknown wire message '{message.GetType().FullName}'."),
        };

        lock (_sync)
        {
            if (_subscriptions.TryGetValue(subscriptionId, out var subscription))
            {
                subscription.Enqueue(message);
                return;
            }

            if (!_pending.TryGetValue(subscriptionId, out var buffered))
            {
                buffered = new List<object>();
                _pending[subscriptionId] = buffered;
            }

            buffered.Add(message);
        }
    }

    private async Task OnClosedAsync(Exception? exception)
    {
        if (OnDisconnected is { } handler)
        {
            await handler(exception).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// After a reconnect the server no longer knows our subscription ids, so every active
    /// subscription is re-created. The result is treated as a fresh snapshot: the cached
    /// <c>Current</c> is cleared and <c>OnSnapshot</c> fires again with the new documents.
    /// </summary>
    private async Task OnReconnectedAsync(string? _)
    {
        IPulseSubscriptionHost[] subscriptions;
        lock (_sync)
        {
            subscriptions = _subscriptions.Values.ToArray();
        }

        foreach (var subscription in subscriptions)
        {
            try
            {
                await ResubscribeAsync(subscription).ConfigureAwait(false);
            }
            catch (Exception)
            {
                // Re-subscribing will be retried on the next successful reconnect.
            }
        }
    }

    private async Task ResubscribeAsync(IPulseSubscriptionHost subscription)
    {
        var whereJson = subscription.Where is null ? null : JsonSerializer.Serialize(subscription.Where);
        var newId = await _connection
            .InvokeAsync<string>("Subscribe", subscription.Source, whereJson)
            .ConfigureAwait(false);

        lock (_sync)
        {
            _subscriptions.Remove(subscription.Id);
            _pending.Remove(subscription.Id);
            _subscriptions[newId] = subscription;

            // Drop stale cache entries before any pending/fresh snapshot is applied.
            subscription.Reset();
            if (_pending.Remove(newId, out var buffered))
            {
                foreach (var message in buffered)
                {
                    subscription.Enqueue(message);
                }
            }

            subscription.UpdateId(newId);
        }
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(PulseClient));
        }
    }
}

/// <summary>Internal seam so <see cref="PulseClient"/> can route messages without knowing T.</summary>
internal interface IPulseSubscriptionHost
{
    string Id { get; }

    string Source { get; }

    FilterExpr? Where { get; }

    void Enqueue(object message);

    void Close();

    void UpdateId(string newId);

    void Reset();
}
