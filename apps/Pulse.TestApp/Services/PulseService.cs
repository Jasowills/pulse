using Pulse.Abstractions;
using Pulse.Client;

namespace Pulse.TestApp.Services;

/// <summary>
/// Owns the single <see cref="PulseClient"/> for the app. Exposes connect/subscribe
/// with automatic reconnect, and raises <see cref="ConnectionStateChanged"/> so the
/// status bar and lifecycle hooks can react. Detail screens subscribe on demand through
/// <see cref="SubscribeAsync"/>.
/// </summary>
public sealed class PulseService : IAsyncDisposable
{
    private PulseClient? _client;

    public event Action<string>? ConnectionStateChanged;

    public string State => _client?.State.ToString() ?? "disconnected";

    public bool IsConnected => _client?.State == Microsoft.AspNetCore.SignalR.Client.HubConnectionState.Connected;

    private string HubUrl => AppConfig.HubUrl;

    public async Task ConnectAsync()
    {
        if (_client is null)
        {
            _client = new PulseClient(HubUrl);
            _client.OnDisconnected += exc =>
            {
                ConnectionStateChanged?.Invoke(State);
                return Task.CompletedTask;
            };
        }

        await _client.ConnectAsync();
        ConnectionStateChanged?.Invoke(State);
    }

    /// <summary>Called on app resume / page appearing to repaint the status bar from the live state.</summary>
    public void NotifyStateChanged() => ConnectionStateChanged?.Invoke(State);

    public async Task<IPulseSubscription<T>> SubscribeAsync<T>(FilterExpr? where)
    {
        if (_client is null)
        {
            throw new InvalidOperationException("Not connected. Call ConnectAsync first.");
        }

        return await _client.Subscribe<T>("orders", where);
    }

    public async ValueTask DisposeAsync()
    {
        if (_client is not null)
        {
            await _client.DisposeAsync();
            _client = null;
        }
    }
}