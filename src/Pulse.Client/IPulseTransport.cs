using Microsoft.AspNetCore.SignalR.Client;
using Pulse.Abstractions;

namespace Pulse.Client;

/// <summary>
/// Seam at the SignalR boundary so <see cref="PulseClient"/> is testable without a real hub.
/// Production uses <see cref="HubConnectionTransport"/>; tests use an in-memory fake.
/// </summary>
public interface IPulseTransport : IAsyncDisposable
{
    HubConnectionState State { get; }
    Task StartAsync(CancellationToken ct);
    Task<string> InvokeSubscribeAsync(string source, string? whereJson, CancellationToken ct);
    Task InvokeUnsubscribeAsync(string subscriptionId, CancellationToken ct);
    void OnSnapshot(Func<PulseSnapshotMessage, Task> handler);
    void OnChange(Func<PulseChangeMessage, Task> handler);
    event Func<Exception?, Task>? Closed;
    event Func<Exception?, Task>? Reconnecting;
    event Func<string?, Task>? Reconnected;
}

internal sealed class HubConnectionTransport : IPulseTransport
{
    private readonly HubConnection _connection;
    public HubConnectionTransport(HubConnection c) => _connection = c;
    public HubConnectionState State => _connection.State;
    public Task StartAsync(CancellationToken ct) => _connection.StartAsync(ct);
    public Task<string> InvokeSubscribeAsync(string source, string? whereJson, CancellationToken ct) => _connection.InvokeAsync<string>("Subscribe", source, whereJson, ct);
    public Task InvokeUnsubscribeAsync(string id, CancellationToken ct) => _connection.InvokeAsync("Unsubscribe", id, ct);
    public void OnSnapshot(Func<PulseSnapshotMessage, Task> h) => _connection.On("PulseSnapshot", (PulseSnapshotMessage m) => h(m));
    public void OnChange(Func<PulseChangeMessage, Task> h) => _connection.On("PulseChange", (PulseChangeMessage m) => h(m));
    public event Func<Exception?, Task>? Closed { add => _connection.Closed += value; remove => _connection.Closed -= value; }
    public event Func<Exception?, Task>? Reconnecting { add => _connection.Reconnecting += value; remove => _connection.Reconnecting -= value; }
    public event Func<string?, Task>? Reconnected { add => _connection.Reconnected += value; remove => _connection.Reconnected -= value; }
    public ValueTask DisposeAsync() => _connection.DisposeAsync();
}
