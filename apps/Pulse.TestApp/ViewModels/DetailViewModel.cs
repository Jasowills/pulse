using System.ComponentModel;
using System.Runtime.CompilerServices;
using Pulse.Abstractions;
using Pulse.Client;
using Pulse.TestApp.Core;

namespace Pulse.TestApp.ViewModels;

/// <summary>The detail screen model: one order subscribed by id, live-updating its status.</summary>
public sealed class DetailViewModel : INotifyPropertyChanged
{
    private readonly PulseService _pulse;
    private IPulseSubscription<Order>? _subscription;
    private OrderRowViewModel? _row;
    private string _title = "";

    public DetailViewModel(PulseService pulse)
    {
        _pulse = pulse;
    }

    public OrderRowViewModel? Row
    {
        get => _row;
        private set { _row = value; OnPropertyChanged(); }
    }

    public string Title
    {
        get => _title;
        private set { _title = value; OnPropertyChanged(); }
    }

    public async Task OpenAsync(string orderId)
    {
        Title = orderId.Length > 12 ? orderId[..12] : orderId;
        OnPropertyChanged();

        if (_subscription is not null)
        {
            _subscription.OnSnapshot -= OnSnapshot;
            _subscription.OnChange -= OnChange;
            await _subscription.UnsubscribeAsync();
            _subscription = null;
        }

        _subscription = await _pulse.SubscribeAsync<Order>(OrderState.DetailFilter(orderId));
        _subscription.OnSnapshot += OnSnapshot;
        _subscription.OnChange += OnChange;
    }

    private void OnSnapshot(IReadOnlyList<Order> documents)
    {
        MainThread.BeginInvokeOnMainThread(() =>
        {
            var order = documents.FirstOrDefault();
            Row = order is null ? null : new OrderRowViewModel(order);
            OnPropertyChanged();
        });
    }

    private void OnChange(PulseChange<Order> change)
    {
        MainThread.BeginInvokeOnMainThread(() =>
        {
            if (change.Document is not null)
            {
                if (Row is null) Row = new OrderRowViewModel(change.Document);
                else Row.Refresh(change.Document);
            }
        });
    }

    public event PropertyChangedEventHandler? PropertyChanged;

    private void OnPropertyChanged([CallerMemberName] string? name = null)
        => PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(name));
}