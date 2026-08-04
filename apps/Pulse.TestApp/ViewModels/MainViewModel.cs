using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Runtime.CompilerServices;
using Pulse.Abstractions;
using Pulse.Client;
using Pulse.TestApp.Core;

namespace Pulse.TestApp.ViewModels;

/// <summary>Observable wrapper around a single Order row for list/detail binding.</summary>
public sealed class OrderRowViewModel : INotifyPropertyChanged
{
    private readonly Order _order;
    private string _status;

    public OrderRowViewModel(Order order)
    {
        _order = order;
        _status = order.Status;
    }

    public string Id => _order.Id;
    public string CustomerName => _order.CustomerName;
    public string Region => _order.Region;
    public string Total => _order.Total.ToString("C");
    public int Items => _order.Items;
    public DateTimeOffset CreatedAt => _order.CreatedAt;

    public string Status
    {
        get => _status;
        set { if (_status != value) { _status = value; OnPropertyChanged(); } }
    }

    public void Refresh(Order updated)
    {
        if (_status != updated.Status) { _status = updated.Status; OnPropertyChanged(nameof(Status)); }
        OnPropertyChanged(nameof(Total));
    }

    public event PropertyChangedEventHandler? PropertyChanged;

    private void OnPropertyChanged([CallerMemberName] string? name = null)
        => PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(name));
}

/// <summary>The list screen model: filter dropdowns, live rows, count badge, connection state.</summary>
public sealed class MainViewModel : INotifyPropertyChanged
{
    private readonly PulseService _pulse;
    private readonly List<string> _statuses = new(OrderState.Statuses);
    private readonly List<string> _regions = new(OrderState.Regions);
    private IPulseSubscription<Order>? _subscription;
    private string _filterStatus = "pending";
    private string _filterRegion = "NA";
    private string _connectionState = "offline";

    public MainViewModel(PulseService pulse)
    {
        _pulse = pulse;
        _pulse.ConnectionStateChanged += s => ConnectionState = s;
    }

    public IReadOnlyList<string> Statuses => _statuses;
    public IReadOnlyList<string> Regions => _regions;

    public string SelectedStatus
    {
        get => _filterStatus;
        set { _filterStatus = value; OnPropertyChanged(); _ = ApplyFilterAsync(); }
    }

    public string SelectedRegion
    {
        get => _filterRegion;
        set { _filterRegion = value; OnPropertyChanged(); _ = ApplyFilterAsync(); }
    }

    public ObservableCollection<OrderRowViewModel> Rows { get; } = new();

    public string ConnectionState
    {
        get => _connectionState;
        private set { if (_connectionState != value) { _connectionState = value; OnPropertyChanged(); } }
    }

    public string CountBadge => $"({Rows.Count} live)";

    public async Task StartAsync()
    {
        await _pulse.ConnectAsync();
        await ApplyFilterAsync();
    }

    public async Task RefreshAsync() => await ApplyFilterAsync();

    private async Task ApplyFilterAsync()
    {
        if (_subscription is not null)
        {
            _subscription.OnSnapshot -= OnSnapshot;
            _subscription.OnChange -= OnChange;
            await _subscription.UnsubscribeAsync();
            _subscription = null;
        }

        Rows.Clear();
        OnPropertyChanged(nameof(CountBadge));

        if (!_pulse.IsConnected)
        {
            return;
        }

        _subscription = await _pulse.SubscribeAsync(OrderState.ListFilter(SelectedStatus, SelectedRegion));
        _subscription.OnSnapshot += OnSnapshot;
        _subscription.OnChange += OnChange;
    }

    private void OnSnapshot(IReadOnlyList<Order> documents)
    {
        MainThread.BeginInvokeOnMainThread(() =>
        {
            Rows.Clear();
            foreach (var document in documents)
            {
                Rows.Add(new OrderRowViewModel(document));
            }

            OnPropertyChanged(nameof(CountBadge));
        });
    }

    private void OnChange(PulseChange<Order> change)
    {
        MainThread.BeginInvokeOnMainThread(() =>
        {
            if (change.Kind == ChangeKind.Delete)
            {
                foreach (var row in Rows.Where(r => r.Id == change.DocumentId).ToList())
                {
                    Rows.Remove(row);
                }
            }
            else if (change.Document is not null)
            {
                var existing = Rows.FirstOrDefault(r => r.Id == change.DocumentId);
                if (existing is null)
                {
                    if (change.Document.Status == SelectedStatus && change.Document.Region == SelectedRegion)
                    {
                        Rows.Add(new OrderRowViewModel(change.Document));
                    }
                }
                else
                {
                    existing.Refresh(change.Document);
                }
            }

            OnPropertyChanged(nameof(CountBadge));
        });
    }

    public event PropertyChangedEventHandler? PropertyChanged;

    private void OnPropertyChanged([CallerMemberName] string? name = null)
        => PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(name));
}
