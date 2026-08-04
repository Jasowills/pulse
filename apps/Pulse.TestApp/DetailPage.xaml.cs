using Pulse.TestApp.Services;
using Pulse.TestApp.ViewModels;

namespace Pulse.TestApp;

[QueryProperty(nameof(OrderId), "id")]
public partial class DetailPage : ContentPage
{
    private readonly DetailViewModel _viewModel;
    private string _orderId = "";

    public DetailPage(DetailViewModel viewModel, PulseService pulse)
    {
        InitializeComponent();
        BindingContext = _viewModel = viewModel;
    }

    public string OrderId
    {
        get => _orderId;
        set { _orderId = value ?? ""; }
    }

    protected override async void OnAppearing()
    {
        base.OnAppearing();
        if (!string.IsNullOrWhiteSpace(OrderId))
        {
            await _viewModel.OpenAsync(OrderId);
        }
    }
}