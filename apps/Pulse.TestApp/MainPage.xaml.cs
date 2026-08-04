using Pulse.TestApp.ViewModels;

namespace Pulse.TestApp;

public partial class MainPage : ContentPage
{
    private readonly MainViewModel _viewModel;

    public MainPage(MainViewModel viewModel)
    {
        InitializeComponent();
        BindingContext = _viewModel = viewModel;
    }

    protected override async void OnAppearing()
    {
        base.OnAppearing();
        if (_viewModel.Rows.Count == 0)
        {
            await _viewModel.StartAsync();
        }
    }

    private async void OnOrderSelected(object? sender, SelectionChangedEventArgs e)
    {
        if (e.CurrentSelection.FirstOrDefault() is OrderRowViewModel row)
        {
            await Shell.Current.GoToAsync($"{nameof(DetailPage)}?id={Uri.EscapeDataString(row.Id)}");
        }

        ((CollectionView)sender!).SelectedItem = null;
    }
}