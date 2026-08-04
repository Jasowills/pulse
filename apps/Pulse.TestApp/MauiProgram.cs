using Microsoft.Extensions.Logging;
using Pulse.TestApp.Services;

namespace Pulse.TestApp;

public static class MauiProgram
{
    public static MauiApp CreateMauiApp()
    {
        var builder = MauiApp.CreateBuilder();
        builder
            .UseMauiApp<App>();

        builder.Services.AddSingleton<PulseService>();
        builder.Services.AddSingleton<ViewModels.MainViewModel>();
        builder.Services.AddSingleton<ViewModels.DetailViewModel>();
        builder.Services.AddTransient<MainPage>();
        builder.Services.AddTransient<DetailPage>();

#if DEBUG
        builder.Logging.AddDebug();
#endif

        return builder.Build();
    }
}