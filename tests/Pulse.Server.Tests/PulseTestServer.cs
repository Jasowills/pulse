using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Pulse.Abstractions;
using Pulse.Server;
using Pulse.TestSupport;

namespace Pulse.Server.Tests;

/// <summary>
/// Hosts a real SignalR server with a <see cref="FakeChangeSource"/> and a
/// <see cref="SubscriptionRegistry"/>, exposing the bound base URL and the fake
/// so tests can publish changes and assert client-observable behavior.
/// </summary>
public sealed class PulseTestServer : IAsyncDisposable
{
    private readonly WebApplication _app;

    private PulseTestServer(WebApplication app, FakeChangeSource changeSource, InMemoryResumeTokenStore resumeTokenStore)
    {
        _app = app;
        ChangeSource = changeSource;
        ResumeTokenStore = resumeTokenStore;
        BaseUrl = app.Urls.First();
    }

    public FakeChangeSource ChangeSource { get; }

    public InMemoryResumeTokenStore ResumeTokenStore { get; }

    public string BaseUrl { get; }

    public static async Task<PulseTestServer> StartAsync(
        bool registerDefaultRegistry = true,
        Action<IServiceCollection>? configureServices = null)
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseUrls("http://127.0.0.1:0");
        builder.Logging.ClearProviders();

        var changeSource = new FakeChangeSource();
        var resumeTokenStore = new InMemoryResumeTokenStore();
        builder.Services.AddSignalR();
        builder.Services.AddSingleton<IChangeSource>(changeSource);
        if (registerDefaultRegistry)
        {
            builder.Services.AddSingleton(sp => new SubscriptionRegistry(
                sp.GetRequiredService<IChangeSource>(),
                sp.GetRequiredService<IHubContext<PulseHub>>(),
                sp.GetRequiredService<ILogger<SubscriptionRegistry>>(),
                resumeTokenStore));
        }

        builder.Services.AddSingleton<IPulseAuthorizer, AllowAllAuthorizer>();
        configureServices?.Invoke(builder.Services);

        var app = builder.Build();
        app.MapHub<PulseHub>("/pulse");
        await app.StartAsync();
        return new PulseTestServer(app, changeSource, resumeTokenStore);
    }

    public async ValueTask DisposeAsync()
    {
        await _app.DisposeAsync();
    }
}
