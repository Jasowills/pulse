using System.Text.Json;
using Pulse.Mongo;
using Pulse.Postgres;
using Pulse.Server;
using Pulse.SqlServer;
using Pulse.TestApp.Core;

namespace Pulse.TestApp.Server;

/// <summary>
/// Provider-switchable Pulse host. The ONLY place provider selection happens: read
/// <c>PULSE_PROVIDER</c> (mongo|postgres|sqlserver) at startup and wire the matching
/// <c>AddXxxSource()</c>. The same app code works against all three databases.
/// </summary>
public static class Program
{
    public static void Main(string[] args)
    {
        var provider = TestAppConfig.ParseProvider(
            Environment.GetEnvironmentVariable("PULSE_PROVIDER") ?? "mongo");

        var builder = WebApplication.CreateBuilder(args);
        builder.WebHost.UseUrls(
            Environment.GetEnvironmentVariable("PULSE_SERVER_URLS") ?? "http://localhost:5210");

        if (TestAppConfig.ResumeTokenDirectory is { } dir)
        {
            builder.Services.AddSingleton<IResumeTokenStore>(new FileResumeTokenStore(dir));
        }

        switch (provider)
        {
            case ProviderKind.Mongo:
                builder.Services.AddMongoSource(options =>
                {
                    options.ConnectionString = TestAppConfig.MongoUri;
                    options.Database = TestAppConfig.MongoDatabase;
                });
                break;
            case ProviderKind.Postgres:
                builder.Services.AddPostgresSource(options =>
                {
                    options.ConnectionString = TestAppConfig.PostgresConnectionString;
                });
                break;
            case ProviderKind.SqlServer:
                builder.Services.AddSqlServerSource(options =>
                {
                    options.ConnectionString = TestAppConfig.SqlServerConnectionString;
                });
                break;
            default:
                throw new InvalidOperationException($"Unhandled provider '{provider}'.");
        }

        builder.Services.AddSingleton(OrderStoreFactory.Create(provider));

        var app = builder.Build();

        app.MapGet("/", () => Results.Ok($"Pulse test server — provider={TestAppConfig.ProviderName(provider)}; hub at /pulse"));
        app.MapGet("/health", () => Results.Ok(new { provider = TestAppConfig.ProviderName(provider), ok = true }));
        app.MapPost("/orders/{id}/status", async (string id, StatusChangeRequest request, IOrderStore store, CancellationToken ct) =>
        {
            if (string.IsNullOrWhiteSpace(request?.Status) || !OrderState.Statuses.Contains(request.Status))
            {
                return Results.BadRequest(new { error = $"Unknown or missing status. Expected one of: {string.Join(", ", OrderState.Statuses)}." });
            }

            if (await store.GetByIdAsync(id, ct) is null)
            {
                return Results.NotFound(new { error = $"Order '{id}' not found." });
            }

            // A plain driver-level write (NOT through Pulse). Pulse picks up the change via its
            // change source and both list + detail screens update live.
            await store.UpdateStatusAsync(id, request.Status, ct);
            return Results.Ok(new { id, status = request.Status });
        });
        app.MapHub<PulseHub>("/pulse");

        app.Run();
    }

    public sealed record StatusChangeRequest(string? Status);
}