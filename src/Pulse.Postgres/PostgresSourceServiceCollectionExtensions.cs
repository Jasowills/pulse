using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Npgsql;
using Pulse.Server;

namespace Pulse.Postgres;

public sealed class PostgresSourceOptions
{
    public string ConnectionString { get; set; } = string.Empty;

    /// <summary>
    /// How long a watcher waits for a NOTIFY before polling the change log anyway. A small
    /// value makes delivery snappier at the cost of more polling; NOTIFY wakes the watcher
    /// immediately in the common case, so this is only a robustness fallback.
    /// </summary>
    public TimeSpan PollInterval { get; set; } = TimeSpan.FromMilliseconds(250);
}

/// <summary>
/// Registers the Postgres change source and its subscription registry. Other providers
/// mirror this shape exactly (see <c>AddMongoSource</c>), so Pulse.Server itself never
/// references a concrete provider.
/// </summary>
public static class PostgresSourceServiceCollectionExtensions
{
    public static IServiceCollection AddPostgresSource(
        this IServiceCollection services,
        Action<PostgresSourceOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        var options = new PostgresSourceOptions();
        configure(options);
        if (string.IsNullOrWhiteSpace(options.ConnectionString))
        {
            throw new ArgumentException("PostgresSourceOptions.ConnectionString is required.", nameof(configure));
        }

        if (options.PollInterval <= TimeSpan.Zero)
        {
            throw new ArgumentException("PostgresSourceOptions.PollInterval must be positive.", nameof(configure));
        }

        services.AddSignalR();
        // Default resume store is in-memory (does not survive restarts — see README caveats).
        // Register IResumeTokenStore yourself before AddPostgresSource to override, e.g. FileResumeTokenStore.
        services.TryAddSingleton<IResumeTokenStore, InMemoryResumeTokenStore>();
        services.AddSingleton(_ => NpgsqlDataSource.Create(options.ConnectionString));
        services.AddSingleton(sp => new PostgresChangeSource(
            sp.GetRequiredService<NpgsqlDataSource>(),
            sp.GetService<ILogger<PostgresChangeSource>>(),
            options.PollInterval));
        services.AddSingleton(sp => new SubscriptionRegistry(
            sp.GetRequiredService<PostgresChangeSource>(),
            sp.GetRequiredService<IHubContext<PulseHub>>(),
            sp.GetRequiredService<ILogger<SubscriptionRegistry>>(),
            sp.GetRequiredService<IResumeTokenStore>()));
        services.AddSingleton<IPulseAuthorizer, AllowAllAuthorizer>();
        return services;
    }
}
