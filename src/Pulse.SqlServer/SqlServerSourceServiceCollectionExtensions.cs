using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Pulse.Server;

namespace Pulse.SqlServer;

public sealed class SqlServerSourceOptions
{
    public string ConnectionString { get; set; } = string.Empty;

    /// <summary>
    /// How long a watcher sleeps between change-tracking polls. Change tracking has no push
    /// notification (unlike Postgres), so this is the delivery latency bound; a small value
    /// makes delivery snappier at the cost of more polling.
    /// </summary>
    public TimeSpan PollInterval { get; set; } = TimeSpan.FromMilliseconds(250);
}

/// <summary>
/// Registers the SQL Server change source and its subscription registry. Other providers
/// mirror this shape exactly (see <c>AddMongoSource</c>/<c>AddPostgresSource</c>), so
/// Pulse.Server itself never references a concrete provider.
/// </summary>
public static class SqlServerSourceServiceCollectionExtensions
{
    public static IServiceCollection AddSqlServerSource(
        this IServiceCollection services,
        Action<SqlServerSourceOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        var options = new SqlServerSourceOptions();
        configure(options);
        if (string.IsNullOrWhiteSpace(options.ConnectionString))
        {
            throw new ArgumentException("SqlServerSourceOptions.ConnectionString is required.", nameof(configure));
        }

        if (options.PollInterval <= TimeSpan.Zero)
        {
            throw new ArgumentException("SqlServerSourceOptions.PollInterval must be positive.", nameof(configure));
        }

        services.AddSignalR();
        // Default resume store is in-memory (does not survive restarts — see README caveats).
        // Register IResumeTokenStore yourself before AddSqlServerSource to override, e.g. FileResumeTokenStore.
        services.TryAddSingleton<IResumeTokenStore, InMemoryResumeTokenStore>();
        services.AddSingleton(sp => new SqlServerChangeSource(
            options.ConnectionString,
            sp.GetService<ILogger<SqlServerChangeSource>>(),
            options.PollInterval));
        services.AddSingleton(sp => new SubscriptionRegistry(
            sp.GetRequiredService<SqlServerChangeSource>(),
            sp.GetRequiredService<IHubContext<PulseHub>>(),
            sp.GetRequiredService<ILogger<SubscriptionRegistry>>(),
            sp.GetRequiredService<IResumeTokenStore>()));
        services.AddSingleton<IPulseAuthorizer, AllowAllAuthorizer>();
        return services;
    }
}
