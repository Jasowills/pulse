using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using MongoDB.Driver;
using Pulse.Server;

namespace Pulse.Mongo;

public sealed class MongoSourceOptions
{
    public string ConnectionString { get; set; } = string.Empty;
    public string Database { get; set; } = string.Empty;
}

/// <summary>
/// Registers the Mongo change source and its subscription registry. Other providers
/// mirror this shape exactly (see <c>AddPostgresSource</c>/<c>AddSqlServerSource</c> in v0.2),
/// so Pulse.Server itself never references a concrete provider.
/// </summary>
public static class MongoSourceServiceCollectionExtensions
{
    public static IServiceCollection AddMongoSource(
        this IServiceCollection services,
        Action<MongoSourceOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        var options = new MongoSourceOptions();
        configure(options);
        if (string.IsNullOrWhiteSpace(options.ConnectionString))
        {
            throw new ArgumentException("MongoSourceOptions.ConnectionString is required.", nameof(configure));
        }

        if (string.IsNullOrWhiteSpace(options.Database))
        {
            throw new ArgumentException("MongoSourceOptions.Database is required.", nameof(configure));
        }

        services.AddSignalR();
        services.AddSingleton(new MongoClient(MongoClientSettings.FromConnectionString(options.ConnectionString)));
        services.AddSingleton(sp => new MongoChangeSource(
            sp.GetRequiredService<IMongoClient>().GetDatabase(options.Database),
            sp.GetService<ILogger<MongoChangeSource>>()));
        services.AddSingleton(sp => new SubscriptionRegistry(
            sp.GetRequiredService<MongoChangeSource>(),
            sp.GetRequiredService<IHubContext<PulseHub>>(),
            sp.GetRequiredService<ILogger<SubscriptionRegistry>>()));
        services.AddSingleton<IPulseAuthorizer, AllowAllAuthorizer>();
        return services;
    }
}
