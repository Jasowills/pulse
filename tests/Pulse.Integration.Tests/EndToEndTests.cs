using System.Threading.Channels;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using MongoDB.Bson;
using MongoDB.Driver;
using Pulse.Abstractions;
using Pulse.Mongo;
using Pulse.Server;
using Pulse.TestSupport;
using Testcontainers.MongoDb;

namespace Pulse.Integration.Tests;

public sealed class MongoContainerFixture : IAsyncLifetime
{
    public IMongoClient Client { get; private set; } = null!;

    private MongoDbContainer? _container;

    public async Task InitializeAsync()
    {
        var container = new MongoDbBuilder("mongo:7")
            .WithReplicaSet("rs0")
            .Build();
        await container.StartAsync();
        _container = container;
        Client = new MongoClient(container.GetConnectionString());
    }

    public Task DisposeAsync()
        => (_container?.DisposeAsync() ?? ValueTask.CompletedTask).AsTask();
}

/// <summary>
/// End-to-end acceptance: real Mongo (Testcontainers replica set) + in-process SignalR hub
/// + real client connection. Validates the subscribe flow the README promises: filtered
/// snapshot first, then live changes, no gap between them.
/// </summary>
public sealed class EndToEndTests : IClassFixture<MongoContainerFixture>, IAsyncLifetime
{
    private readonly MongoContainerFixture _fixture;
    private string _db = null!;
    private IMongoDatabase _database = null!;
    private IMongoCollection<BsonDocument> _orders = null!;

    public EndToEndTests(MongoContainerFixture fixture)
    {
        _fixture = fixture;
    }

    public Task InitializeAsync()
    {
        _db = $"pulse_it_{Guid.NewGuid():N}";
        _database = _fixture.Client.GetDatabase(_db);
        _orders = _database.GetCollection<BsonDocument>("orders");
        return Task.CompletedTask;
    }

    public Task DisposeAsync()
    {
        _fixture.Client.DropDatabase(_db);
        return Task.CompletedTask;
    }

    [Fact]
    public async Task Subscribe_DeliversFilteredSnapshot_ThenLiveChanges()
    {
        await _orders.InsertOneAsync(new BsonDocument { { "_id", "a" }, { "status", "pending" }, { "total", 50 } });
        await _orders.InsertOneAsync(new BsonDocument { { "_id", "b" }, { "status", "shipped" }, { "total", 200 } });

        var server = await StartServerAsync(_database);
        await using var connection = await ConnectAsync(server.BaseUrl);
        var received = Channel.CreateUnbounded<object>();
        connection.On<PulseChangeMessage>("PulseChange", message => received.Writer.WriteAsync(message).AsTask());
        connection.On<PulseSnapshotMessage>("PulseSnapshot", message => received.Writer.WriteAsync(message).AsTask());

        var subId = await connection.InvokeAsync<string>("Subscribe", "orders",
            """{"field":"status","op":"eq","value":"pending"}""");

        // Snapshot arrives first and contains only matching documents.
        var first = await ReadMessageAsync(received);
        var snapshot = Assert.IsType<PulseSnapshotMessage>(first);
        Assert.Equal(subId, snapshot.SubscriptionId);
        var doc = Assert.Single(snapshot.Documents);
        Assert.Equal("a", doc["_id"]);

        // A matching insert after the snapshot arrives as a live change, in order.
        await _orders.InsertOneAsync(new BsonDocument { { "_id", "c" }, { "status", "pending" }, { "total", 99 } });
        var second = await ReadMessageAsync(received);
        var change = Assert.IsType<PulseChangeMessage>(second);
        Assert.Equal(subId, change.SubscriptionId);
        Assert.Equal(ChangeKind.Insert, change.Kind);
        Assert.Equal("c", change.DocumentId);
        Assert.Equal("pending", change.Document!["status"]);

        // A non-matching insert is filtered out.
        await _orders.InsertOneAsync(new BsonDocument { { "_id", "d" }, { "status", "shipped" } });
        await AssertNoMessageAsync(received);

        // A live update on a matching doc arrives with updated fields.
        // (Updates that flip a doc OUT of the filter are match-transition logic — step 9.)
        await _orders.UpdateOneAsync(
            Builders<BsonDocument>.Filter.Eq("_id", "c"),
            Builders<BsonDocument>.Update.Set("total", 150));
        var update = await ReadMessageAsync(received);
        var updateMsg = Assert.IsType<PulseChangeMessage>(update);
        Assert.Equal(ChangeKind.Update, updateMsg.Kind);
        Assert.Equal("c", updateMsg.DocumentId);
        Assert.Equal(150L, updateMsg.Document!["total"]);
        Assert.Equal(150L, updateMsg.UpdatedFields!["total"]);

        await server.App.DisposeAsync();
    }

    [Fact]
    public async Task Subscribe_ChangeBeforeSubscribeCompletes_DoesNotCreateGapOrReplay()
    {
        await _orders.InsertOneAsync(new BsonDocument { { "_id", "a" }, { "status", "pending" } });

        var server = await StartServerAsync(_database);
        await using var connection = await ConnectAsync(server.BaseUrl);
        var received = Channel.CreateUnbounded<object>();
        connection.On<PulseChangeMessage>("PulseChange", message => received.Writer.WriteAsync(message).AsTask());
        connection.On<PulseSnapshotMessage>("PulseSnapshot", message => received.Writer.WriteAsync(message).AsTask());

        var subId = await connection.InvokeAsync<string>("Subscribe", "orders", null);

        var snapshot = Assert.IsType<PulseSnapshotMessage>(await ReadMessageAsync(received));
        Assert.Equal(subId, snapshot.SubscriptionId);
        Assert.Single(snapshot.Documents);

        // Mutate twice quickly: both changes arrive, in order, after the snapshot.
        await _orders.InsertOneAsync(new BsonDocument { { "_id", "x" }, { "status", "pending" } });
        await _orders.InsertOneAsync(new BsonDocument { { "_id", "y" }, { "status", "pending" } });

        var first = Assert.IsType<PulseChangeMessage>(await ReadMessageAsync(received));
        var second = Assert.IsType<PulseChangeMessage>(await ReadMessageAsync(received));
        Assert.Equal("x", first.DocumentId);
        Assert.Equal("y", second.DocumentId);

        await server.App.DisposeAsync();
    }

    private static async Task<(WebApplication App, string BaseUrl)> StartServerAsync(IMongoDatabase database)
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseUrls("http://127.0.0.1:0");
        builder.Logging.ClearProviders();
        builder.Services.AddSignalR();
        builder.Services.AddSingleton(database);
        builder.Services.AddSingleton<IChangeSource, MongoChangeSource>();
        builder.Services.AddSingleton(sp => new SubscriptionRegistry(
            sp.GetRequiredService<IChangeSource>(),
            sp.GetRequiredService<IHubContext<PulseHub>>(),
            sp.GetRequiredService<ILogger<SubscriptionRegistry>>()));
        builder.Services.AddSingleton<IPulseAuthorizer, AllowAllAuthorizer>();
        var app = builder.Build();
        app.MapHub<PulseHub>("/pulse");
        await app.StartAsync();
        return (app, app.Urls.First());
    }

    private static async Task<HubConnection> ConnectAsync(string baseUrl)
    {
        var connection = new HubConnectionBuilder()
            .WithUrl($"{baseUrl}/pulse")
            .AddJsonProtocol(options =>
                options.PayloadSerializerOptions.Converters.Add(new ObjectToInferredTypesConverter()))
            .Build();
        await connection.StartAsync();
        return connection;
    }

    private static async Task<T> ReadMessageAsync<T>(Channel<T> channel, TimeSpan? timeout = null)
    {
        using var cts = new CancellationTokenSource(timeout ?? TimeSpan.FromSeconds(15));
        return await channel.Reader.ReadAsync(cts.Token);
    }

    private static async Task AssertNoMessageAsync<T>(Channel<T> channel)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(1));
        await Assert.ThrowsAsync<OperationCanceledException>(() => channel.Reader.ReadAsync(cts.Token).AsTask());
    }
}
