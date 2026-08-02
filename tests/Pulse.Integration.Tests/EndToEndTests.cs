using System.Net.Sockets;
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
using Pulse.Abstractions.Json;
using Pulse.Client;
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
        // (Updates that flip a doc in/out of the filter are match-transition logic, covered
        // in PulseClient_UpdateTransitions_ReflectInCurrent.)
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

    [Fact]
    public async Task ServerRestart_ResumesFromPersistedToken_WithNoGap()
    {
        var resumeDir = Path.Combine(Path.GetTempPath(), "pulse_resume_it_" + Guid.NewGuid().ToString("N"));
        try
        {
            await _orders.InsertOneAsync(new BsonDocument { { "_id", "a" }, { "status", "pending" } });
            await _orders.InsertOneAsync(new BsonDocument { { "_id", "b" }, { "status", "pending" } });

            // First server: subscribe, receive one change (c), then shut down.
            var serverA = await StartServerAsync(_database, resumeDir);
            var connA = await ConnectAsync(serverA.BaseUrl);
            var receivedA = Channel.CreateUnbounded<object>();
            connA.On<PulseChangeMessage>("PulseChange", message => receivedA.Writer.WriteAsync(message).AsTask());
            connA.On<PulseSnapshotMessage>("PulseSnapshot", message => receivedA.Writer.WriteAsync(message).AsTask());
            await connA.InvokeAsync<string>("Subscribe", "orders", null);
            Assert.IsType<PulseSnapshotMessage>(await ReadMessageAsync(receivedA));

            await _orders.InsertOneAsync(new BsonDocument { { "_id", "c" }, { "status", "pending" } });
            Assert.Equal("c", Assert.IsType<PulseChangeMessage>(await ReadMessageAsync(receivedA)).DocumentId);
            await connA.DisposeAsync();
            await serverA.App.DisposeAsync();

            // The resume point is persisted (asynchronously after delivery, so poll).
            await WaitUntilAsync(() => Directory.GetFiles(resumeDir).Length == 1);

            // A change lands while nothing is watching.
            await _orders.InsertOneAsync(new BsonDocument { { "_id", "d" }, { "status", "pending" } });

            // Second server with the same store: nothing is lost (d is in the snapshot)
            // and nothing before the resume point (a/b/c) is replayed.
            var serverB = await StartServerAsync(_database, resumeDir);
            var connB = await ConnectAsync(serverB.BaseUrl);
            var receivedB = Channel.CreateUnbounded<object>();
            connB.On<PulseChangeMessage>("PulseChange", message => receivedB.Writer.WriteAsync(message).AsTask());
            connB.On<PulseSnapshotMessage>("PulseSnapshot", message => receivedB.Writer.WriteAsync(message).AsTask());
            await connB.InvokeAsync<string>("Subscribe", "orders", null);

            var snapshot = Assert.IsType<PulseSnapshotMessage>(await ReadMessageAsync(receivedB));
            Assert.Equal(4, snapshot.Documents.Count);
            Assert.Contains(snapshot.Documents, doc => doc["_id"] is "d");

            // The resumed watch may replay the change that happened while nothing was
            // watching (a benign duplicate of the snapshot). If it does, it must be 'd' —
            // never a pre-resume document.
            if (await TryReadAsync(receivedB, TimeSpan.FromSeconds(2)) is PulseChangeMessage replay)
            {
                Assert.Equal("d", replay.DocumentId);
            }

            // Live flow still works after resume.
            await _orders.InsertOneAsync(new BsonDocument { { "_id", "e" }, { "status", "pending" } });
            var live = Assert.IsType<PulseChangeMessage>(await ReadMessageAsync(receivedB));
            Assert.Equal("e", live.DocumentId);

            await connB.DisposeAsync();
            await serverB.App.DisposeAsync();
        }
        finally
        {
            if (Directory.Exists(resumeDir))
            {
                Directory.Delete(resumeDir, recursive: true);
            }
        }
    }

    [Fact]
    public async Task PulseClient_Subscribes_AndMaintainsCurrent()
    {
        await _orders.InsertOneAsync(new BsonDocument { { "_id", "a" }, { "status", "pending" }, { "total", 50 } });
        await _orders.InsertOneAsync(new BsonDocument { { "_id", "b" }, { "status", "shipped" }, { "total", 200 } });

        var server = await StartServerAsync(_database);
        await using var client = new PulseClient(server.BaseUrl + "/pulse");
        await client.ConnectAsync();

        var snapshotTcs = new TaskCompletionSource<IReadOnlyList<Order>>();
        var sub = await client.Subscribe<Order>("orders", new FieldCompare("status", CompareOp.Eq, "pending"));
        sub.OnSnapshot += docs => snapshotTcs.TrySetResult(docs);

        var snapshot = await snapshotTcs.Task.WaitAsync(TimeSpan.FromSeconds(15));
        Assert.Single(snapshot);
        Assert.Equal("a", snapshot[0]._id);
        Assert.Equal("a", Assert.Single(sub.Current)._id);

        // A matching insert arrives live and updates Current.
        var changes = Channel.CreateUnbounded<PulseChange<Order>>();
        sub.OnChange += change => changes.Writer.TryWrite(change);
        await _orders.InsertOneAsync(new BsonDocument { { "_id", "c" }, { "status", "pending" }, { "total", 99 } });
        var insert = await ReadMessageAsync(changes);
        Assert.Equal(ChangeKind.Insert, insert.Kind);
        Assert.Equal("c", insert.DocumentId);
        Assert.Equal(99, insert.Document!.total);
        Assert.Equal(2, sub.Current.Count);

        // A matching update arrives with updated fields.
        await _orders.UpdateOneAsync(
            Builders<BsonDocument>.Filter.Eq("_id", "c"),
            Builders<BsonDocument>.Update.Set("total", 150));
        var update = await ReadMessageAsync(changes);
        Assert.Equal(ChangeKind.Update, update.Kind);
        Assert.Equal(150L, update.UpdatedFields!["total"]);
        Assert.Equal(150, sub.Current.Single(d => d._id == "c").total);

        // A delete removes the document from Current.
        await _orders.DeleteOneAsync(Builders<BsonDocument>.Filter.Eq("_id", "a"));
        var delete = await ReadMessageAsync(changes);
        Assert.Equal(ChangeKind.Delete, delete.Kind);
        Assert.DoesNotContain(sub.Current, d => d._id == "a");

        await sub.UnsubscribeAsync();
        await client.DisposeAsync();
        await server.App.DisposeAsync();
    }

    [Fact]
    public async Task PulseClient_Reconnects_Resubscribes_AndFiresFreshSnapshot()
    {
        await _orders.InsertOneAsync(new BsonDocument { { "_id", "a" }, { "status", "pending" } });

        var serverA = await StartServerAsync(_database);
        var port = new Uri(serverA.BaseUrl).Port;

        await using var client = new PulseClient(serverA.BaseUrl + "/pulse");
        await client.ConnectAsync();

        var snapshotCount = 0;
        var secondSnapshot = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var sub = await client.Subscribe<Order>("orders", new FieldCompare("status", CompareOp.Eq, "pending"));
        sub.OnSnapshot += _ =>
        {
            if (Interlocked.Increment(ref snapshotCount) == 2)
            {
                secondSnapshot.TrySetResult(true);
            }
        };

        await WaitUntilAsync(() => Volatile.Read(ref snapshotCount) == 1);

        // Kill the server: the client drops and starts automatic reconnect attempts.
        await serverA.App.StopAsync();
        await serverA.App.DisposeAsync();

        // Wait for the client to notice the drop AND for serverA's port to be fully
        // released before the new server starts, so the client reconnects exactly once
        // (its immediate retry fails against the closed port, the next hits serverB).
        await WaitUntilAsync(() => client.State == HubConnectionState.Reconnecting);
        await WaitUntilAsync(() => !IsPortOpen("127.0.0.1", port));

        // Bring it back on the same port; the client reconnects and re-subscribes,
        // treating the result as a fresh snapshot.
        var serverB = await StartServerAsync(_database, null, port);
        await secondSnapshot.Task.WaitAsync(TimeSpan.FromSeconds(20));

        Assert.Equal(2, snapshotCount);
        Assert.Equal("a", Assert.Single(sub.Current)._id);

        // Live changes still flow on the resubscribed connection.
        var changes = Channel.CreateUnbounded<PulseChange<Order>>();
        sub.OnChange += change => changes.Writer.TryWrite(change);
        await _orders.InsertOneAsync(new BsonDocument { { "_id", "b" }, { "status", "pending" } });
        var change = await ReadMessageAsync(changes);
        Assert.Equal("b", change.DocumentId);
        Assert.Equal(ChangeKind.Insert, change.Kind);
        Assert.NotNull(change.Document);
        await ((PulseSubscription<Order>)sub).WaitForIdleAsync();
        Assert.Equal(2, sub.Current.Count);

        await sub.UnsubscribeAsync();
        await client.DisposeAsync();
        await serverB.App.DisposeAsync();
    }

    [Fact]
    public async Task PulseClient_UpdateTransitions_ReflectInCurrent()
    {
        await _orders.InsertOneAsync(new BsonDocument { { "_id", "a" }, { "status", "pending" } });
        await _orders.InsertOneAsync(new BsonDocument { { "_id", "b" }, { "status", "shipped" } });

        var server = await StartServerAsync(_database);
        await using var client = new PulseClient(server.BaseUrl + "/pulse");
        await client.ConnectAsync();

        var snapshotTcs = new TaskCompletionSource<IReadOnlyList<Order>>();
        var sub = await client.Subscribe<Order>("orders", new FieldCompare("status", CompareOp.Eq, "pending"));
        sub.OnSnapshot += docs => snapshotTcs.TrySetResult(docs);
        await snapshotTcs.Task.WaitAsync(TimeSpan.FromSeconds(15));
        Assert.Equal("a", Assert.Single(sub.Current)._id);

        var changes = Channel.CreateUnbounded<PulseChange<Order>>();
        sub.OnChange += change => changes.Writer.TryWrite(change);

        // Flip a out of the filter → the subscriber gets a synthetic delete.
        await _orders.UpdateOneAsync(
            Builders<BsonDocument>.Filter.Eq("_id", "a"),
            Builders<BsonDocument>.Update.Set("status", "shipped"));
        var del = await ReadMessageAsync(changes);
        Assert.Equal(ChangeKind.Delete, del.Kind);
        Assert.Equal("a", del.DocumentId);
        Assert.Null(del.Document);
        Assert.Empty(sub.Current);

        // Flip b into the filter → it arrives as an insert (the subscriber never saw it matching).
        await _orders.UpdateOneAsync(
            Builders<BsonDocument>.Filter.Eq("_id", "b"),
            Builders<BsonDocument>.Update.Set("status", "pending"));
        var ins = await ReadMessageAsync(changes);
        Assert.Equal(ChangeKind.Insert, ins.Kind);
        Assert.Equal("b", ins.DocumentId);
        Assert.Equal("b", Assert.Single(sub.Current)._id);

        await sub.UnsubscribeAsync();
        await client.DisposeAsync();
        await server.App.DisposeAsync();
    }

    private static async Task<(WebApplication App, string BaseUrl)> StartServerAsync(IMongoDatabase database)
        => await StartServerAsync(database, null, null);

    private static async Task<(WebApplication App, string BaseUrl)> StartServerAsync(
        IMongoDatabase database,
        string? resumeTokenDirectory)
        => await StartServerAsync(database, resumeTokenDirectory, null);

    private static async Task<(WebApplication App, string BaseUrl)> StartServerAsync(
        IMongoDatabase database,
        string? resumeTokenDirectory,
        int? port)
    {
        var url = port is null ? "http://127.0.0.1:0" : $"http://127.0.0.1:{port}";
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseUrls(url);
        builder.Logging.ClearProviders();
        builder.Services.AddSignalR();
        builder.Services.AddSingleton(database);
        builder.Services.AddSingleton<IChangeSource, MongoChangeSource>();
        if (resumeTokenDirectory is not null)
        {
            builder.Services.AddSingleton<IResumeTokenStore>(new FileResumeTokenStore(resumeTokenDirectory));
        }

        builder.Services.AddSingleton(sp => new SubscriptionRegistry(
            sp.GetRequiredService<IChangeSource>(),
            sp.GetRequiredService<IHubContext<PulseHub>>(),
            sp.GetRequiredService<ILogger<SubscriptionRegistry>>(),
            sp.GetService<IResumeTokenStore>()));
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

    private static async Task<T?> TryReadAsync<T>(Channel<T> channel, TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        try
        {
            return await channel.Reader.ReadAsync(cts.Token);
        }
        catch (OperationCanceledException)
        {
            return default;
        }
    }

    private static bool IsPortOpen(string host, int port)
    {
        using var client = new System.Net.Sockets.TcpClient();
        try
        {
            client.Connect(host, port);
            return true;
        }
        catch (SocketException)
        {
            return false;
        }
    }

    private static async Task WaitUntilAsync(Func<bool> condition, TimeSpan? timeout = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(5));
        while (!condition())
        {
            if (DateTime.UtcNow > deadline)
            {
                Assert.Fail("Condition was not satisfied within the timeout.");
            }

            await Task.Delay(50);
        }
    }
}

/// <summary>POCO used to exercise typed client-side deserialization of snapshot/changes.</summary>
public sealed class Order
{
    public string _id { get; set; } = "";
    public string status { get; set; } = "";
    public long total { get; set; }
}
