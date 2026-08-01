using System.Security.Claims;
using System.Threading.Channels;
using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Pulse.Abstractions;
using Pulse.Server;
using Pulse.TestSupport;

namespace Pulse.Server.Tests;

public sealed class PulseHubTests
{
    [Fact]
    public async Task Subscribe_DeliversChangesToEverySubscriber()
    {
        await using var server = await PulseTestServer.StartAsync();
        await using var clientA = await ConnectAsync(server.BaseUrl);
        await using var clientB = await ConnectAsync(server.BaseUrl);
        var receivedA = SubscribeAsync(clientA, "orders");
        var receivedB = SubscribeAsync(clientB, "orders");

        var subIdA = await clientA.InvokeAsync<string>("Subscribe", "orders", null);
        var subIdB = await clientB.InvokeAsync<string>("Subscribe", "orders", null);
        await server.ChangeSource.PublishAsync(InsertEvent("orders", "doc-1", ("status", "pending")));

        var msgA = await ReadMessageAsync(receivedA);
        var msgB = await ReadMessageAsync(receivedB);
        Assert.Equal(subIdA, msgA.SubscriptionId);
        Assert.Equal(subIdB, msgB.SubscriptionId);
        Assert.Equal(ChangeKind.Insert, msgA.Kind);
        Assert.Equal("doc-1", msgA.DocumentId);
        Assert.Equal("pending", msgA.Document!["status"]);
        Assert.Null(msgA.UpdatedFields);
        Assert.NotEqual(default, msgA.Timestamp);
    }

    [Fact]
    public async Task Update_EventIncludesUpdatedFieldsAndDocument()
    {
        await using var server = await PulseTestServer.StartAsync();
        await using var client = await ConnectAsync(server.BaseUrl);
        var received = SubscribeAsync(client, "orders");
        await client.InvokeAsync<string>("Subscribe", "orders", null);

        await server.ChangeSource.PublishAsync(new ChangeEvent(
            Source: "orders",
            Kind: ChangeKind.Update,
            DocumentId: "doc-1",
            FullDocument: new Dictionary<string, object?> { ["status"] = "shipped", ["total"] = 42 },
            UpdatedFields: new Dictionary<string, object?> { ["status"] = "shipped" },
            Token: new ResumeToken("fake:orders", new byte[0]),
            Timestamp: DateTimeOffset.UtcNow));

        var msg = await ReadMessageAsync(received);
        Assert.Equal(ChangeKind.Update, msg.Kind);
        Assert.Equal("shipped", msg.Document!["status"]);
        Assert.Equal("shipped", msg.UpdatedFields!["status"]);
    }

    [Fact]
    public async Task Delete_EventHasNoDocument()
    {
        await using var server = await PulseTestServer.StartAsync();
        await using var client = await ConnectAsync(server.BaseUrl);
        var received = SubscribeAsync(client, "orders");
        await client.InvokeAsync<string>("Subscribe", "orders", null);

        await server.ChangeSource.PublishAsync(InsertEvent("orders", "doc-1", ("status", "pending"))
            with { Kind = ChangeKind.Delete, FullDocument = null });

        var msg = await ReadMessageAsync(received);
        Assert.Equal(ChangeKind.Delete, msg.Kind);
        Assert.Null(msg.Document);
        Assert.Null(msg.UpdatedFields);
    }

    [Fact]
    public async Task SeparateSources_AreDeliveredOnlyToTheirSubscribers()
    {
        await using var server = await PulseTestServer.StartAsync();
        await using var ordersClient = await ConnectAsync(server.BaseUrl);
        await using var customersClient = await ConnectAsync(server.BaseUrl);
        var orders = SubscribeAsync(ordersClient, "orders");
        var customers = SubscribeAsync(customersClient, "customers");
        await ordersClient.InvokeAsync<string>("Subscribe", "orders", null);
        await customersClient.InvokeAsync<string>("Subscribe", "customers", null);

        await server.ChangeSource.PublishAsync(InsertEvent("customers", "cust-1", ("name", "alice")));

        var msg = await ReadMessageAsync(customers);
        Assert.Equal("cust-1", msg.DocumentId);
        await AssertNoMessageAsync(orders);
    }

    [Fact]
    public async Task Unsubscribe_StopsDelivery()
    {
        await using var server = await PulseTestServer.StartAsync();
        await using var client = await ConnectAsync(server.BaseUrl);
        var received = SubscribeAsync(client, "orders");
        var subId = await client.InvokeAsync<string>("Subscribe", "orders", null);

        await client.InvokeAsync("Unsubscribe", subId);
        await server.ChangeSource.PublishAsync(InsertEvent("orders", "doc-1", ("status", "pending")));
        await AssertNoMessageAsync(received);
        Assert.Equal(0, server.ChangeSource.ActiveWatchCount("orders"));
    }

    [Fact]
    public async Task Disconnect_RemovesSubscriptionsAndDisposesWatch()
    {
        await using var server = await PulseTestServer.StartAsync();
        var client = await ConnectAsync(server.BaseUrl);
        await client.InvokeAsync<string>("Subscribe", "orders", null);
        Assert.Equal(1, server.ChangeSource.ActiveWatchCount("orders"));

        await client.DisposeAsync();

        await WaitUntilAsync(() => server.ChangeSource.ActiveWatchCount("orders") == 0);
    }

    [Fact]
    public async Task InvalidFilterJson_ThrowsHubException()
    {
        await using var server = await PulseTestServer.StartAsync();
        await using var client = await ConnectAsync(server.BaseUrl);

        var ex = await Assert.ThrowsAsync<HubException>(
            () => client.InvokeAsync<string>("Subscribe", "orders", "{not json"));
        Assert.Contains("Invalid filter JSON", ex.Message);
    }

    [Fact]
    public async Task DeniedAuthorizer_RejectsSubscription()
    {
        await using var server = await PulseTestServer.StartAsync(configureServices: services =>
        {
            services.AddSingleton<IPulseAuthorizer, DenyAllAuthorizer>();
        });
        await using var client = await ConnectAsync(server.BaseUrl);

        var ex = await Assert.ThrowsAsync<HubException>(
            () => client.InvokeAsync<string>("Subscribe", "orders", null));
        Assert.Contains("Not authorized", ex.Message);
    }

    [Fact]
    public async Task UnhandledSource_ThrowsHubException()
    {
        await using var server = await PulseTestServer.StartAsync(registerDefaultRegistry: false, configureServices: services =>
        {
            services.AddSingleton(sp => new RejectingRegistry(
                sp.GetRequiredService<IChangeSource>(),
                sp.GetRequiredService<IHubContext<PulseHub>>(),
                sp.GetRequiredService<ILogger<SubscriptionRegistry>>()));
        });
        await using var client = await ConnectAsync(server.BaseUrl);

        var ex = await Assert.ThrowsAsync<HubException>(
            () => client.InvokeAsync<string>("Subscribe", "orders", null));
        Assert.Contains("No registered provider", ex.Message);
    }

    [Fact]
    public async Task ProviderStartFailure_ThrowsHubException_AndCleansUp()
    {
        await using var server = await PulseTestServer.StartAsync();
        server.ChangeSource.StartException = new InvalidOperationException("oplog not available");
        await using var client = await ConnectAsync(server.BaseUrl);

        var ex = await Assert.ThrowsAsync<HubException>(
            () => client.InvokeAsync<string>("Subscribe", "orders", null));
        Assert.Contains("oplog not available", ex.Message);
        Assert.Equal(0, server.ChangeSource.ActiveWatchCount("orders"));
    }

    [Fact]
    public async Task FilteredSubscription_DeliversOnlyMatchingChanges()
    {
        await using var server = await PulseTestServer.StartAsync();
        await using var client = await ConnectAsync(server.BaseUrl);
        var received = SubscribeAsync(client, "orders");
        var whereJson = """{"field":"status","op":"eq","value":"pending"}""";
        await client.InvokeAsync<string>("Subscribe", "orders", whereJson);

        await server.ChangeSource.PublishAsync(InsertEvent("orders", "doc-1", ("status", "shipped")));
        await AssertNoMessageAsync(received);

        await server.ChangeSource.PublishAsync(InsertEvent("orders", "doc-2", ("status", "pending")));

        var msg = await ReadMessageAsync(received);
        Assert.Equal("doc-2", msg.DocumentId);
    }

    [Fact]
    public async Task FilteredSubscription_AlwaysReceivesDeletes()
    {
        await using var server = await PulseTestServer.StartAsync();
        await using var client = await ConnectAsync(server.BaseUrl);
        var received = SubscribeAsync(client, "orders");
        await client.InvokeAsync<string>("Subscribe", "orders",
            """{"field":"status","op":"eq","value":"pending"}""");

        await server.ChangeSource.PublishAsync(InsertEvent("orders", "doc-1", ("status", "pending"))
            with { Kind = ChangeKind.Delete, FullDocument = null });

        var msg = await ReadMessageAsync(received);
        Assert.Equal(ChangeKind.Delete, msg.Kind);
        Assert.Equal("doc-1", msg.DocumentId);
    }

    [Fact]
    public async Task FilteredSubscription_UsesNestedFilter()
    {
        await using var server = await PulseTestServer.StartAsync();
        await using var client = await ConnectAsync(server.BaseUrl);
        var received = SubscribeAsync(client, "orders");
        await client.InvokeAsync<string>("Subscribe", "orders",
            """{"and":[{"field":"customer.address.city","op":"eq","value":"berlin"},{"field":"total","op":"gte","value":100}]}""");

        await server.ChangeSource.PublishAsync(InsertEvent("orders", "doc-1",
            ("customer", new Dictionary<string, object?> { ["address"] = new Dictionary<string, object?> { ["city"] = "berlin" } }),
            ("total", 100)));
        var msg = await ReadMessageAsync(received);
        Assert.Equal("doc-1", msg.DocumentId);

        await server.ChangeSource.PublishAsync(InsertEvent("orders", "doc-2",
            ("customer", new Dictionary<string, object?> { ["address"] = new Dictionary<string, object?> { ["city"] = "paris" } }),
            ("total", 500)));
        await AssertNoMessageAsync(received);
    }

    [Fact]
    public async Task Subscribe_DeliversSnapshotBeforeChanges()
    {
        await using var server = await PulseTestServer.StartAsync();
        await using var client = await ConnectAsync(server.BaseUrl);
        var received = SubscribeAllAsync(client);
        var snapshotDocs = new IReadOnlyDictionary<string, object?>[]
        {
            new Dictionary<string, object?> { ["_id"] = "a", ["status"] = "pending" },
            new Dictionary<string, object?> { ["_id"] = "b", ["status"] = "pending" },
        };
        server.ChangeSource.SnapshotProvider = (source, filter, ct) => Task.FromResult(
            ((IReadOnlyList<IReadOnlyDictionary<string, object?>>)snapshotDocs, new ResumeToken("fake:orders", Array.Empty<byte>())));

        await client.InvokeAsync<string>("Subscribe", "orders", null);
        await server.ChangeSource.PublishAsync(InsertEvent("orders", "c", ("status", "pending")));

        var first = await ReadMessageAsync(received);
        var snapshot = Assert.IsType<PulseSnapshotMessage>(first);
        Assert.Equal(2, snapshot.Documents.Count);
        Assert.Equal("a", snapshot.Documents[0]["_id"]);

        var second = await ReadMessageAsync(received);
        var change = Assert.IsType<PulseChangeMessage>(second);
        Assert.Equal("c", change.DocumentId);
    }

    [Fact]
    public async Task Subscribe_PassesWhereFilterToSnapshot()
    {
        await using var server = await PulseTestServer.StartAsync();
        await using var client = await ConnectAsync(server.BaseUrl);

        await client.InvokeAsync<string>("Subscribe", "orders",
            """{"field":"status","op":"eq","value":"pending"}""");

        Assert.NotNull(server.ChangeSource.LastSnapshotFilter);
        Assert.Equal("orders", server.ChangeSource.LastSnapshotFilter!.Source);
        var compare = Assert.IsType<FieldCompare>(server.ChangeSource.LastSnapshotFilter.Where);
        Assert.Equal("status", compare.Field);
        Assert.Equal(CompareOp.Eq, compare.Op);
        Assert.Equal("pending", compare.Value);
        Assert.Equal(1, server.ChangeSource.SnapshotCallCount);
    }

    [Fact]
    public async Task ChangeArrivingDuringSnapshot_DeliveredAfterSnapshot()
    {
        await using var server = await PulseTestServer.StartAsync();
        await using var client = await ConnectAsync(server.BaseUrl);
        var received = SubscribeAllAsync(client);
        server.ChangeSource.SnapshotProvider = async (source, filter, ct) =>
        {
            await Task.Delay(150, ct);
            return (Array.Empty<IReadOnlyDictionary<string, object?>>(), new ResumeToken("fake:orders", Array.Empty<byte>()));
        };

        var subscribeTask = client.InvokeAsync<string>("Subscribe", "orders", null);
        await WaitUntilAsync(() => server.ChangeSource.ActiveWatchCount("orders") == 1);
        await server.ChangeSource.PublishAsync(InsertEvent("orders", "c", ("status", "pending")));
        await subscribeTask;

        var first = await ReadMessageAsync(received);
        Assert.IsType<PulseSnapshotMessage>(first);

        var second = await ReadMessageAsync(received);
        var change = Assert.IsType<PulseChangeMessage>(second);
        Assert.Equal("c", change.DocumentId);
    }

    [Fact]
    public async Task SnapshotProviderFailure_ThrowsHubException_AndCleansUp()
    {
        await using var server = await PulseTestServer.StartAsync();
        await using var client = await ConnectAsync(server.BaseUrl);
        server.ChangeSource.SnapshotProvider = (source, filter, ct)
            => throw new InvalidOperationException("snapshot boom");

        await Assert.ThrowsAsync<HubException>(
            () => client.InvokeAsync<string>("Subscribe", "orders", null));
        Assert.Equal(0, server.ChangeSource.ActiveWatchCount("orders"));
    }

    private static Channel<object> SubscribeAllAsync(HubConnection connection)
    {
        var channel = Channel.CreateUnbounded<object>();
        connection.On<PulseChangeMessage>("PulseChange", message => channel.Writer.WriteAsync(message).AsTask());
        connection.On<PulseSnapshotMessage>("PulseSnapshot", message => channel.Writer.WriteAsync(message).AsTask());
        return channel;
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

    private static Channel<PulseChangeMessage> SubscribeAsync(
        HubConnection connection,
        string source)
    {
        var channel = Channel.CreateUnbounded<PulseChangeMessage>();
        connection.On<PulseChangeMessage>("PulseChange", message => channel.Writer.WriteAsync(message).AsTask());
        return channel;
    }

    private static ChangeEvent InsertEvent(
        string source,
        string documentId,
        params (string Key, object? Value)[] fields)
        => new(
            Source: source,
            Kind: ChangeKind.Insert,
            DocumentId: documentId,
            FullDocument: fields.ToDictionary(f => f.Key, f => f.Value),
            UpdatedFields: null,
            Token: new ResumeToken($"fake:{source}", new byte[0]),
            Timestamp: DateTimeOffset.UtcNow);

    private static async Task<T> ReadMessageAsync<T>(Channel<T> channel, TimeSpan? timeout = null)
    {
        using var cts = new CancellationTokenSource(timeout ?? TimeSpan.FromSeconds(10));
        return await channel.Reader.ReadAsync(cts.Token);
    }

    private static async Task AssertNoMessageAsync<T>(Channel<T> channel)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(1));
        await Assert.ThrowsAsync<OperationCanceledException>(() => channel.Reader.ReadAsync(cts.Token).AsTask());
    }

    private static async Task WaitUntilAsync(Func<bool> condition, TimeSpan? timeout = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(5));
        while (!condition())
        {
            if (DateTime.UtcNow > deadline)
            {
                Assert.Fail("Condition was not met within the timeout.");
            }

            await Task.Delay(25);
        }
    }

    private sealed class DenyAllAuthorizer : IPulseAuthorizer
    {
        public ValueTask<bool> AuthorizeAsync(string source, ClaimsPrincipal? principal)
            => ValueTask.FromResult(false);
    }

    private sealed class RejectingRegistry : SubscriptionRegistry
    {
        public RejectingRegistry(
            IChangeSource changeSource,
            IHubContext<PulseHub> hubContext,
            ILogger<SubscriptionRegistry> logger)
            : base(changeSource, hubContext, logger)
        {
        }

        public override bool CanHandle(string source) => false;
    }
}
