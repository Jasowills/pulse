using System.Threading.Channels;
using MongoDB.Bson;
using MongoDB.Driver;
using Pulse.Abstractions;
using Testcontainers.MongoDb;
using Xunit;

namespace Pulse.Mongo.Tests;

public sealed class MongoContainerFixture : IAsyncLifetime
{
    public IMongoClient Client { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        var container = new MongoDbBuilder("mongo:7")
            .WithReplicaSet("rs0")
            .Build();
        await container.StartAsync();
        Container = container;
        Client = new MongoClient(container.GetConnectionString());
    }

    public MongoDbContainer? Container { get; private set; }

    public Task DisposeAsync()
        => (Container?.DisposeAsync() ?? ValueTask.CompletedTask).AsTask();
}

public sealed class MongoChangeSourceTests : IClassFixture<MongoContainerFixture>, IDisposable
{
    private readonly MongoContainerFixture _fixture;
    private readonly string _db;
    private readonly IMongoCollection<BsonDocument> _orders;
    private readonly IChangeSource _source;

    public MongoChangeSourceTests(MongoContainerFixture fixture)
    {
        _fixture = fixture;
        _db = $"pulse_{Guid.NewGuid():N}";
        _orders = fixture.Client.GetDatabase(_db).GetCollection<BsonDocument>("orders");
        _source = new MongoChangeSource(fixture.Client.GetDatabase(_db));
    }

    public void Dispose()
    {
        _fixture.Client.DropDatabase(_db);
    }

    [Fact]
    public async Task Insert_PublishesChangeEvent_WithFullDocument()
    {
        await using var sub = await SubscribeAsync(_source, "orders");

        var doc = new BsonDocument { { "status", "pending" }, { "total", 42 } };
        await _orders.InsertOneAsync(doc);
        var before = DateTimeOffset.UtcNow;

        var e = await WaitForAsync(sub);

        Assert.Equal("orders", e.Source);
        Assert.Equal(ChangeKind.Insert, e.Kind);
        Assert.Equal(doc["_id"].AsObjectId.ToString(), e.DocumentId);
        Assert.NotNull(e.FullDocument);
        Assert.Equal("pending", e.FullDocument["status"]);
        Assert.Equal(42, e.FullDocument["total"]);
        Assert.Null(e.UpdatedFields);
        Assert.Equal($"mongo:{_db}.orders", e.Token.ProviderId);
        Assert.NotEmpty(e.Token.Opaque);
        Assert.InRange(e.Timestamp, before.AddSeconds(-5), DateTimeOffset.UtcNow.AddSeconds(5));
    }

    [Fact]
    public async Task Update_PublishesUpdatedFields_AndFullDocumentFromLookup()
    {
        var doc = new BsonDocument { { "status", "pending" }, { "total", 42 } };
        await _orders.InsertOneAsync(doc);

        await using var sub = await SubscribeAsync(_source, "orders");

        await _orders.UpdateOneAsync(
            Builders<BsonDocument>.Filter.Eq("_id", doc["_id"]),
            Builders<BsonDocument>.Update.Set("status", "shipped"));

        var e = await WaitForAsync(sub);

        Assert.Equal(ChangeKind.Update, e.Kind);
        Assert.Equal(doc["_id"].AsObjectId.ToString(), e.DocumentId);
        Assert.NotNull(e.UpdatedFields);
        Assert.Equal("shipped", e.UpdatedFields["status"]);
        Assert.NotNull(e.FullDocument);
        Assert.Equal("shipped", e.FullDocument["status"]);
        Assert.Equal(42, e.FullDocument["total"]);
    }

    [Fact]
    public async Task Replace_PublishesReplace_WithReplacementDocument()
    {
        var id = "order-1";
        await _orders.InsertOneAsync(new BsonDocument { { "_id", id }, { "status", "pending" } });

        await using var sub = await SubscribeAsync(_source, "orders");

        await _orders.ReplaceOneAsync(
            Builders<BsonDocument>.Filter.Eq("_id", id),
            new BsonDocument { { "_id", id }, { "status", "done" }, { "total", 99 } });

        var e = await WaitForAsync(sub);

        Assert.Equal(ChangeKind.Replace, e.Kind);
        Assert.Equal(id, e.DocumentId);
        Assert.NotNull(e.FullDocument);
        Assert.Equal("done", e.FullDocument["status"]);
        Assert.Equal(99, e.FullDocument["total"]);
    }

    [Fact]
    public async Task Delete_PublishesDelete_WithoutFullDocument()
    {
        var doc = new BsonDocument { { "status", "pending" } };
        await _orders.InsertOneAsync(doc);

        await using var sub = await SubscribeAsync(_source, "orders");

        await _orders.DeleteOneAsync(Builders<BsonDocument>.Filter.Eq("_id", doc["_id"]));

        var e = await WaitForAsync(sub);

        Assert.Equal(ChangeKind.Delete, e.Kind);
        Assert.Equal(doc["_id"].AsObjectId.ToString(), e.DocumentId);
        Assert.Null(e.FullDocument);
        Assert.Null(e.UpdatedFields);
    }

    [Fact]
    public async Task SharedWatch_FansOutToAllSubscribers_AndSurvivesOneDisposal()
    {
        await using var sub1 = await SubscribeAsync(_source, "orders");
        await using var sub2 = await SubscribeAsync(_source, "orders");

        var doc = new BsonDocument { { "status", "pending" } };
        await _orders.InsertOneAsync(doc);

        var e1 = await WaitForAsync(sub1);
        var e2 = await WaitForAsync(sub2);
        Assert.Equal(e1.DocumentId, e2.DocumentId);

        await sub1.DisposeAsync();
        await _orders.InsertOneAsync(new BsonDocument { { "status", "second" } });
        var e3 = await WaitForAsync(sub2);
        Assert.NotEqual(e1.DocumentId, e3.DocumentId);
        await AssertNoEventAsync(sub1, TimeSpan.FromSeconds(1));
    }

    [Fact]
    public async Task SeparateCollections_AreIsolated()
    {
        var customers = _fixture.Client.GetDatabase(_db).GetCollection<BsonDocument>("customers");

        await using var ordersSub = await SubscribeAsync(_source, "orders");
        await using var customersSub = await SubscribeAsync(_source, "customers");

        await customers.InsertOneAsync(new BsonDocument { { "name", "alice" } });

        var e = await WaitForAsync(customersSub);
        Assert.Equal("customers", e.Source);
        await AssertNoEventAsync(ordersSub, TimeSpan.FromSeconds(1));
    }

    [Fact]
    public async Task ResumeFromToken_ContinuesAfterResumedPoint_WithoutReplay()
    {
        var docA = new BsonDocument { { "status", "a" } };
        await _orders.InsertOneAsync(docA);

        var first = await SubscribeAsync(_source, "orders");
        await _orders.InsertOneAsync(new BsonDocument { { "status", "b" } });
        var eventA = await WaitForAsync(first);
        await first.DisposeAsync();

        await using var second = await SubscribeAsync(_source, "orders", eventA.Token);
        await _orders.InsertOneAsync(new BsonDocument { { "status", "c" } });

        var eventB = await WaitForAsync(second);
        Assert.Equal(ChangeKind.Insert, eventB.Kind);
        Assert.NotEqual(eventA.DocumentId, eventB.DocumentId);
        await AssertNoEventAsync(second, TimeSpan.FromSeconds(1));
    }

    [Fact]
    public async Task ResumeToken_FromAnotherProvider_IsRejected()
    {
        var foreign = new ResumeToken("mongo:other.orders", new byte[] { 1, 2, 3 });
        var source = new MongoChangeSource(_fixture.Client.GetDatabase(_db));

        var ex = await Assert.ThrowsAsync<ResumeTokenInvalidException>(
            () => source.WatchAsync("orders", _ => Task.CompletedTask, foreign, CancellationToken.None));

        Assert.Contains("Refusing to misinterpret", ex.Message);
    }

    [Fact]
    public async Task GarbageResumeToken_IsReportedAsResumeTokenInvalid()
    {
        var opaque = new BsonDocument("_data", "zzzz").ToBson();
        var token = new ResumeToken($"mongo:{_db}.orders", opaque);

        await Assert.ThrowsAsync<ResumeTokenInvalidException>(
            () => _source.WatchAsync("orders", _ => Task.CompletedTask, token, CancellationToken.None));
    }

    [Fact]
    public async Task GetSnapshotAsync_ReturnsMatchingDocuments_WithAsOfToken()
    {
        await _orders.InsertOneAsync(new BsonDocument
        {
            { "_id", "a" }, { "status", "pending" }, { "total", 50 },
        });
        await _orders.InsertOneAsync(new BsonDocument
        {
            { "_id", "b" }, { "status", "shipped" }, { "total", 200 },
        });
        await _orders.InsertOneAsync(new BsonDocument
        {
            { "_id", "c" }, { "status", "pending" }, { "total", 150 },
        });

        var (documents, asOf) = await _source.GetSnapshotAsync(
            "orders",
            new SubscriptionFilter("orders", new FieldCompare("status", CompareOp.Eq, "pending")),
            CancellationToken.None);

        Assert.Equal(2, documents.Count);
        Assert.Contains(documents, d => (string)d["_id"]! == "a");
        Assert.Contains(documents, d => (string)d["_id"]! == "c");
        Assert.All(documents, d => Assert.Equal("pending", d["status"]));
        Assert.Equal($"mongo:{_db}.orders", asOf.ProviderId);
        Assert.NotEmpty(asOf.Opaque);
    }

    [Fact]
    public async Task GetSnapshotAsync_NoFilter_ReturnsAllDocuments()
    {
        await _orders.InsertOneAsync(new BsonDocument { { "_id", "a" } });
        await _orders.InsertOneAsync(new BsonDocument { { "_id", "b" } });

        var (documents, _) = await _source.GetSnapshotAsync(
            "orders",
            new SubscriptionFilter("orders", null),
            CancellationToken.None);

        Assert.Equal(2, documents.Count);
    }

    [Fact]
    public async Task GetSnapshotAsync_SupportsNestedAndArithmeticFilters()
    {
        await _orders.InsertOneAsync(new BsonDocument
        {
            { "_id", "a" },
            { "customer", new BsonDocument { { "address", new BsonDocument { { "city", "berlin" } } } } },
            { "total", 150 },
        });
        await _orders.InsertOneAsync(new BsonDocument
        {
            { "_id", "b" },
            { "customer", new BsonDocument { { "address", new BsonDocument { { "city", "paris" } } } } },
            { "total", 500 },
        });

        var where = new And(new FilterExpr[]
        {
            new FieldCompare("customer.address.city", CompareOp.Eq, "berlin"),
            new FieldCompare("total", CompareOp.Gte, 100),
        });

        var (documents, _) = await _source.GetSnapshotAsync(
            "orders",
            new SubscriptionFilter("orders", where),
            CancellationToken.None);

        var doc = Assert.Single(documents);
        Assert.Equal("a", doc["_id"]);
    }

    [Fact]
    public async Task GetSnapshotAsync_AsOfToken_ResumesWithNoReplayOrGap()
    {
        await _orders.InsertOneAsync(new BsonDocument { { "_id", "a" } });

        var (_, asOf) = await _source.GetSnapshotAsync(
            "orders",
            new SubscriptionFilter("orders", null),
            CancellationToken.None);

        // The as-of token must resume the change stream right after the snapshot point:
        // a change made before the snapshot must NOT arrive, a change after it must.
        await using var sub = await SubscribeAsync(_source, "orders", asOf);
        await _orders.InsertOneAsync(new BsonDocument { { "_id", "b" } });

        var e = await WaitForAsync(sub);
        Assert.Equal("b", e.DocumentId);
        await AssertNoEventAsync(sub, TimeSpan.FromSeconds(1));
    }

    private async Task<Subscription> SubscribeAsync(
        IChangeSource source,
        string collection,
        ResumeToken? resumeFrom = null)
    {
        var channel = Channel.CreateUnbounded<ChangeEvent>();
        var handle = await source.WatchAsync(
            collection,
            e => channel.Writer.WriteAsync(e).AsTask(),
            resumeFrom,
            CancellationToken.None);
        return new Subscription(handle, channel);
    }

    private static async Task<ChangeEvent> WaitForAsync(Subscription sub, TimeSpan? timeout = null)
    {
        using var cts = new CancellationTokenSource(timeout ?? TimeSpan.FromSeconds(15));
        return await sub.Channel.Reader.ReadAsync(cts.Token);
    }

    private static async Task AssertNoEventAsync(Subscription sub, TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => sub.Channel.Reader.ReadAsync(cts.Token).AsTask());
    }

    private sealed class Subscription : IAsyncDisposable
    {
        private readonly IAsyncDisposable _handle;
        public Channel<ChangeEvent> Channel { get; }

        public Subscription(IAsyncDisposable handle, Channel<ChangeEvent> channel)
        {
            _handle = handle;
            Channel = channel;
        }

        public ValueTask DisposeAsync() => _handle.DisposeAsync();
    }
}
