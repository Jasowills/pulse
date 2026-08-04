using MongoDB.Bson;
using MongoDB.Driver;

namespace Pulse.TestApp.Core;

/// <summary>
/// Direct Mongo access. Uses BSON ObjectIds as <c>_id</c> (the natural Mongo primary key),
/// surfaced to Pulse as their hex string — the case the spec's "ObjectId ... surfaced as
/// string" line is about, and the case that exercises Pulse's id handling hardest.
/// </summary>
public sealed class MongoOrderStore : IOrderStore
{
    private readonly IMongoDatabase _database;
    private readonly IMongoCollection<BsonDocument> _orders;

    public MongoOrderStore(string uri, string database)
    {
        var client = new MongoClient(MongoClientSettings.FromConnectionString(uri));
        _database = client.GetDatabase(database);
        _orders = _database.GetCollection<BsonDocument>("orders");
    }

    public ProviderKind Provider => ProviderKind.Mongo;

    public async Task InitializeAsync(CancellationToken ct = default)
    {
        await _orders.Indexes.CreateOneAsync(
            new CreateIndexModel<BsonDocument>(
                Builders<BsonDocument>.IndexKeys.Ascending("Status").Ascending("Region")),
            cancellationToken: ct);
        await _orders.Indexes.CreateOneAsync(
            new CreateIndexModel<BsonDocument>(
                Builders<BsonDocument>.IndexKeys.Ascending("UpdatedAt")),
            cancellationToken: ct);
    }

    public async Task SeedAsync(int count, CancellationToken ct = default)
    {
        await _orders.DeleteManyAsync(FilterDefinition<BsonDocument>.Empty, ct);
        var rng = new Random(20260803);
        var docs = new List<BsonDocument>(count);
        for (var i = 0; i < count; i++)
        {
            docs.Add(NewOrderDocument(rng, i));
        }

        if (docs.Count > 0)
        {
            await _orders.InsertManyAsync(docs, cancellationToken: ct);
        }
    }

    public async Task UpdateStatusAsync(string id, string status, CancellationToken ct = default)
    {
        var update = Builders<BsonDocument>.Update
            .Set("Status", status)
            .Set("UpdatedAt", DateTime.UtcNow);
        await _orders.UpdateOneAsync(
            Builders<BsonDocument>.Filter.Eq("_id", new ObjectId(id)),
            update,
            cancellationToken: ct);
    }

    public async Task UpdateRegionAsync(string id, string region, CancellationToken ct = default)
    {
        var update = Builders<BsonDocument>.Update
            .Set("Region", region)
            .Set("UpdatedAt", DateTime.UtcNow);
        await _orders.UpdateOneAsync(
            Builders<BsonDocument>.Filter.Eq("_id", new ObjectId(id)),
            update,
            cancellationToken: ct);
    }

    public async Task<Order?> GetByIdAsync(string id, CancellationToken ct = default)
    {
        var doc = await _orders.Find(Builders<BsonDocument>.Filter.Eq("_id", new ObjectId(id)))
            .FirstOrDefaultAsync(ct);
        return doc is null ? null : ToOrder(doc);
    }

    public async Task<IReadOnlyList<Order>> GetAllAsync(CancellationToken ct = default)
    {
        using var cursor = await _orders.Find(FilterDefinition<BsonDocument>.Empty)
            .ToCursorAsync(ct);
        var list = new List<Order>();
        while (await cursor.MoveNextAsync(ct))
        {
            foreach (var doc in cursor.Current)
            {
                list.Add(ToOrder(doc));
            }
        }

        return list;
    }

    public async Task<int> BulkMutateAsync(int count, int intervalMs, Random rng, CancellationToken ct = default)
    {
        var all = await GetAllAsync(ct);
        if (all.Count == 0)
        {
            return 0;
        }

        var applied = 0;
        for (var i = 0; i < count; i++)
        {
            ct.ThrowIfCancellationRequested();
            var target = all[rng.Next(all.Count)];
            var newStatus = OrderState.Statuses[rng.Next(OrderState.Statuses.Length)];
            await UpdateStatusAsync(target.Id, newStatus, ct);
            applied++;
            if (intervalMs > 0)
            {
                await Task.Delay(intervalMs, ct);
            }
        }

        return applied;
    }

    public async Task<SetupCheck> VerifySetupAsync(CancellationToken ct = default)
    {
        var requirements = new List<SetupRequirement>();
        try
        {
            var hello = await _database.RunCommandAsync<BsonDocument>(
                new BsonDocument("hello", 1), cancellationToken: ct);
            var setName = hello.GetValue("setName", BsonNull.Value) is BsonString s ? s.Value : null;
            requirements.Add(new SetupRequirement(
                "Replica set",
                !string.IsNullOrEmpty(setName),
                setName is null
                    ? "Standalone server — change streams require a replica set (even single-node)."
                    : $"Member of replica set '{setName}'."));
        }
        catch (Exception ex)
        {
            requirements.Add(new SetupRequirement("Replica set", false, ex.Message));
        }

        try
        {
            await _orders.Find(FilterDefinition<BsonDocument>.Empty).FirstOrDefaultAsync(ct);
            requirements.Add(new SetupRequirement("Orders collection", true, "Readable."));
        }
        catch (Exception ex)
        {
            requirements.Add(new SetupRequirement("Orders collection", false, ex.Message));
        }

        return new SetupCheck(requirements);
    }

    private static BsonDocument NewOrderDocument(Random rng, int index)
    {
        var now = DateTime.UtcNow;
        return new BsonDocument
        {
            { "_id", ObjectId.GenerateNewId() },
            { "CustomerName", $"Customer {rng.Next(1, 500)}" },
            { "Status", OrderState.Statuses[rng.Next(OrderState.Statuses.Length)] },
            { "Total", rng.Next(1, 2000) + (rng.Next(1, 100) / 100m) },
            { "Items", rng.Next(1, 20) },
            { "CreatedAt", now.AddMinutes(-index) },
            { "UpdatedAt", now },
            { "Region", OrderState.Regions[rng.Next(OrderState.Regions.Length)] },
        };
    }

    private static Order ToOrder(BsonDocument doc)
        => new()
        {
            Id = doc["_id"] is BsonObjectId oid ? oid.ToString() : doc["_id"].AsString,
            CustomerName = doc.GetValue("CustomerName", string.Empty).AsString,
            Status = doc.GetValue("Status", string.Empty).AsString,
            Total = (decimal)doc.GetValue("Total", 0).ToDouble(),
            Items = doc.GetValue("Items", 0).AsInt32,
            CreatedAt = doc.GetValue("CreatedAt", DateTime.UtcNow).ToUniversalTime(),
            UpdatedAt = doc.GetValue("UpdatedAt", DateTime.UtcNow).ToUniversalTime(),
            Region = doc.GetValue("Region", string.Empty).AsString,
        };
}