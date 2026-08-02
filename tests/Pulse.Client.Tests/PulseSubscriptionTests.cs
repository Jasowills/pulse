using System.Text.Json;
using System.Threading.Channels;
using Pulse.Abstractions;

namespace Pulse.Client.Tests;

public sealed class Order
{
    public string _id { get; set; } = "";
    public string status { get; set; } = "";
    public long total { get; set; }
}

public class PulseSubscriptionTests
{
    private static readonly JsonSerializerOptions Options = new() { PropertyNameCaseInsensitive = true };

    [Fact]
    public async Task Snapshot_PopulatesCurrent_AndRaisesOnSnapshot()
    {
        var sub = new PulseSubscription<Order>("sub-1", "orders", Options, _ => Task.CompletedTask);
        var snapshot = new TaskCompletionSource<IReadOnlyList<Order>>();
        sub.OnSnapshot += docs => snapshot.TrySetResult(docs);
        try
        {
            sub.Enqueue(new PulseSnapshotMessage("sub-1", new IReadOnlyDictionary<string, object?>[]
            {
                Doc("a", "pending", 50),
                Doc("b", "shipped", 200),
            }));

            var docs = await snapshot.Task.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.Equal(2, docs.Count);
            Assert.Equal(2, sub.Current.Count);
            Assert.Equal("a", sub.Current.Single(d => d._id == "a")._id);
        }
        finally
        {
            sub.Close();
        }
    }

    [Fact]
    public async Task Snapshot_ReplacesCurrent_NotMerge()
    {
        var sub = new PulseSubscription<Order>("sub-1", "orders", Options, _ => Task.CompletedTask);
        var snapshots = Channel.CreateUnbounded<IReadOnlyList<Order>>();
        sub.OnSnapshot += docs => snapshots.Writer.TryWrite(docs);
        try
        {
            sub.Enqueue(new PulseSnapshotMessage("sub-1", new IReadOnlyDictionary<string, object?>[]
            {
                Doc("a", "pending", 50),
                Doc("b", "shipped", 200),
            }));
            await snapshots.Reader.ReadAsync(new CancellationTokenSource(TimeSpan.FromSeconds(5)).Token);

            sub.Enqueue(new PulseSnapshotMessage("sub-1", new IReadOnlyDictionary<string, object?>[]
            {
                Doc("c", "pending", 10),
            }));
            await snapshots.Reader.ReadAsync(new CancellationTokenSource(TimeSpan.FromSeconds(5)).Token);

            Assert.Single(sub.Current);
            Assert.Equal("c", sub.Current[0]._id);
        }
        finally
        {
            sub.Close();
        }
    }

    [Fact]
    public async Task Changes_UpdateCurrent_AndRaiseOnChange()
    {
        var sub = new PulseSubscription<Order>("sub-1", "orders", Options, _ => Task.CompletedTask);
        var changes = Channel.CreateUnbounded<PulseChange<Order>>();
        sub.OnChange += change => changes.Writer.TryWrite(change);
        try
        {
            sub.Enqueue(new PulseSnapshotMessage("sub-1", new IReadOnlyDictionary<string, object?>[]
            {
                Doc("a", "pending", 50),
                Doc("b", "shipped", 200),
            }));
            await sub.WaitForIdleAsync();

            sub.Enqueue(Change("sub-1", ChangeKind.Insert, "c", Doc("c", "pending", 99), null));
            var insert = await ReadAsync(changes);
            Assert.Equal(ChangeKind.Insert, insert.Kind);
            Assert.Equal("c", insert.DocumentId);
            Assert.Equal(99, insert.Document!.total);
            Assert.Equal(3, sub.Current.Count);

            sub.Enqueue(Change("sub-1", ChangeKind.Update, "a", Doc("a", "pending", 150), new Dictionary<string, object?> { ["total"] = 150L }));
            var update = await ReadAsync(changes);
            Assert.Equal(ChangeKind.Update, update.Kind);
            Assert.Equal(150L, update.UpdatedFields!["total"]);
            Assert.Equal(150, sub.Current.Single(d => d._id == "a").total);

            sub.Enqueue(Change("sub-1", ChangeKind.Delete, "b", null, null));
            var delete = await ReadAsync(changes);
            Assert.Equal(ChangeKind.Delete, delete.Kind);
            Assert.Null(delete.Document);
            Assert.DoesNotContain(sub.Current, d => d._id == "b");
            Assert.Equal(2, sub.Current.Count);
        }
        finally
        {
            sub.Close();
        }
    }

    [Fact]
    public void FilterExpr_SerializesToWireFormat_AndRoundTrips()
    {
        FilterExpr[] filters =
        {
            new FieldCompare("status", CompareOp.Eq, "pending"),
            new FieldCompare("total", CompareOp.Gte, 100L),
            new And(new FilterExpr[]
            {
                new FieldCompare("status", CompareOp.Eq, "pending"),
                new Or(new FilterExpr[] { new FieldCompare("total", CompareOp.Gt, 100L), new FieldCompare("total", CompareOp.Lt, 200L) }),
            }),
            new Not(new FieldCompare("archived", CompareOp.Exists, true)),
        };

        foreach (var filter in filters)
        {
            var json = JsonSerializer.Serialize(filter);
            var back = JsonSerializer.Deserialize<FilterExpr>(json);
            Assert.Equal(filter, back);
        }

        var compareJson = JsonSerializer.Serialize(new FieldCompare("status", CompareOp.Eq, "pending"));
        Assert.Equal("""{"field":"status","op":"eq","value":"pending"}""", compareJson);
    }

    private static async Task<PulseChange<Order>> ReadAsync(Channel<PulseChange<Order>> channel)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        return await channel.Reader.ReadAsync(cts.Token);
    }

    private static IReadOnlyDictionary<string, object?> Doc(string id, string status, long total)
        => new Dictionary<string, object?>
        {
            ["_id"] = id,
            ["status"] = status,
            ["total"] = total,
        };

    private static PulseChangeMessage Change(
        string subscriptionId,
        ChangeKind kind,
        string documentId,
        IReadOnlyDictionary<string, object?>? document,
        IReadOnlyDictionary<string, object?>? updatedFields)
        => new(subscriptionId, kind, documentId, document, updatedFields, DateTimeOffset.UtcNow);
}
