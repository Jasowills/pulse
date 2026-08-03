using System.Threading.Channels;
using Npgsql;
using Pulse.Abstractions;
using Testcontainers.PostgreSql;

namespace Pulse.Postgres.Tests;

public sealed class PostgresContainerFixture : IAsyncLifetime
{
    public NpgsqlDataSource DataSource { get; private set; } = null!;

    private PostgreSqlContainer? _container;

    public async Task InitializeAsync()
    {
        var container = new PostgreSqlBuilder("postgres:16")
            .Build();
        await container.StartAsync();
        _container = container;
        DataSource = NpgsqlDataSource.Create(container.GetConnectionString());
    }

    public Task DisposeAsync()
    {
        DataSource.Dispose();
        return (_container?.DisposeAsync() ?? ValueTask.CompletedTask).AsTask();
    }
}

public sealed class PostgresChangeSourceTests : IClassFixture<PostgresContainerFixture>, IAsyncLifetime
{
    private readonly PostgresContainerFixture _fixture;
    private NpgsqlConnection _conn = null!;
    private string _table = null!;
    private IChangeSource _source = null!;

    public PostgresChangeSourceTests(PostgresContainerFixture fixture)
    {
        _fixture = fixture;
    }

    public async Task InitializeAsync()
    {
        _conn = await _fixture.DataSource.OpenConnectionAsync();
        _table = "orders_" + Guid.NewGuid().ToString("N");
        _source = new PostgresChangeSource(_fixture.DataSource, pollInterval: TimeSpan.FromMilliseconds(50));
        await CreateOrdersTableAsync(_table);
    }

    public async Task DisposeAsync()
        => await _conn.DisposeAsync();

    [Fact]
    public async Task Insert_PublishesChangeEvent_WithFullDocument()
    {
        await using var sub = await SubscribeAsync(_source, _table);

        var before = DateTimeOffset.UtcNow;
        await InsertAsync(_table, "a", "pending", 42);

        var e = await WaitForAsync(sub);

        Assert.Equal(_table, e.Source);
        Assert.Equal(ChangeKind.Insert, e.Kind);
        Assert.Equal("a", e.DocumentId);
        Assert.NotNull(e.FullDocument);
        Assert.Equal("a", e.FullDocument["_id"]);
        Assert.Equal("pending", e.FullDocument["status"]);
        Assert.Equal(42L, e.FullDocument["total"]);
        Assert.Null(e.UpdatedFields);
        Assert.Equal($"postgres:public.{_table}", e.Token.ProviderId);
        Assert.Equal(8, e.Token.Opaque.Length);
        Assert.InRange(e.Timestamp, before.AddSeconds(-5), DateTimeOffset.UtcNow.AddSeconds(5));
    }

    [Fact]
    public async Task Update_PublishesUpdatedFields_AndFullDocument()
    {
        await InsertAsync(_table, "a", "pending", 42);

        await using var sub = await SubscribeAsync(_source, _table);

        await UpdateAsync(_table, "a", status: "shipped");

        var e = await WaitForAsync(sub);

        Assert.Equal(ChangeKind.Update, e.Kind);
        Assert.Equal("a", e.DocumentId);
        Assert.NotNull(e.UpdatedFields);
        Assert.Equal("shipped", e.UpdatedFields["status"]);
        Assert.DoesNotContain("total", e.UpdatedFields.Keys);
        Assert.NotNull(e.FullDocument);
        Assert.Equal("shipped", e.FullDocument["status"]);
        Assert.Equal(42L, e.FullDocument["total"]);
    }

    [Fact]
    public async Task Delete_PublishesDelete_WithoutFullDocument()
    {
        await InsertAsync(_table, "a", "pending");

        await using var sub = await SubscribeAsync(_source, _table);

        await DeleteAsync(_table, "a");

        var e = await WaitForAsync(sub);

        Assert.Equal(ChangeKind.Delete, e.Kind);
        Assert.Equal("a", e.DocumentId);
        Assert.Null(e.FullDocument);
        Assert.Null(e.UpdatedFields);
    }

    [Fact]
    public async Task SharedWatch_FansOutToAllSubscribers_AndSurvivesOneDisposal()
    {
        await using var sub1 = await SubscribeAsync(_source, _table);
        await using var sub2 = await SubscribeAsync(_source, _table);

        await InsertAsync(_table, "a", "pending");

        var e1 = await WaitForAsync(sub1);
        var e2 = await WaitForAsync(sub2);
        Assert.Equal(e1.DocumentId, e2.DocumentId);

        await sub1.DisposeAsync();
        await InsertAsync(_table, "b", "second");
        var e3 = await WaitForAsync(sub2);
        Assert.NotEqual(e1.DocumentId, e3.DocumentId);
        await AssertNoEventAsync(sub1, TimeSpan.FromSeconds(1));
    }

    [Fact]
    public async Task SeparateTables_AreIsolated()
    {
        var other = _table + "_b";
        await CreateOrdersTableAsync(other);

        await using var ordersSub = await SubscribeAsync(_source, _table);
        await using var otherSub = await SubscribeAsync(_source, other);

        await InsertAsync(other, "x", "pending");

        var e = await WaitForAsync(otherSub);
        Assert.Equal(other, e.Source);
        await AssertNoEventAsync(ordersSub, TimeSpan.FromSeconds(1));
    }

    [Fact]
    public async Task ResumeFromToken_ContinuesAfterResumedPoint_WithoutReplay()
    {
        await InsertAsync(_table, "a", "pending");

        var first = await SubscribeAsync(_source, _table);
        await InsertAsync(_table, "b", "pending");
        var eventA = await WaitForAsync(first);
        await first.DisposeAsync();

        await using var second = await SubscribeAsync(_source, _table, eventA.Token);
        await InsertAsync(_table, "c", "pending");

        var eventB = await WaitForAsync(second);
        Assert.Equal(ChangeKind.Insert, eventB.Kind);
        Assert.Equal("c", eventB.DocumentId);
        await AssertNoEventAsync(second, TimeSpan.FromSeconds(1));
    }

    [Fact]
    public async Task ResumeToken_FromAnotherProvider_IsRejected()
    {
        var foreign = new ResumeToken("mongo:orders", new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 });

        var ex = await Assert.ThrowsAsync<ResumeTokenInvalidException>(
            () => _source.WatchAsync(_table, _ => Task.CompletedTask, foreign, CancellationToken.None));

        Assert.Contains("Refusing to misinterpret", ex.Message);
    }

    [Fact]
    public async Task GarbageResumeToken_IsReportedAsResumeTokenInvalid()
    {
        var token = new ResumeToken($"postgres:public.{_table}", new byte[] { 1, 2, 3 });

        await Assert.ThrowsAsync<ResumeTokenInvalidException>(
            () => _source.WatchAsync(_table, _ => Task.CompletedTask, token, CancellationToken.None));
    }

    [Fact]
    public async Task GetSnapshotAsync_ReturnsMatchingDocuments_WithAsOfToken()
    {
        await InsertAsync(_table, "a", "pending", 50);
        await InsertAsync(_table, "b", "shipped", 200);
        await InsertAsync(_table, "c", "pending", 150);

        var (documents, asOf) = await _source.GetSnapshotAsync(
            _table,
            new SubscriptionFilter(_table, new FieldCompare("status", CompareOp.Eq, "pending")),
            CancellationToken.None);

        Assert.Equal(2, documents.Count);
        Assert.Contains(documents, d => (string)d["_id"]! == "a");
        Assert.Contains(documents, d => (string)d["_id"]! == "c");
        Assert.All(documents, d => Assert.Equal("pending", d["status"]));
        Assert.Equal($"postgres:public.{_table}", asOf.ProviderId);
        Assert.Equal(8, asOf.Opaque.Length);
    }

    [Fact]
    public async Task GetSnapshotAsync_NoFilter_ReturnsAllDocuments()
    {
        await InsertAsync(_table, "a", "pending");
        await InsertAsync(_table, "b", "shipped");

        var (documents, _) = await _source.GetSnapshotAsync(
            _table,
            new SubscriptionFilter(_table, null),
            CancellationToken.None);

        Assert.Equal(2, documents.Count);
    }

    [Fact]
    public async Task GetSnapshotAsync_SupportsNestedAndArithmeticFilters()
    {
        await InsertAsync(_table, "a", "pending", 150, """{"address":{"city":"berlin"}}""");
        await InsertAsync(_table, "b", "pending", 500, """{"address":{"city":"paris"}}""");

        var where = new And(new FilterExpr[]
        {
            new FieldCompare("customer.address.city", CompareOp.Eq, "berlin"),
            new FieldCompare("total", CompareOp.Gte, 100),
        });

        var (documents, _) = await _source.GetSnapshotAsync(
            _table,
            new SubscriptionFilter(_table, where),
            CancellationToken.None);

        var doc = Assert.Single(documents);
        Assert.Equal("a", doc["_id"]);
    }

    [Fact]
    public async Task GetSnapshotAsync_AsOfToken_ResumesWithNoReplayOrGap()
    {
        await InsertAsync(_table, "a", "pending");

        var (_, asOf) = await _source.GetSnapshotAsync(
            _table,
            new SubscriptionFilter(_table, null),
            CancellationToken.None);

        await using var sub = await SubscribeAsync(_source, _table, asOf);
        await InsertAsync(_table, "b", "pending");

        var e = await WaitForAsync(sub);
        Assert.Equal("b", e.DocumentId);
        await AssertNoEventAsync(sub, TimeSpan.FromSeconds(1));
    }

    [Fact]
    public async Task CompositePrimaryKey_IsRejectedWithActionableError()
    {
        var composite = _table + "_comp";
        await CreateTableAsync($"CREATE TABLE \"{composite}\" (a int, b int, PRIMARY KEY (a, b))");

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => _source.WatchAsync(composite, _ => Task.CompletedTask, null, CancellationToken.None));

        Assert.Contains("composite primary key", ex.Message);
    }

    [Fact]
    public async Task NoPrimaryKey_IsRejectedWithActionableError()
    {
        var noPk = _table + "_nopk";
        await CreateTableAsync($"CREATE TABLE \"{noPk}\" (x int)");

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => _source.WatchAsync(noPk, _ => Task.CompletedTask, null, CancellationToken.None));

        Assert.Contains("no primary key", ex.Message);
    }

[Fact]
    public async Task MissingTable_IsRejectedWithActionableError()
    {
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => _source.WatchAsync(_table + "_missing", _ => Task.CompletedTask, null, CancellationToken.None));
        Assert.Contains("does not exist", ex.Message);
    }

    [Fact]
    public async Task Pruning_DeletesOldRows_BelowAnActiveWatcherFloor()
    {
        var source = new PostgresChangeSource(
            _fixture.DataSource,
            pollInterval: TimeSpan.FromMilliseconds(50),
            pruneInterval: TimeSpan.FromMilliseconds(50));
        await InstallTriggerAsync(source, _table);

        await InsertAsync(_table, "r1", "pending");
        await InsertAsync(_table, "r2", "pending");
        await InsertAsync(_table, "r3", "pending");

        await using var sub = await SubscribeAsync(source, _table);

        await Task.Delay(TimeSpan.FromMilliseconds(600));

        Assert.Equal(1L, await CountChangesAsync(_table));
    }

    [Fact]
    public async Task Pruning_SparesLogsWithNoActiveWatcher()
    {
        var source = new PostgresChangeSource(
            _fixture.DataSource,
            pollInterval: TimeSpan.FromMilliseconds(50),
            pruneInterval: TimeSpan.FromMilliseconds(50));
        await InstallTriggerAsync(source, _table);

        await InsertAsync(_table, "r1", "pending");

        await Task.Delay(TimeSpan.FromMilliseconds(600));

        Assert.Equal(1L, await CountChangesAsync(_table));
    }

    [Fact]
    public async Task Pruning_RespectsAResumedWatchFloor_ThenDeletesAfterItEnds()
    {
        var source = new PostgresChangeSource(
            _fixture.DataSource,
            pollInterval: TimeSpan.FromMilliseconds(50),
            pruneInterval: TimeSpan.FromMilliseconds(200));
        await InstallTriggerAsync(source, _table);

        await using var sharedSub = await SubscribeAsync(source, _table);

        await InsertAsync(_table, "r1", "pending");
        var r1Event = await WaitForAsync(sharedSub);

        await using var resumedSub = await SubscribeAsync(source, _table, r1Event.Token);

        await InsertAsync(_table, "r2", "pending");
        await InsertAsync(_table, "r3", "pending");
        await WaitForAsync(sharedSub);
        await WaitForAsync(sharedSub);

        await Task.Delay(TimeSpan.FromMilliseconds(500));
        Assert.Equal(3L, await CountChangesAsync(_table));

        await resumedSub.DisposeAsync();
        await Task.Delay(TimeSpan.FromMilliseconds(600));
        Assert.Equal(1L, await CountChangesAsync(_table));
    }

    private async Task InstallTriggerAsync(IChangeSource source, string table)
    {
        await using var setup = await SubscribeAsync(source, table);
        await setup.DisposeAsync();
    }

    private async Task<long> CountChangesAsync(string table)
    {
        await using var cmd = _conn.CreateCommand();
        cmd.CommandText = "SELECT count(*) FROM pulse._changes WHERE source = @source";
        cmd.Parameters.AddWithValue("source", $"public.{table}");
        var scalar = await cmd.ExecuteScalarAsync();
        return Convert.ToInt64(scalar);
    }

    private async Task CreateOrdersTableAsync(string table)
    {
        await CreateTableAsync($"""
            CREATE TABLE "{table}" (
                id text PRIMARY KEY,
                status text NOT NULL,
                total bigint,
                customer jsonb
            )
            """);
    }

    private async Task CreateTableAsync(string sql)
    {
        await using var cmd = _conn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task InsertAsync(string table, string id, string status, long? total = null, string? customerJson = null)
    {
        await using var cmd = _conn.CreateCommand();
        cmd.CommandText = $"""INSERT INTO "{table}" (id, status, total, customer) VALUES (@id, @status, @total, @customer::jsonb)""";
        cmd.Parameters.AddWithValue("id", id);
        cmd.Parameters.AddWithValue("status", status);
        cmd.Parameters.AddWithValue("total", (object?)total ?? DBNull.Value);
        cmd.Parameters.AddWithValue("customer", (object?)customerJson ?? DBNull.Value);
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task UpdateAsync(string table, string id, string? status = null, long? total = null)
    {
        var sets = new List<string>();
        if (status is not null)
        {
            sets.Add("status = @status");
        }

        if (total is not null)
        {
            sets.Add("total = @total");
        }

        await using var cmd = _conn.CreateCommand();
        cmd.CommandText = $"UPDATE \"{table}\" SET {string.Join(", ", sets)} WHERE id = @id";
        cmd.Parameters.AddWithValue("id", id);
        if (status is not null)
        {
            cmd.Parameters.AddWithValue("status", status);
        }

        if (total is not null)
        {
            cmd.Parameters.AddWithValue("total", total);
        }

        await cmd.ExecuteNonQueryAsync();
    }

    private async Task DeleteAsync(string table, string id)
    {
        await using var cmd = _conn.CreateCommand();
        cmd.CommandText = $"DELETE FROM \"{table}\" WHERE id = @id";
        cmd.Parameters.AddWithValue("id", id);
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task<Subscription> SubscribeAsync(
        IChangeSource source,
        string table,
        ResumeToken? resumeFrom = null)
    {
        var channel = Channel.CreateUnbounded<ChangeEvent>();
        var handle = await source.WatchAsync(
            table,
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
