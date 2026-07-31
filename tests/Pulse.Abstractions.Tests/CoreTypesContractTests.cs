namespace Pulse.Abstractions.Tests;

public class CoreTypesContractTests
{
    [Fact]
    public void ResumeToken_IsProviderIdAndOpaqueBytes()
    {
        var token = new ResumeToken("mongo:orders-db", new byte[] { 1, 2, 3 });
        Assert.Equal("mongo:orders-db", token.ProviderId);
        Assert.Equal(new byte[] { 1, 2, 3 }, token.Opaque);
    }

    [Fact]
    public void ChangeEvent_HoldsAllFields()
    {
        var token = new ResumeToken("mongo:orders-db", new byte[] { 9 });
        var change = new ChangeEvent(
            Source: "orders",
            Kind: ChangeKind.Insert,
            DocumentId: "507f1f77bcf86cd799439011",
            FullDocument: new Dictionary<string, object?> { ["status"] = "pending" },
            UpdatedFields: null,
            Token: token,
            Timestamp: new DateTimeOffset(2026, 7, 31, 12, 0, 0, TimeSpan.Zero));

        Assert.Equal("orders", change.Source);
        Assert.Equal(ChangeKind.Insert, change.Kind);
        Assert.Equal("507f1f77bcf86cd799439011", change.DocumentId);
        Assert.Equal("pending", change.FullDocument!["status"]);
        Assert.Null(change.UpdatedFields);
        Assert.Same(token, change.Token);
        Assert.Equal(new DateTimeOffset(2026, 7, 31, 12, 0, 0, TimeSpan.Zero), change.Timestamp);
    }

    [Fact]
    public void ResumeTokenInvalidException_IsThrowable()
    {
        Action action = () => throw new ResumeTokenInvalidException("oplog rolled off");
        var ex = Assert.Throws<ResumeTokenInvalidException>(action);
        Assert.Equal("oplog rolled off", ex.Message);
    }

    private sealed class FakeChangeSource : IChangeSource
    {
        public Task<IAsyncDisposable> WatchAsync(
            string source,
            Func<ChangeEvent, Task> onChange,
            ResumeToken? resumeFrom,
            CancellationToken cancellationToken)
        {
            return Task.FromResult<IAsyncDisposable>(new NoopHandle());
        }

        public Task<(IReadOnlyList<IReadOnlyDictionary<string, object?>> Documents, ResumeToken AsOf)>
            GetSnapshotAsync(string source, SubscriptionFilter filter, CancellationToken cancellationToken)
        {
            return Task.FromResult<(IReadOnlyList<IReadOnlyDictionary<string, object?>>, ResumeToken)>(
                (new IReadOnlyDictionary<string, object?>[0], new ResumeToken("mongo:orders-db", new byte[0])));
        }

        private sealed class NoopHandle : IAsyncDisposable
        {
            public ValueTask DisposeAsync() => ValueTask.CompletedTask;
        }
    }

    [Fact]
    public async Task IChangeSource_ContractCompilesAndRuns()
    {
        var source = new FakeChangeSource();
        using var cts = new CancellationTokenSource();

        var (documents, asOf) = await source
            .GetSnapshotAsync("orders", new SubscriptionFilter("orders", null), cts.Token);

        Assert.Empty(documents);
        Assert.Equal("mongo:orders-db", asOf.ProviderId);
    }

    [Fact]
    public async Task IChangeSource_WatchAsyncReturnsDisposable()
    {
        var source = new FakeChangeSource();
        var handle = await source
            .WatchAsync("orders", _ => Task.CompletedTask, null, CancellationToken.None);

        Assert.IsAssignableFrom<IAsyncDisposable>(handle);
    }
}
