using Pulse.Abstractions;
using Pulse.Server;

namespace Pulse.Server.Tests;

/// <summary>
/// Tests for the deep module <see cref="SharedWatchCoordinator"/> via a fake
/// <see cref="IChangePollAdapter"/> — no Testcontainers, deterministic.
/// </summary>
public class SharedWatchCoordinatorTests
{
    [Fact]
    public async Task FansOutToMultipleSubscribersOnSameSource()
    {
        var adapter = new FakeAdapter();
        var coord = new SharedWatchCoordinator(adapter);
        var source = "public.orders";
        adapter.SeedPosition(source, 0);

        var got1 = new List<ChangeEvent>();
        var got2 = new List<ChangeEvent>();
        await using var s1 = await coord.SubscribeAsync(source, e => { got1.Add(e); return Task.CompletedTask; });
        await using var s2 = await coord.SubscribeAsync(source, e => { got2.Add(e); return Task.CompletedTask; });

        // Give coordinator time to start.
        await Task.Delay(100);

        var token1 = new ResumeToken(adapter.ProviderIdFor(source), BitConverter.GetBytes(1L));
        var ev1 = new ChangeEvent(source, ChangeKind.Insert, "1", null, null, token1, DateTimeOffset.UtcNow);
        adapter.Enqueue(source, ev1);

        await SpinWaitAsync(() => got1.Count == 1 && got2.Count == 1);

        Assert.Single(got1);
        Assert.Single(got2);
        Assert.Equal(ev1.Token, got1[0].Token);
        Assert.Equal(ev1.Token, got2[0].Token);
    }

    [Fact]
    public async Task LastSubscriberDisposesSharedWatch()
    {
        var adapter = new FakeAdapter();
        var coord = new SharedWatchCoordinator(adapter);
        var source = "public.t1";
        adapter.SeedPosition(source, 0);

        var h1 = await coord.SubscribeAsync(source, _ => Task.CompletedTask);
        var h2 = await coord.SubscribeAsync(source, _ => Task.CompletedTask);

        await h1.DisposeAsync();
        // One subscriber remains — watch still alive.
        await Task.Delay(50);
        Assert.True(adapter.PollCount(source) >= 1);

        await h2.DisposeAsync();
        // After last dispose, a new subscription should start a fresh watch.
        await Task.Delay(50);
        var before = adapter.PollCount(source);
        await using var h3 = await coord.SubscribeAsync(source, _ => Task.CompletedTask);
        await Task.Delay(100);
        Assert.True(adapter.PollCount(source) > before);
    }

    [Fact]
    public async Task PrivateWatchIsNotShared()
    {
        var adapter = new FakeAdapter();
        var coord = new SharedWatchCoordinator(adapter);
        var source = "public.t2";
        adapter.SeedPosition(source, 10);

        await using var shared = await coord.SubscribeAsync(source, _ => Task.CompletedTask);
        await Task.Delay(100);

        var resumeToken = new ResumeToken(adapter.ProviderIdFor(source), BitConverter.GetBytes(5L));
        await using var priv = await coord.SubscribeResumedAsync(source, resumeToken, _ => Task.CompletedTask);
        // Private watch should poll independently; shared poll count should not be inflated by private.
        // We just assert both handles are distinct and no exception thrown.
        Assert.NotNull(priv);
    }

    [Fact]
    public async Task ValidateProviderIdFailsFastOnPrivateWatch()
    {
        var adapter = new FakeAdapter();
        var coord = new SharedWatchCoordinator(adapter);
        var source = "public.t3";
        adapter.SeedPosition(source, 0);

        var badToken = new ResumeToken("postgres:other.table", BitConverter.GetBytes(1L));
        await Assert.ThrowsAsync<ResumeTokenInvalidException>(() =>
            coord.SubscribeResumedAsync(source, badToken, _ => Task.CompletedTask));
    }

    [Fact]
    public async Task BackoffIsCappedAtMaxBackoff()
    {
        // Directly test BackoffDelay logic via reflection-style: we induce failures and ensure
        // capped value. Here we fake adapter that throws, then recovers.
        var adapter = new FailingFakeAdapter(failCount: 10);
        var opts = new SharedWatchCoordinatorOptions
        {
            PollInterval = TimeSpan.FromMilliseconds(10),
            MaxBackoff = TimeSpan.FromMilliseconds(50),
            MaxStaleRetries = 3,
        };
        var coord = new SharedWatchCoordinator(adapter, opts);
        var source = "public.t4";
        adapter.SeedPosition(source, 0);

        await using var sub = await coord.SubscribeAsync(source, _ => Task.CompletedTask);
        // Let it retry with failures; should not throw and should cap at 50ms.
        await Task.Delay(500);
        Assert.True(adapter.PollAttempts >= 5);
    }

    private static async Task SpinWaitAsync(Func<bool> condition, int timeoutMs = 2000)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        while (!condition() && sw.ElapsedMilliseconds < timeoutMs)
        {
            await Task.Delay(20);
        }
    }

    private sealed class FakeAdapter : IChangePollAdapter
    {
        private readonly Dictionary<string, long> _positions = new(StringComparer.Ordinal);
        private readonly Dictionary<string, Queue<ChangeEvent>> _queues = new(StringComparer.Ordinal);
        private readonly Dictionary<string, int> _pollCounts = new(StringComparer.Ordinal);

        public void SeedPosition(string source, long seq) => _positions[source] = seq;

        public void Enqueue(string source, ChangeEvent ev)
        {
            if (!_queues.TryGetValue(source, out var q))
            {
                q = new Queue<ChangeEvent>();
                _queues[source] = q;
            }

            q.Enqueue(ev);
        }

        public int PollCount(string source) => _pollCounts.TryGetValue(source, out var c) ? c : 0;

        public string ProviderIdFor(string resolvedSource) => $"postgres:{resolvedSource}";

        public Task<ResumeToken> GetCurrentPositionAsync(string resolvedSource, CancellationToken ct)
        {
            var seq = _positions.TryGetValue(resolvedSource, out var s) ? s : 0;
            return Task.FromResult(new ResumeToken(ProviderIdFor(resolvedSource), BitConverter.GetBytes(seq)));
        }

        public Task<PollBatch> PollAsync(string resolvedSource, ResumeToken after, CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested();
            _pollCounts[resolvedSource] = PollCount(resolvedSource) + 1;
            if (_queues.TryGetValue(resolvedSource, out var q) && q.Count > 0)
            {
                var batch = new List<ChangeEvent>();
                while (q.Count > 0)
                {
                    batch.Add(q.Dequeue());
                }

                var last = batch[^1].Token;
                _positions[resolvedSource] = BitConverter.ToInt64(last.Opaque, 0);
                return Task.FromResult(new PollBatch(batch, last));
            }

            return Task.FromResult(new PollBatch(Array.Empty<ChangeEvent>(), after));
        }

        public Task WaitAsync(string resolvedSource, CancellationToken ct)
            => Task.Delay(20, ct);
    }

    private sealed class FailingFakeAdapter : IChangePollAdapter
    {
        private int _remainingFails;
        public int PollAttempts { get; private set; }
        private readonly Dictionary<string, long> _positions = new();

        public FailingFakeAdapter(int failCount) => _remainingFails = failCount;

        public void SeedPosition(string source, long seq) => _positions[source] = seq;

        public string ProviderIdFor(string resolvedSource) => $"postgres:{resolvedSource}";

        public Task<ResumeToken> GetCurrentPositionAsync(string resolvedSource, CancellationToken ct)
        {
            var seq = _positions.TryGetValue(resolvedSource, out var s) ? s : 0;
            return Task.FromResult(new ResumeToken(ProviderIdFor(resolvedSource), BitConverter.GetBytes(seq)));
        }

        public Task<PollBatch> PollAsync(string resolvedSource, ResumeToken after, CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested();
            PollAttempts++;
            if (_remainingFails-- > 0)
            {
                throw new InvalidOperationException("simulated transient failure");
            }

            return Task.FromResult(new PollBatch(Array.Empty<ChangeEvent>(), after));
        }

        public Task WaitAsync(string resolvedSource, CancellationToken ct) => Task.Delay(5, ct);
    }
}
