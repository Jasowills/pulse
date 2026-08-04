using System.Diagnostics;
using System.Net.Http.Json;
using Pulse.Abstractions;
using Pulse.Client;
using Pulse.TestApp.Core;

namespace Pulse.TestApp.Harness;

/// <summary>One headless scenario result, rolled up into the /apps/TEST-REPORT.md table.</summary>
public sealed record ScenarioResult(string Id, string Name, bool Passed, string Notes);

/// <summary>Records a diagnostic state transition observed on the client connection.</summary>
public sealed record StateTransition(string From, string To, double AtSeconds);

/// <summary>
/// Drives Pulse.Client headlessly through the manual QA scenarios from the test-app
/// spec, against a freshly-seeded database via a real Pulse.TestApp.Server process.
/// Every assertion computes its expected value from a direct database read, so it is
/// valid regardless of the exact seed distribution.
/// </summary>
public sealed class Harness
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(30);

    private readonly ProviderKind _provider;
    private readonly IOrderStore _store;
    private readonly string _serverDll;
    private readonly HttpClient _http = new();
    private readonly List<ScenarioResult> _results = new();
    private readonly List<StateTransition> _transitions = new();
    private readonly Random _rng = new(DateTime.UtcNow.Millisecond | 1);

    private PulseClient? _client;
    private ServerProcess? _server;
    private CancellationToken _ct;
    private string _lastConnectionState = "";

    public Harness(ProviderKind provider, string serverDll)
    {
        _provider = provider;
        _store = OrderStoreFactory.Create(provider);
        _serverDll = serverDll;
    }

    public IReadOnlyList<ScenarioResult> Results => _results;

    public IReadOnlyList<StateTransition> Transitions => _transitions;

    public async Task RunAsync(CancellationToken ct)
    {
        _ct = ct;
        try
        {
            await RunCoreAsync(ct);
        }
        finally
        {
            if (_server is not null)
            {
                await _server.DisposeAsync();
                _server = null;
            }

            if (_client is not null)
            {
                await _client.DisposeAsync();
                _client = null;
            }
        }
    }

    private async Task RunCoreAsync(CancellationToken ct)
    {
        Log($"==== Harness run: provider={TestAppConfig.ProviderName(_provider)} ====");

        var setup = await _store.VerifySetupAsync(ct);
        foreach (var req in setup.Requirements)
        {
            Log($"  setup  {req.Name,-22}: {(req.Passed ? "ok" : "FAIL")}  {req.Detail}");
        }

        if (!setup.AllPassed)
        {
            Log("  !! one or more prerequisites unmet; scenarios may fail.");
        }

        // ---- Server A: no resume-token directory ----
        _server = await ServerProcess.StartAsync(_serverDll, TestAppConfig.ProviderName(_provider), null, ct);
        Log($"server A up at {_server.BaseUrl} (no resume dir; log: {_server.LogPath})");

        _client = new PulseClient(_server.HubUrl);
        _client.OnDisconnected += exc =>
        {
            RecordTransition("", "Disconnected");
            return Task.CompletedTask;
        };
        await _client.ConnectAsync(ct);
        RecordTransition("", "Connected");
        Log($"hub connected: {_server.HubUrl}");

        await S1_FilterCorrectness();
        await S2_LiveCountAndDetail();
        await S3_BulkWriteCoalescing();
        await S6_CrossProviderLatency();
        await S4_Reconnect();

        // ---- Server B: file-backed resume tokens ----
        await _server.DisposeAsync();
        _server = null;

        var resumeDir = Path.Combine(Path.GetTempPath(), "pulse-resume-" + Guid.NewGuid().ToString("N")[..8]);
        _server = await ServerProcess.StartAsync(_serverDll, TestAppConfig.ProviderName(_provider), resumeDir, ct);
        Log($"server B up at {_server.BaseUrl} (resume dir: {resumeDir})");
        await S5_ResumePersistence(resumeDir);

        await _client.DisposeAsync();
        _client = null;
        Log("==== harness run complete ====");
    }

    // ---------------------------------------------------------------------
    // S1 (4.1) Filter correctness: initial set, transition-out, transition-in.
    // ---------------------------------------------------------------------
    private async Task S1_FilterCorrectness()
    {
        var notes = new List<string>();
        var sub = await _client!.Subscribe<Order>("orders", OrderState.ListFilter("pending", "NA"), _ct);
        var model = new ListModel();
        model.Bind(sub);
        try
        {
            var first = await WaitUntilAsync(() => model.FirstSnapshotTask.IsCompleted, "S1a snapshot");
            var expected = (await _store.GetAllAsync(_ct)).Count(o => o.Status == "pending" && o.Region == "NA");
            var current = model.Snapshot();
            var a = first && current.Length == expected;
            notes.Add($"a) initial filtered set {current.Length} == direct DB {expected}  {(a ? "PASS" : "FAIL")}");

            var allInFilter = current.All(o => o.Status == "pending" && o.Region == "NA");
            notes.Add($"b) all {current.Length} rows Status=pending & Region=NA  {(allInFilter ? "PASS" : "FAIL")}");

            // Detail subscription alongside the list (detail screen open).
            var probe = current.FirstOrDefault();
            if (probe is null)
            {
                probe = await ForceIntoFilterAsync(model, "S1");
                notes.Add("   (list was empty for pending/NA; forced one in)");
            }

            var detailSub = await _client.Subscribe<Order>("orders", OrderState.DetailFilter(probe.Id), _ct);
            var detail = new ListModel();
            detail.Bind(detailSub);
            var detailOk = await WaitUntilAsync(
                () => detail.Get(probe.Id) is { } o && o.Status == "pending", "S1c detail snapshot");
            notes.Add($"c) detail screen row {ShortId(probe.Id)} shows 'pending'  {(detailOk ? "PASS" : "FAIL")}");

            // App write (REST) pending -> shipped: leaves list filter AND updates detail.
            var postOk = await PostStatusAsync(probe.Id, "shipped");
            var leftList = await WaitUntilAsync(() => !model.Contains(probe.Id), "S1d transition-out");
            var detailMoved = await WaitUntilAsync(
                () => detail.Get(probe.Id) is { } o && o.Status == "shipped", "S1d detail moved");
            var outCount = model.Snapshot().Length;
            notes.Add($"d) REST move out of filter: removed={leftList}, list {expected}->{outCount}, detail->shipped={detailMoved}, post={postOk}  " +
                $"{(postOk && leftList && detailMoved ? "PASS" : "FAIL")}");

            // External writer moves a non-NA order's Region to NA -> enters the filter.
            var outsider = (await _store.GetAllAsync(_ct)).FirstOrDefault(o => o.Region != "NA");
            var eOk = true;
            if (outsider is not null)
            {
                // The filter is pending/NA: set both so the row genuinely crosses the boundary.
                await _store.UpdateStatusAsync(outsider.Id, "pending", _ct);
                await _store.UpdateRegionAsync(outsider.Id, "NA", _ct);
                var appeared = await WaitUntilAsync(
                    () => model.Get(outsider.Id) is { } o && o.Region == "NA" && o.Status == "pending",
                    "S1e transition-in");
                eOk = appeared;
                notes.Add($"e) external writer moved {ShortId(outsider.Id)} into filter  {(appeared ? "PASS" : "FAIL")}");
            }
            else
            {
                notes.Add("e) no non-NA order available; skipped");
            }

            await detailSub.UnsubscribeAsync(_ct);
            notes.Add("f) detail unsubscribed cleanly");

            Add(new ScenarioResult(
                "S1", "Filter correctness (4.1)",
                a && allInFilter && detailOk && postOk && leftList && detailMoved && eOk,
                string.Join("   ", notes)));
        }
        finally
        {
            model.Unbind(sub);
            await sub.UnsubscribeAsync(CancellationToken.None);
        }
    }

    // ---------------------------------------------------------------------
    // S2 (3.2/3.3) Live count badge + list/detail co-subscription; detail
    //             dispose while list stays live.
    // ---------------------------------------------------------------------
    private async Task S2_LiveCountAndDetail()
    {
        var notes = new List<string>();
        var sub = await _client!.Subscribe<Order>("orders", OrderState.ListFilter("pending", "NA"), _ct);
        var model = new ListModel();
        model.Bind(sub);
        var detailSub = (IPulseSubscription<Order>?)null;
        try
        {
            await WaitUntilAsync(() => model.FirstSnapshotTask.IsCompleted, "S2a snapshot");
            var probe = model.Snapshot().FirstOrDefault() ?? await ForceIntoFilterAsync(model, "S2");

            var badge = () => model.Snapshot().Length;
            var expected = (await _store.GetAllAsync(_ct)).Count(o => o.Status == "pending" && o.Region == "NA");
            notes.Add($"a) badge {badge()} == direct DB {expected}  {(badge() == expected ? "PASS" : "FAIL")}");

            detailSub = await _client.Subscribe<Order>("orders", OrderState.DetailFilter(probe.Id), _ct);
            var detail = new ListModel();
            detail.Bind(detailSub);
            await WaitUntilAsync(() => detail.Contains(probe.Id), "S2b detail snapshot");

            // App write to the probed order: BOTH screens must move together.
            await PostStatusAsync(probe.Id, "delivered");
            var listMoved = await WaitUntilAsync(() => !model.Contains(probe.Id), "S2c list updated");
            var detailMoved = await WaitUntilAsync(
                () => detail.Get(probe.Id) is { } o && o.Status == "delivered", "S2c detail updated");
            notes.Add($"c) app write: list dropped ({listMoved}), detail showed delivered ({detailMoved})  " +
                $"{(listMoved && detailMoved ? "PASS" : "FAIL")}");

            // Detail screen closed (unsubscribe); list keeps streaming.
            await detailSub.UnsubscribeAsync(_ct);
            detail.Unbind(detailSub);
            detailSub = null;

            await _store.UpdateStatusAsync(probe.Id, "pending", _ct);
            var reEnters = await WaitUntilAsync(
                () => model.Get(probe.Id) is { } o && o.Status == "pending", "S2d list still live after detail close");
            notes.Add($"d) after detail close, list still streams external write back in  {(reEnters ? "PASS" : "FAIL")}");

            Add(new ScenarioResult(
                "S2", "Live count + detail co-subscription (3.2/3.3)",
                badge() == expected && listMoved && detailMoved && reEnters,
                string.Join("   ", notes)));
        }
        finally
        {
            model.Unbind(sub);
            await sub.UnsubscribeAsync(CancellationToken.None);
            if (detailSub is not null) await detailSub.UnsubscribeAsync(CancellationToken.None);
        }
    }

    // ---------------------------------------------------------------------
    // S3 (4.2) Bulk write resilience: 100 flips @15ms, then settle & verify
    //             the mirror equals the direct DB; record coalescing.
    // ---------------------------------------------------------------------
    private async Task S3_BulkWriteCoalescing()
    {
        var notes = new List<string>();
        var sub = await _client!.Subscribe<Order>("orders", where: null, _ct);
        var model = new ListModel();
        model.Bind(sub);
        try
        {
            await WaitUntilAsync(() => model.FirstSnapshotTask.IsCompleted, "S3a snapshot");
            var before = model.Snapshot().Length;

            var sw = Stopwatch.StartNew();
            var applied = await _store.BulkMutateAsync(100, 15, _rng, _ct);
            sw.Stop();
            notes.Add($"a) wrote {applied} status flips in {sw.Elapsed.TotalMilliseconds:F0} ms");

            // Settle: mirror identical to direct DB (all 50 rows, matching statuses).
            var settled = await WaitUntilAsync(() =>
            {
                var mirror = model.Snapshot();
                var db = _store.GetAllAsync(_ct).GetAwaiter().GetResult();
                return mirror.Length == db.Count
                    && mirror.All(m => db.FirstOrDefault(d => d.Id == m.Id)?.Status == m.Status);
            }, "S3b mirror == db");

            var dbNow = await _store.GetAllAsync(_ct);
            notes.Add($"b) mirror == direct DB after settle (rows {before} -> {dbNow.Count})  {(settled ? "PASS" : "FAIL")}");
            notes.Add($"c) received {model.ChangeCount} change events for {applied} writes " +
                $"(coalescing ratio {applied / Math.Max(1, model.ChangeCount):F1}:1 if >1)");

            Add(new ScenarioResult(
                "S3", "Bulk write resilience (4.2)",
                settled && applied == 100,
                string.Join("   ", notes)));
        }
        finally
        {
            model.Unbind(sub);
            await sub.UnsubscribeAsync(CancellationToken.None);
        }
    }

    // ---------------------------------------------------------------------
    // S4 (4.3) Reconnect: kill server, mutate during outage, restart on the
    //             same port, expect resync snapshot + Reconnecting transition.
    // ---------------------------------------------------------------------
    private async Task S4_Reconnect()
    {
        var notes = new List<string>();
        var baseUrl = _server!.BaseUrl;
        var sub = await _client!.Subscribe<Order>("orders", OrderState.ListFilter("processing", "EU"), _ct);
        var model = new ListModel();
        model.Bind(sub);
        try
        {
            await WaitUntilAsync(() => model.FirstSnapshotTask.IsCompleted, "S4a snapshot");

            // Kill server; wait for the client to notice (Reconnecting).
            _server.Kill();
            var noticed = await WaitForStateAsync(state => state is "Reconnecting" or "Disconnected", "S4b reconnect notice");
            notes.Add($"a) client noticed the drop (Reconnecting/Disconnected)  {(noticed ? "PASS" : "FAIL")}");

            // Changes made while the server is down.
            var outsider = (await _store.GetAllAsync(_ct)).FirstOrDefault(o => !(o.Status == "processing" && o.Region == "EU"));
            if (outsider is not null)
            {
                await _store.UpdateStatusAsync(outsider.Id, "processing", _ct);
                await _store.UpdateRegionAsync(outsider.Id, "EU", _ct);
                notes.Add($"b) during outage moved {ShortId(outsider.Id)} into (processing, EU)");
            }

            // Restart on the same port (new process, same URL).
            await _server.DisposeAsync();
            _server = await ServerProcess.StartAsync(_serverDll, TestAppConfig.ProviderName(_provider), null, _ct, baseUrl);
            notes.Add($"c) server restarted on {_server.BaseUrl}");

            var recon = await WaitForStateAsync(state => state == "Connected", "S4c reconnect");
            notes.Add($"d) client reached Connected  {(recon ? "PASS" : "FAIL")}");

            var resynced = outsider is null || await WaitUntilAsync(
                () => model.Contains(outsider.Id) && model.Get(outsider.Id)!.Region == "EU",
                "S4d resync reflects outage changes");
            notes.Add($"e) post-reconnect snapshot includes the outage-era change  {(resynced ? "PASS" : "FAIL")}");

            var transitions = string.Join(" -> ", _transitions.Select(t => $"{t.From}|{t.To}"));
            Add(new ScenarioResult(
                "S4", "Reconnect & resync (4.3)",
                noticed && recon && resynced,
                string.Join("   ", notes) + $"  [states: {transitions}]"));
        }
        finally
        {
            model.Unbind(sub);
            await sub.UnsubscribeAsync(CancellationToken.None);
        }
    }

    // ---------------------------------------------------------------------
    // S5 (4.5) Restart persistence: server B with a file-backed resume store;
    //             kill, mutate during outage, restart, verify no loss + token file.
    // ---------------------------------------------------------------------
    private async Task S5_ResumePersistence(string resumeDir)
    {
        var notes = new List<string>();
        var baseUrl = _server!.BaseUrl;

        _client?.DisposeAsync().GetAwaiter().GetResult();
        _client = new PulseClient(_server.HubUrl);
        await _client.ConnectAsync(_ct);
        RecordTransition("", "Connected");

        var sub = await _client.Subscribe<Order>("orders", OrderState.ListFilter("cancelled", "APAC"), _ct);
        var model = new ListModel();
        model.Bind(sub);
        try
        {
            await WaitUntilAsync(() => model.FirstSnapshotTask.IsCompleted, "S5a snapshot");

            // A change the token store should capture before the kill.
            var before = (await _store.GetAllAsync(_ct)).FirstOrDefault(o => !(o.Status == "cancelled" && o.Region == "APAC"));
            if (before is not null)
            {
                await _store.UpdateStatusAsync(before.Id, "cancelled", _ct);
                await _store.UpdateRegionAsync(before.Id, "APAC", _ct);
                await WaitUntilAsync(() => model.Contains(before.Id), "S5b pre-kill change");
                notes.Add($"b) pre-kill change observed by client");
            }

            await Task.Delay(500, _ct); // let the token flush to disk
            _server.Kill();
            notes.Add("c) server killed");

            var during = (await _store.GetAllAsync(_ct)).FirstOrDefault(o => o.Region == "NA");
            if (during is not null)
            {
                await _store.UpdateStatusAsync(during.Id, "cancelled", _ct);
                await _store.UpdateRegionAsync(during.Id, "APAC", _ct);
                notes.Add($"d) during outage moved {ShortId(during.Id)} into (cancelled, APAC)");
            }

            await _server.DisposeAsync();
            _server = await ServerProcess.StartAsync(_serverDll, TestAppConfig.ProviderName(_provider), resumeDir, _ct, baseUrl);

            var recon = await WaitForStateAsync(state => state == "Connected", "S5e reconnect");
            var resynced = during is null || await WaitUntilAsync(
                () => model.Contains(during.Id), "S5f resync");
            notes.Add($"e) restarted + client reconnected ({recon})  ({(recon && resynced ? "PASS" : "FAIL")})");
            notes.Add($"f) outage-era change present after restart  {(resynced ? "PASS" : "FAIL")}");

            var tokens = Directory.Exists(resumeDir)
                ? Directory.GetFiles(resumeDir, "*.json").Select(Path.GetFileName).ToArray()
                : Array.Empty<string>();
            notes.Add($"g) resume-token files persisted: {tokens.Length}  [{(tokens.Length > 0 ? "PASS" : "FAIL")}]");

            Add(new ScenarioResult(
                "S5", "Restart persistence / resume tokens (4.5)",
                recon && resynced && tokens.Length > 0,
                string.Join("   ", notes)));
        }
        finally
        {
            model.Unbind(sub);
            await sub.UnsubscribeAsync(CancellationToken.None);
        }
    }

    // ---------------------------------------------------------------------
    // S6 (4.6) Cross-provider latency: single-write end-to-end times.
    // ---------------------------------------------------------------------
    private async Task S6_CrossProviderLatency()
    {
        var notes = new List<string>();
        var sub = await _client!.Subscribe<Order>("orders", where: null, _ct);
        var model = new ListModel();
        model.Bind(sub);
        try
        {
            await WaitUntilAsync(() => model.FirstSnapshotTask.IsCompleted, "S6a snapshot");
            var all = await _store.GetAllAsync(_ct);
            if (all.Count == 0)
            {
                Add(new ScenarioResult("S6", "Cross-provider latency (4.6)", false, "no orders to update"));
                return;
            }

            var samples = new List<double>();
            for (var i = 0; i < 12; i++)
            {
                var target = all[_rng.Next(all.Count)];
                var newStatus = OrderState.Statuses
                    .Where(s => s != target.Status)
                    .OrderBy(_ => _rng.Next())
                    .First();
                var sw = Stopwatch.StartNew();
                await _store.UpdateStatusAsync(target.Id, newStatus, _ct);
                sw.Stop();

                // Wait for the new status to actually arrive (containment alone is meaningless —
                // the row is already present), so the sample is write-commit -> client receipt.
                var received = await WaitUntilAsync(
                    () => model.Get(target.Id) is { } o && o.Status == newStatus,
                    "S6b receipt", TimeSpan.FromSeconds(10));
                if (received)
                {
                    samples.Add(sw.Elapsed.TotalMilliseconds);
                }
            }

            if (samples.Count == 0)
            {
                Add(new ScenarioResult("S6", "Cross-provider latency (4.6)", false, "no receipts measured"));
                return;
            }

            var sorted = samples.OrderBy(x => x).ToArray();
            var avg = samples.Average();
            var p50 = sorted[(int)(sorted.Length * 0.5)];
            var p99 = sorted[(int)(sorted.Length * 0.99)];
            notes.Add($"n={samples.Count}  avg={avg:F0} ms  p50={p50:F0} ms  p99={p99:F0} ms  max={sorted[^1]:F0} ms");

            Add(new ScenarioResult(
                "S6", "Cross-provider latency (4.6)",
                samples.Count >= 10 && p99 < TimeSpan.FromSeconds(5).TotalMilliseconds,
                string.Join("   ", notes)));
        }
        finally
        {
            model.Unbind(sub);
            await sub.UnsubscribeAsync(CancellationToken.None);
        }
    }

    // ---------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------

    /// <summary>Picks any order outside the pending/NA filter and moves it in (external writer).</summary>
    private async Task<Order> ForceIntoFilterAsync(ListModel model, string tag)
    {
        var any = (await _store.GetAllAsync(_ct)).FirstOrDefault();
        if (any is not null)
        {
            await _store.UpdateStatusAsync(any.Id, "pending", _ct);
            await _store.UpdateRegionAsync(any.Id, "NA", _ct);
            await WaitUntilAsync(() => model.Contains(any.Id), $"{tag} forced into filter");
            return (await _store.GetByIdAsync(any.Id, _ct))!;
        }

        await _store.SeedAsync(50, _ct);
        return (await _store.GetAllAsync(_ct))[0];
    }

    private async Task<bool> PostStatusAsync(string id, string status)
    {
        try
        {
            using var resp = await _http.PostAsJsonAsync(
                $"{_server!.BaseUrl}/orders/{id}/status",
                new { status },
                _ct);
            return resp.IsSuccessStatusCode;
        }
        catch
        {
            return false;
        }
    }

    private async Task<bool> WaitUntilAsync(Func<bool> condition, string what, TimeSpan? timeout = null)
    {
        var sw = Stopwatch.StartNew();
        var deadline = timeout ?? Wait;
        while (sw.Elapsed < deadline)
        {
            _ct.ThrowIfCancellationRequested();
            if (condition()) return true;
            await Task.Delay(50, _ct);
        }

        Log($"  ! timed out waiting: {what}");
        return false;
    }

    private async Task<bool> WaitForStateAsync(Func<string, bool> predicate, string what)
    {
        var sw = Stopwatch.StartNew();
        while (sw.Elapsed < Wait)
        {
            _ct.ThrowIfCancellationRequested();
            var state = _client?.State.ToString() ?? "";
            if (state != _lastConnectionState)
            {
                RecordTransition(_lastConnectionState, state);
            }

            if (predicate(state)) return true;
            await Task.Delay(100, _ct);
        }

        Log($"  ! timed out waiting for state: {what} (last={_client?.State})");
        return false;
    }

    private void RecordTransition(string from, string to)
    {
        if (from == to) return;
        _lastConnectionState = to;
        _transitions.Add(new StateTransition(from, to, Math.Round(DateTime.UtcNow.Subtract(_epoch).TotalSeconds, 1)));
        Log($"  [state] {from} -> {to}");
    }

    private static readonly DateTime _epoch = DateTime.UtcNow;

    private static string ShortId(string id) => id.Length <= 12 ? id : id[..12];

    private void Add(ScenarioResult result)
    {
        _results.Add(result);
        Log($"{result.Id,-4} {(result.Passed ? "PASS" : "FAIL")}  {result.Name}\n       {result.Notes}");
    }

    private void Log(string message) => Console.WriteLine($"[{TestAppConfig.ProviderName(_provider)}] {message}");
}
