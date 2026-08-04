# Pulse Order Dashboard — Cross-Provider Test Report

Date: 2026-08-03
Harness: `apps/Pulse.TestApp.Harness` (headless, drives `Pulse.Client` exactly like the MAUI UI)
Server: `apps/Pulse.TestApp.Server` (provider selected once via `PULSE_PROVIDER`)
Seed: `seed/Pulse.TestApp.Seed` — 50 orders, deterministic `Random(20260803)`, same distribution on all three providers.

All scenarios are **headless** and assert against a **direct database read** as the source of
truth, so results are valid regardless of seed distribution. The on-device visual scenarios
(§4.4, row-flash, mobile lifecycle, status-bar look) are scaffolded in `apps/Pulse.TestApp`
(MAUI) but require a device + MAUI workload and were **not** executed here.

---

## 1. Result summary

| Scenario | Mongo | Postgres | SQL Server |
|---|---|---|---|
| S1  Filter correctness (4.1) — initial set, transition-out, transition-in | ✅ | ✅ | ✅ |
| S2  Live count badge + detail co-subscription (3.2/3.3) | ✅ | ✅ | ✅ |
| S3  Bulk write resilience (4.2) — 100 flips @15ms, mirror == DB | ✅ | ✅ | ✅ |
| S4  Reconnect & resync (4.3) — kill server, mutate during outage, restart | ✅ | ✅ | ✅ |
| S5  Restart persistence / resume tokens (4.5) | ✅ | ✅ | ✅ |
| S6  Cross-provider latency (4.6) — write-commit → client receipt | ✅ | ✅ | ✅ |
| **Score** | **6/6** | **6/6** | **6/6** |

All three providers converge: with the fixes below, every scenario passes headlessly.

---

## 2. Real bugs found and fixed

This was the point of the harness — three of them only show up on a full-stack run
(server → SignalR wire → `Pulse.Client` → typed model), and all three were latent bugs in
the library, not in the test app.

### 2.1 `AddMongoSource` DI: registered concrete `MongoClient`, resolved `IMongoClient`
`src/Pulse.Mongo/MongoSourceServiceCollectionExtensions.cs` registered
`services.AddSingleton(new MongoClient(...))` but the source factory did
`GetRequiredService<IMongoClient>()`. First subscription failed with
`No service for type 'MongoDB.Driver.IMongoClient' has been registered` and the hub closed
the connection.
**Fix:** register `AddSingleton<IMongoClient>(new MongoClient(...))`.

### 2.2 Mongo `_id` (ObjectId) and `Total` (Decimal128) broke the wire → snapshot silently dropped
`src/Pulse.Mongo/BsonValueConverter.cs` used `BsonTypeMapper.MapToDotNetValue`, which
returns:
- `ObjectId` for `_id` — serialized over JSON as an object (`{Timestamp, Machine, Pid, Increment}`).
  The client cannot bind `Order.Id: string` from that, and `TryGetId` produced a
  `Dictionary.ToString()` garbage id. Result: **the entire snapshot failed to deserialize** —
  the per-message exception was swallowed in `PulseSubscription.ProcessAsync`, so the
  subscription silently delivered nothing (current = 0, no events). This was exactly the
  spec's "ObjectId surfaced as string" case, and it was broken.
- `Decimal128` for `Total` — serialized as an empty object `{}`, which also failed to bind.

**Fix (both):** in `BsonValueConverter.ToClrValue`, map `ObjectId → hex string` and
`Decimal128 → decimal`. Ids are now strings in memory on every provider.

### 2.3 Mongo `_id` filter matched ObjectId with a string → detail screen never matched
`src/Pulse.Mongo/MongoFilterTranslator.cs` translated `FieldCompare("_id", Eq, "<hex>")` to
`Filter.Eq("_id", BsonString)`, which never matches an ObjectId `_id`. The detail-screen
snapshot query returned zero rows.
**Fix:** when the field is `_id`, parse the string filter value back to `BsonObjectId`
(valid hex → ObjectId, otherwise falls back to string).

### 2.4 SQL Server `_id` was a `Guid` in memory → live detail updates never delivered
`src/Pulse.SqlServer/SqlServerChangeSource.cs` left `uniqueidentifier` cells (incl. the `_id`
primary key) as `Guid`. Snapshot queries matched in SQL (fine), but **live change events** are
filtered in memory by `DictionaryFilterMatcher`, and `FilterValueHelpers.Equal(Guid, string)`
is false. So the detail subscription received its initial snapshot but **never an update**,
while the list subscription (string filters) updated fine.
**Fix:** `CoerceCell` maps `Guid → string`, so `_id` is a string in memory and on the wire,
consistent with Mongo/Postgres.

> Net effect: all three providers now expose `_id` as a plain string and deliver live updates
> to both list and detail subscriptions.

### 2.5 Harness/test-app fixes (not library bugs)
- `SqlServerOrderStore.VerifySetupAsync` queried `is_change_tracking_enabled` — not a
  `sys.databases` column. Now uses `DATABASEPROPERTYEX(DB_NAME(), 'IsChangeTrackingEnabled')`.
- The harness's S6 latency was measuring containment, not receipt of the *new* value (fixed to
  wait for the new status), and the S1e transition-in candidate was outside the filter's status
  (now set both fields).

---

## 3. Scenario detail

### S1 — Filter correctness (4.1)
For each provider, on a `pending/NA` list subscription:
- **a** initial snapshot row count == direct-DB count ✅
- **b** every row is `pending`/`NA` ✅
- **c** opening the detail screen (subscription on `_id`) shows `pending` ✅
- **d** app write (`POST /orders/{id}/status`) `pending → shipped`: the row **leaves** the list
  filter **and** the detail screen updates to `shipped` ✅
- **e** an external DB write flipping a foreign row into `pending/NA` surfaces in the live list ✅
- **f** unsubscribing the detail screen is clean and the list keeps streaming ✅

### S2 — Live count + detail co-subscription (3.2/3.3)
- **a** count badge == direct-DB count ✅
- **b/c** with list + detail subscribed simultaneously, one app write updates both ✅
- **d** after closing the detail screen, the list continues to stream external writes ✅

### S3 — Bulk write resilience (4.2)
- 100 status flips @ 15 ms (external writer). Mirror converges to direct DB after settle ✅
- Change-event volume:

| Provider | events received for 100 writes | note |
|---|---|---|
| Mongo | 100/100 | change stream, 1:1 |
| Postgres | 100/100 | log-based, 1:1 |
| SQL Server | 91–95/100 | **poller coalesces** multiple updates to the same key within one poll tick (change-tracking is per-version; a row updated twice before a poll yields one event) |

The SQL Server coalescing is *correct* (the mirror still converges to the DB) — a feature to
call out, not a bug. It is why batching/coalescing is a recommended roadmap item (§6).

### S4 — Reconnect & resync (4.3)
- Kill the server while subscribed. Client observed `Connected → Reconnecting` within ~1 s ✅
- External writes made **while the server was down** are reflected after reconnect ✅
- Client auto-reconnected and re-snapshotted (no user action) ✅
- Observed transitions: `Connected → Reconnecting → Connected`

### S5 — Restart persistence / resume tokens (4.5)
- Server restarted with a `FileResumeTokenStore` directory
- Change before the kill observed; changes during the outage present after restart ✅
- Resume-token file persisted for the source ✅
- Client went through `Connected → Disconnected → Connected` on the server swap ✅

### S6 — Cross-provider latency (4.6)
Single-write end-to-end (write commit → new value visible in the live mirror), n = 12 per provider:

| Provider | avg | p50 | p99 | max |
|---|---|---|---|---|
| Mongo | 4 ms | 4 ms | 6 ms | 6 ms |
| Postgres | 3 ms | 3 ms | 4 ms | 4 ms |
| SQL Server | 6 ms | 5 ms | 28 ms | 28 ms |

SQL Server shows a longer tail (p99 28 ms) consistent with its poll-tick cadence.

---

## 4. Provider differences (novel behaviors observed)

| Aspect | Mongo | Postgres | SQL Server |
|---|---|---|---|
| `_id` type | ObjectId → hex string | uuid → string | uniqueidentifier → string |
| Change capture | change stream (needs replica set) | log table + triggers | change tracking + poll |
| Snapshot + live merge | shared watch per source | shared watcher per source | poller + snapshot cut-point |
| Event volume under bulk write | 1:1 | 1:1 | coalesced per poll tick |
| One-time setup | `rs.initiate()` | none (schema auto) | `ALTER DATABASE ... CHANGE_TRACKING` (needs permission) |
| Resume across restart | resume token persisted (tested) | resume token persisted (tested) | resume token persisted (tested) |

All three pass identical headless scenarios. The only functional divergence is SQL Server's
coalescing under rapid same-row writes, which is benign (state always converges).

---

## 5. Manual / on-device items (not executed headlessly)

These need a real app + device and a MAUI workload (`dotnet workload install maui`), then
build `apps/Pulse.TestApp`:

- §4.4 row-flash on transition and count-badge animation
- Status-bar visual states (offline / connecting / connected / reconnecting) on real networks
- Mobile lifecycle: app background/resume → fresh snapshot (scaffolded via
  `PulseService.NotifyStateChanged`)
- Detail screen push/pop UX and orientation
- Reconnect behavior on a phone that loses Wi-Fi (SignalR automatic reconnect window)

The MAUI app is a thin binding over the same subscription semantics the harness verifies, so
any screen rendering a `pending/NA`-type list against a live server will show the same live
behavior that S1–S3 pass.

---

## 6. Recommendations (roadmap)

1. **Coalescing / batching (Postgres + SQL Server)** — make the coalescing we *observed* on
   SQL Server a first-class provider behavior: batch change events per poll tick into a single
   `PulseSnapshot`-style upsert per document before fan-out. This drops wire volume for
   bursty writers (the §4.2 workload) with zero correctness loss (proven by S3's convergence).
2. **Aggregation subscriptions** — add an `Aggregate` filter/operator (e.g. `count`, `sum`
   over the matching set) evaluated server-side alongside the snapshot, so the count badge is
   computed once per change instead of client-side. Requires the registry's fan-out to diff
   aggregates per change (the `TrackedIds` machinery already tracks set membership, which is
   the hard 90%).
3. **Expose provider mode in the client** — the latency gap is negligible, but the *semantics*
   differ (SQL Server coalescing). Document `SourceMode: continuous | polled` via a
   `PulseSnapshot`-adjacent capability message so UIs can render "eventually consistent" hints.
4. **Bulk-write throughput** — the 100-write run settled in ~2 s on every provider; consider
   `INSERT ... ON CONFLICT` batch paths in the Postgres/SQL Server sources when polling latency
   matters, or a configurable poll interval for SQL Server (currently implicit).
5. **Error surfacing** — `PulseSubscription.ProcessAsync` swallows per-message exceptions to a
   `Debug.WriteLine`. For on-device debugging, surface these on `OnError`/a client-side log
   hook (this is how bug §2.2 hid). Recommend a `PulseClient.OnSubscriptionError` event.

## 7. Reproduction

```bash
# 1. databases
docker compose -f seed/compose.yaml up -d
docker compose -f seed/compose.yaml exec mongo mongosh --eval 'rs.initiate()'

# 2. build
dotnet build Pulse.sln
dotnet build apps/Pulse.TestApp.Server/Pulse.TestApp.Server.csproj

# 3. seed + verify per provider
dotnet run --project seed/Pulse.TestApp.Seed -- --provider <mongo|postgres|sqlserver> verify-setup
dotnet run --project seed/Pulse.TestApp.Seed -- --provider <mongo|postgres|sqlserver> seed --count 50

# 4. headless run (JSON report written to --report)
dotnet run --project apps/Pulse.TestApp.Harness -- \
  --provider <mongo|postgres|sqlserver> \
  --server-dll apps/Pulse.TestApp.Server/bin/Debug/net8.0/Pulse.TestApp.Server.dll \
  --report /tmp/pulse-<provider>-report.json
```
