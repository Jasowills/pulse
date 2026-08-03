# Pulse

**Firebase-style reactive queries and live sync, native to C#/.NET, for MongoDB, Postgres, and SQL Server.**

Pulse is a .NET package suite that lets clients subscribe to a query (source + filter), receive an initial snapshot of matching documents, and then stay in sync as the database changes — MongoDB change streams, Postgres triggers, and SQL Server change tracking on the server, delivered to clients over SignalR.

Target framework: **.NET 8** (LTS, MAUI-compatible) for all projects.

> **Status: v0.3 — SQL Server provider shipped.** The v0.1 MVP steps 0–9 are done: scaffold, `Pulse.Abstractions`, `Pulse.Mongo`'s change-stream `WatchAsync` (verified end-to-end against a real Testcontainers Mongo replica set), `Pulse.Server`'s `PulseHub` broadcast with shared per-source watches and `AddMongoSource`, server-side filter matching (`DictionaryFilterMatcher`), gap-free snapshot delivery on subscribe (`GetSnapshotAsync` with watch-first as-of token capture), resume-token persistence (`IResumeTokenStore`, in-memory + file-based, with stale-token resync verified across a server restart), the `Pulse.Client` SDK's `Subscribe<T>` (typed snapshot + live changes, `Current` cache, in-order message processing), automatic client reconnect with resubscribe-treats-as-fresh-snapshot, and match-transition handling (an update that flips a doc out of a filter becomes a synthetic delete; one that flips it in becomes an insert). v0.2 adds a second provider: `Pulse.Postgres` watches tables via triggers plus a `pulse._changes` log (single-column primary key required, `_id` = pk), with the same resume-token, snapshot, and match-transition semantics — verified end-to-end against a real Testcontainers Postgres. v0.3 adds a third provider: `Pulse.SqlServer` watches tables via native **Change Tracking** (`CHANGETABLE(CHANGES ...)` + version-based resume tokens; single-column primary key required, `_id` = pk), verified end-to-end against a real Testcontainers SQL Server. API shapes below are the design contract and will land in that order.

---

## Table of contents

- [Why Pulse](#why-pulse)
- [Features](#features)
- [Non-goals for v0.1](#non-goals-for-v0-1)
- [How it works](#how-it-works)
- [Package layout](#package-layout)
- [Filter DSL](#filter-dsl)
- [Wire protocol](#wire-protocol)
- [Server integration](#server-integration)
- [Client SDK](#client-sdk)
- [Resume tokens and gapless delivery](#resume-tokens-and-gapless-delivery)
- [Match-transition handling](#match-transition-handling)
- [Postgres provider (v0.2)](#postgres-provider-v02)
- [SQL Server provider (v0.3)](#sql-server-provider-v03)
- [Important caveats](#important-caveats)
- [Development](#development)
- [Testing](#testing)
- [Build status](#build-status)
- [Roadmap / explicitly deferred](#roadmap--explicitly-deferred)

---

## Why Pulse

Many C# apps end up polling the database to reflect state changes in the UI, or hand-rolling a pub/sub layer that drifts from the data. Pulse treats the database as the source of truth and pushes changes out, so a client's view stays correct without a query being re-run on every change.

Pulse is designed to be **provider-agnostic from day one**: the core abstractions carry no database concepts, so the Postgres provider (v0.2) and the SQL Server provider (v0.3) were added without touching the server hub, the subscription registry, or the client SDK.

## Features

- **Filter-based subscriptions** — subscribe to a source (collection/table) with a filter expression; only matching change events are delivered.
- **Snapshot + live diffs** — on subscribe, the client receives the current matching documents, then incremental changes.
- **Resume-token persistence** — a server restart or brief outage does not silently drop events.
- **Automatic reconnect + resubscribe** — handled by the client SDK.
- **Multi-provider** — MongoDB (v0.1), Postgres (v0.2), and SQL Server (v0.3) behind the same `IChangeSource` / `IFilterMatcher` seam.
- **Live list semantics** — updates that move a document in or out of a filter's match set are surfaced correctly (synthetic insert/remove to each affected subscriber).

## Non-goals for v0.1

These are explicitly deferred and **not built** in v0.1:

- Offline-first sync / conflict resolution on mobile.
- Auth/authorization framework (v0.1 exposes the `IPulseAuthorizer` seam only; the default allows everything — see [Important caveats](#important-caveats)).
- Multi-database support (Mongo only for v0.1, Postgres added in v0.2, SQL Server in v0.3; the interfaces must not leak provider-specific concepts).
- Horizontal scale-out across multiple server instances.
- Arbitrary pub/sub / message-queue behavior — this is DB-state-change subscription only.

---

## How it works

```
  Mongo (change streams) ──┐
                          ├─ IChangeSource.WatchAsync / GetSnapshotAsync
  Postgres (triggers +     │
    pulse._changes log) ───┼─┐
                          │ │
  SQL Server (change      ┘ │
    tracking) ──────────────┘
        ▼
  Pulse.Server  ──  SubscriptionRegistry (match-transition detection, resume tokens)
        │  SignalR Hub  /pulse
        ▼
  PulseClient ── IPulseSubscription<T> (snapshot + live changes, local cache)
```

1. A client calls `Subscribe(source, filter)`.
2. The hub opens a change stream for the source, captures a resume token, and takes a snapshot of matching documents (**watch first, then snapshot** — see [Resume tokens](#resume-tokens-and-gapless-delivery)).
3. The client receives the snapshot, then live `PulseChange` events as matching documents are inserted, updated, or deleted.
4. If the server restarts, the persisted resume token lets the provider resume from where it left off.
5. If the client drops, the SDK reconnects and re-subscribes, treating the result as a fresh snapshot.

## Package layout

```
/src
  Pulse.Abstractions/        IChangeSource, IFilterMatcher, ChangeEvent, SubscriptionFilter, ResumeToken, FilterExpr
  Pulse.Server/              SignalR hub, subscription registry, match engine, hosting extensions
  Pulse.Mongo/               MongoChangeSource : IChangeSource, Mongo resume-token codec
  Pulse.Postgres/            PostgresChangeSource : IChangeSource (triggers + _changes log), PostgresFilterTranslator
  Pulse.SqlServer/           SqlServerChangeSource : IChangeSource (change tracking), SqlServerFilterTranslator
  Pulse.Client/              PulseClient SDK (ASP.NET, desktop, MAUI)
/tests
  Pulse.Server.Tests/
  Pulse.Mongo.Tests/         real Mongo via Testcontainers
  Pulse.Postgres.Tests/      real Postgres via Testcontainers
  Pulse.SqlServer.Tests/     real SQL Server via Testcontainers
  Pulse.Client.Tests/
  Pulse.Integration.Tests/   end-to-end: Testcontainers Mongo/Postgres/SQL Server + in-process SignalR + real PulseClient
/samples
  Pulse.Sample.Server/       ASP.NET Core minimal API host wiring Pulse in
  Pulse.Sample.MauiClient/   MAUI app (deferred until build step 10)
```

NuGet package IDs: `Pulse.Abstractions`, `Pulse.Server`, `Pulse.Mongo`, `Pulse.Postgres`, `Pulse.SqlServer`, `Pulse.Client`.

## Filter DSL

Subscriptions use a small, provider-neutral expression tree — **not** raw MongoDB query syntax — so it maps cleanly onto a BSON document (Mongo), a JSON document (Postgres), and a SQL row (SQL Server).

```json
{
  "source": "orders",
  "where": {
    "and": [
      { "field": "status", "op": "eq", "value": "pending" },
      { "field": "total", "op": "gte", "value": 100 }
    ]
  }
}
```

Operators: `eq`, `ne`, `gt`, `gte`, `lt`, `lte`, `in`, `notIn`, `exists`.

Semantics that matter (defined and tested explicitly):

- Field paths support dot notation for nested documents (`"customer.address.city"`).
- Comparisons perform explicit type coercion (a JSON `long` vs a `double` field, etc.) — this is the #1 source of "filter silently doesn't match" bugs, so it is spec'd and covered by tests rather than left implicit.
- `exists` means **key present and non-null**.

`FilterExpr` has System.Text.Json converters in `Pulse.Abstractions` so server and client serialize/deserialize identically.

## Wire protocol

| Direction | Method / Event | Payload |
|---|---|---|
| Client → Server | `Subscribe` | `{ source, where }` → returns `subscriptionId` |
| Client → Server | `Unsubscribe` | `subscriptionId` |
| Server → Client | `PulseSnapshot` | `{ subscriptionId, documents: [...] }` |
| Server → Client | `PulseChange` | `{ subscriptionId, kind, documentId, document? }` |
| Server → Client | `PulseError` | `{ subscriptionId?, message }` |

All wire DTOs live in `Pulse.Abstractions` so server and client share the exact same types — no hand-synced copies.

## Server integration

```csharp
var builder = WebApplication.CreateBuilder(args);

// Mongo provider (change streams)
builder.Services.AddMongoSource(options =>
{
    options.ConnectionString = mongoConnectionString;
    options.Database = databaseName;
});

// Postgres provider (triggers + pulse._changes log)
builder.Services.AddPostgresSource(options =>
{
    options.ConnectionString = postgresConnectionString;
});

// SQL Server provider (change tracking)
builder.Services.AddSqlServerSource(options =>
{
    options.ConnectionString = sqlServerConnectionString;
});

var app = builder.Build();
app.MapHub<PulseHub>("/pulse");
app.Run();
```

`AddXxxSource()` is provider-specific and registers its own `SubscriptionRegistry`, `IResumeTokenStore` (in-memory by default), and change source; nothing in `Pulse.Server` references a concrete provider. A server typically wires a single provider — multiple registries route by `CanHandle`, which defaults to claiming every source.

Shared per-source watchers are resilient to transient failures: if the underlying poller faults (a dropped connection, change-tracking cleanup, a missed NOTIFY), it restarts with capped exponential backoff and resumes from the last delivered resume point, so a momentary database blip doesn't permanently stop delivery. Private resumed watches surface the same fault through their `Completion` task.

Authorization is an extension point, not a system: implement `IPulseAuthorizer` and register it. See the caveat below — the default allows everything.

## Client SDK

`PulseClient` works anywhere `Microsoft.AspNetCore.SignalR.Client` and the BCL run: ASP.NET server-to-server, WPF/desktop, and MAUI.

```csharp
await using var client = new PulseClient("https://example.com/pulse");
await client.ConnectAsync();

var sub = await client.Subscribe<Order>("orders", new FieldCompare("status", CompareOp.Eq, "pending"));

sub.OnSnapshot += docs => /* initial matching documents */;
sub.OnChange   += change => /* live diffs after the snapshot */;
IReadOnlyList<Order> current = sub.Current; // maintained local cache for direct UI binding
```

Key behaviors:

- The connection uses `WithAutomaticReconnect()`.
- **After reconnect, the SDK re-subscribes and treats the result as a fresh snapshot** (replaces `Current`, fires `OnSnapshot` again). This is *not* the same as the server-side gapless resume — see [Resume tokens](#resume-tokens-and-gapless-delivery).
- Final connection failure surfaces through `PulseClient.OnDisconnected` so the app can show a "reconnected failed" state.
- `Current` is an optional convenience cache; raw events are always exposed for apps that manage their own state.

## Resume tokens and gapless delivery

There are two distinct mechanisms in Pulse — **do not conflate them**:

1. **Server-side resume tokens** (`ResumeToken { ProviderId, Opaque }`). Each watched source keeps a resume point, persisted to a pluggable `IResumeTokenStore`. On restart, watching resumes from that point so events are not silently dropped. If a token is stale/invalid (e.g. the Mongo oplog rolled off), the provider throws `ResumeTokenInvalidException` and the caller decides to resync from a fresh snapshot — the token is never silently ignored.

2. **Client-side reconnect** — the SDK's automatic resubscribe yields a fresh snapshot. The client cannot guarantee gapless delivery across a dropped connection, so it starts over. Callers should not expect a client reconnect to produce the same gapless behavior the server's resume token provides.

The snapshot-then-watch sequencing is deliberate: the provider opens the change stream **first**, captures its initial resume token as the snapshot's "as of" point, and *then* runs the snapshot query. Any change at or after that token supersedes the snapshot for a given document, so there is no gap or duplicate.

## Match-transition handling

The Mongo provider emits accurate `ChangeEvent`s with full post-change documents attached (`FullDocument = UpdateLookup`). The server registry then decides what each subscriber sees:

- **Matched → no longer matches** (update) → subscriber receives a synthetic remove (`ChangeKind.Delete`, original `DocumentId`) so live lists drop the row.
- **Didn't match → now matches** (update) → subscriber receives it as an insert.

This logic lives in `Pulse.Server`'s `SubscriptionRegistry`, not in the Mongo provider.

## Postgres provider (v0.2)

`Pulse.Postgres` implements the same `IChangeSource` contract on top of Postgres:

- **Trigger + change log.** Pulse installs a `pulse` schema with `pulse._changes` (an append-only log) and a per-table `AFTER INSERT OR UPDATE OR DELETE` trigger that records each change as JSON and NOTIFies the `pulse_changes` channel. Watchers LISTEN with a polling fallback, so delivery is immediate and resilient to a missed notification.
- **`_id` = the primary key.** Each row is delivered with its single-column primary key exposed as `_id` (column renamed). A table with no primary key or a composite primary key is rejected at subscribe time with an actionable error — Pulse cannot identify documents without a single `_id`.
- **Snapshot + resume tokens.** `GetSnapshotAsync` captures the change-log sequence number *before* the snapshot query (watch-first), so live watching picks up without a gap or duplicate. A resume token is the sequence number; a token that points past the current log is rejected as stale.
- **Change kinds.** Postgres emits `insert`, `update`, and `delete` (no `replace`). Updates carry the changed columns in `updated_fields`, computed by diffing the new and old JSON rows.

Caveats specific to Postgres:

- **Writes before the first subscription are not captured.** The trigger is installed the first time a table is subscribed to; earlier writes are simply not in the change log (the subscribe-time snapshot covers them). The snapshot is always taken from the live table, so nothing is lost.
- **Filter paths are JSON paths.** Filters navigate `to_jsonb(row)` with jsonb `#>`/`#>>`, so dotted paths (`customer.address.city`) and array indexes work, and comparisons preserve JSON types (a number field never equals a string filter value). Range comparisons require numeric or string filter values.
- **The change log grows unbounded** in v0.2 (no pruning). A cleanup job that deletes rows below the minimum active resume token is planned.

## SQL Server provider (v0.3)

`Pulse.SqlServer` implements the same `IChangeSource` contract on top of SQL Server **Change Tracking**:

- **Native change tracking.** On first subscribe, the provider runs `ALTER DATABASE SET CHANGE_TRACKING = ON` (best-effort — it errors clearly if the login lacks permission) and `ALTER TABLE ... ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = ON)` per table. Watchers poll `CHANGETABLE(CHANGES schema.table, @version)` in 250 ms intervals; there is no trigger or change-log table of our own.
- **`_id` = the primary key.** Each row is delivered with its single-column primary key exposed as `_id`. A table with no primary key or a composite primary key is rejected at subscribe time with an actionable error. Sources are resolved as `schema.table`; a bare table name defaults to `dbo`. `ProviderIdFor` is `sqlserver:{schema}.{table}`.
- **Snapshot + resume tokens.** `GetSnapshotAsync` captures `CHANGE_TRACKING_CURRENT_VERSION()` *before* the snapshot query (watch-first), so live watching picks up without a gap or duplicate. A resume token is the 8-byte big-endian version; a token that points past the current version, belongs to a different provider, or has been cleaned up by retention (`22119` / `22122`) is rejected as stale via `ResumeTokenInvalidException`.
- **Change kinds.** SQL Server emits `insert`, `update`, and `delete` (no `replace`). Updates carry the changed columns in `updated_fields`, decoded from the per-row `SYS_CHANGE_COLUMNS` mask via `CHANGE_TRACKING_IS_COLUMN_IN_MASK`.
- **JSON columns.** `nvarchar`/`varchar` columns that hold a valid JSON object or array are embedded as nested documents (mirroring `jsonb` on Postgres); plain text stays a string.

Caveats specific to SQL Server:

- **Writes before the first subscription are not captured.** Change tracking starts the moment the table is enabled; earlier writes are simply not in the change set (the subscribe-time snapshot covers them, taken from the live table).
- **The primary key must be single-column and immutable.** Change tracking records row keys, not full rows, so a row whose PK changes reads as a delete + insert. Decimal PKs are compared by exact byte value; other decimal fields still compare as doubles (a caveat shared with Postgres).
- **Filter paths are JSON paths.** Filters navigate the first segment as a real column and any remaining segments via `JSON_VALUE` inside it, so dotted paths (`customer.address.city`) work on JSON columns, with numeric range comparisons via `TRY_CONVERT(decimal(38,18), ...)`. `_id` maps to the primary key column.
- **No `LIMIT`/`BIGSERIAL`/`RETURNING`.** Snapshot and change queries use `TOP`-free ordered reads; tables need a `bigint identity` (or similar) for numeric `_id` columns.
- **`updated_fields` is unavailable on two-column tables.** SQL Server reports `SYS_CHANGE_COLUMNS = NULL` for updates on tables with only a primary key and one other column, so Pulse cannot report which field changed there (the change event itself is still correct). Tables with three or more columns decode `updated_fields` normally.

## Important caveats

Read these before using Pulse in anything beyond a prototype.

- **The default `IResumeTokenStore` is in-memory and does NOT survive a server restart.** Persistence is opt-in: `Pulse.Server` ships a simple file-based store for v0.1; swap it in when you want restart durability. Without it, a restart can drop events.
- **The default `IPulseAuthorizer` (`AllowAllAuthorizer`) allows any client to subscribe to anything.** This is unsafe for production and must be replaced with your own `IPulseAuthorizer` implementation.
- **Unfiltered subscriptions** (`where == null`) are allowed but logged as a warning server-side — on a large collection they are a footgun.
- **Single-instance server.** v0.1 does not scale horizontally. Running multiple server instances behind a load balancer is not supported (see [Roadmap](#roadmap--explicitly-deferred)).
- **Not a general message queue.** Pulse is DB-state-change subscription only.

## Development

Prerequisites:

- [.NET 8 SDK](https://dotnet.microsoft.com/download/dotnet/8.0) (pinned in `global.json`)
- [Docker](https://www.docker.com/products/docker-desktop/) (Docker Desktop or any Docker daemon) — required for the Testcontainers-based Mongo, Postgres, and SQL Server tests
- A shell with `dotnet` on `PATH` (Homebrew `dotnet@8` is keg-only: `export PATH="/usr/local/opt/dotnet@8/bin:$PATH"`)

Commands:

```bash
dotnet build Pulse.sln          # build everything
dotnet test Pulse.sln           # run all test suites (spins up Mongo + Postgres + SQL Server via Testcontainers)
dotnet run --project samples/Pulse.Sample.Server   # run the sample server
```

Solution-wide settings (target framework, nullability, lang version) live in `Directory.Build.props`; the SDK version is pinned in `global.json`.

## Testing

- `Pulse.Abstractions` — `FilterExpr` JSON round-trip (serialize → deserialize → equal), including nested `And` / `Or` / `Not`.
- `Pulse.Server` — `DictionaryFilterMatcher` against a table of documents/filters/expected results (all operators, nested paths); registry match-transition logic with a mocked `IChangeSource`.
- `Pulse.Mongo` — a real Mongo instance via `Testcontainers.MongoDb` (change streams are not mocked): insert/update/delete detection, resume-after-token continuation, invalid-token exception path, snapshot-then-watch gap-free behavior.
- `Pulse.Postgres` — a real Postgres instance via `Testcontainers.PostgreSql` (triggers and NOTIFY are not mocked): insert/update/delete detection with changed-field diffs, shared-watch fan-out, resume-after-token continuation, filtered snapshots (nested + arithmetic), composite/no-primary-key rejection, snapshot-then-watch gap-free behavior.
- `Pulse.SqlServer` — a real SQL Server instance via `Testcontainers.MsSql` (change tracking is not mocked): insert/update/delete detection with changed-field masks, shared-watch fan-out, resume-after-token continuation, filtered snapshots (nested + arithmetic), composite/no-primary-key/missing-table rejection, stale-token rejection, snapshot-then-watch gap-free behavior.
- `Pulse.Client` — reconnect/resubscribe against an in-process test SignalR server.
- `Pulse.Integration.Tests` — end-to-end: real Mongo/Postgres/SQL Server + in-process SignalR hub + real `PulseClient`; subscribe, snapshot, mutate the database directly, assert the change arrives client-side; kill and restart the connection and assert resubscribe; restart the server with a file-backed resume token and assert no-gap resume.

## Build status

The v0.1 implementation follows this order — each step is independently demoable:

| # | Step | Status |
|---|---|---|
| 0 | Scaffold solution, project graph, dependencies | Done |
| 1 | `Pulse.Abstractions`: types + JSON converters + round-trip tests | Done |
| 2 | `Pulse.Mongo`: `WatchAsync` against Testcontainers Mongo | Done |
| 3 | `Pulse.Server`: minimal `PulseHub` broadcast (no filter/snapshot) | Done |
| 4 | `DictionaryFilterMatcher` + filtered fan-out | Done |
| 5 | `GetSnapshotAsync` + `PulseSnapshot` on subscribe (gap-free) | Done |
| 6 | Resume-token persistence (`IResumeTokenStore`, in-memory + file-based) | Done |
| 7 | `Pulse.Client` basic `Subscribe<T>` | Done |
| 8 | Client reconnect/resubscribe | Done |
| 9 | Match-transition logic | Done |
| 10 | `Pulse.Sample.MauiClient` (MAUI list bound to `Current`) | Pending |

**v0.2 — multi-provider:**

| # | Step | Status |
|---|---|---|
| 1 | `Pulse.Postgres` provider: `_changes` log + per-table triggers, shared watches, LISTEN/poll pump, resume tokens | Done |
| 2 | Postgres `GetSnapshotAsync` + `PostgresFilterTranslator` (jsonb WHERE), composite/no-PK rejection | Done |
| 3 | `AddPostgresSource` DI extension | Done |
| 4 | Postgres unit + end-to-end tests (Testcontainers, restart-resume) | Done |
| 5 | `Pulse.SqlServer` provider (change tracking) | Done |

**v0.3 — third provider:**

| # | Step | Status |
|---|---|---|
| 1 | `Pulse.SqlServer` provider: change tracking bootstrap, `CHANGETABLE(CHANGES ...)` pump, shared watches, version resume tokens | Done |
| 2 | SQL Server `GetSnapshotAsync` + `SqlServerFilterTranslator` (JSON_VALUE WHERE), composite/no-PK rejection | Done |
| 3 | `AddSqlServerSource` DI extension | Done |
| 4 | SQL Server unit + end-to-end tests (Testcontainers, restart-resume) | Done |

## Roadmap / explicitly deferred

- **Horizontal scale-out** — multiple server instances sharing subscription state. SignalR already supports a Redis backplane for connection fan-out; filter state and match-transition tracking would need to be shared/sharded too. The registry is structured so this is a non-breaking addition.
- **Offline queue / conflict resolution** for mobile clients.
- **Row-level authorization** beyond the `IPulseAuthorizer` seam.
- **Richer filter DSL** — array-element matching, full-text search, etc. (v2).
