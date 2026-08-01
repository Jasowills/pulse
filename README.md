# Pulse

**Firebase-style reactive queries and live sync, native to C#/.NET, starting with MongoDB.**

Pulse is a .NET package suite that lets clients subscribe to a query (collection + filter), receive an initial snapshot of matching documents, and then stay in sync as the database changes — MongoDB change streams on the server, delivered to clients over SignalR.

Target framework: **.NET 8** (LTS, MAUI-compatible) for all projects.

> **Status: v0.1 MVP — in progress.** Steps 0–6 of the [build order](#build-status) are done: scaffold, `Pulse.Abstractions`, `Pulse.Mongo`'s change-stream `WatchAsync` (verified end-to-end against a real Testcontainers Mongo replica set), `Pulse.Server`'s `PulseHub` broadcast with shared per-source watches and `AddMongoSource`, server-side filter matching (`DictionaryFilterMatcher`), gap-free snapshot delivery on subscribe (`GetSnapshotAsync` with watch-first as-of token capture), and resume-token persistence (`IResumeTokenStore`, in-memory + file-based, with stale-token resync verified across a server restart). API shapes below are the design contract and will land in that order.

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
- [Important caveats](#important-caveats)
- [Development](#development)
- [Testing](#testing)
- [Build status](#build-status)
- [Roadmap / explicitly deferred](#roadmap--explicitly-deferred)

---

## Why Pulse

Many C# apps end up polling the database to reflect state changes in the UI, or hand-rolling a pub/sub layer that drifts from the data. Pulse treats the database as the source of truth and pushes changes out, so a client's view stays correct without a query being re-run on every change.

Pulse is designed to be **provider-agnostic from day one**: the core abstractions carry no MongoDB concepts, so Postgres and SQL Server providers can be added without touching the server hub, the subscription registry, or the client SDK.

## Features

- **Filter-based subscriptions** — subscribe to a source (collection) with a filter expression; only matching change events are delivered.
- **Snapshot + live diffs** — on subscribe, the client receives the current matching documents, then incremental changes.
- **Resume-token persistence** — a server restart or brief outage does not silently drop events.
- **Automatic reconnect + resubscribe** — handled by the client SDK.
- **Provider abstraction** — `IChangeSource` / `IFilterMatcher` keep the database provider swappable.
- **Live list semantics** — updates that move a document in or out of a filter's match set are surfaced correctly (synthetic insert/remove to each affected subscriber).

## Non-goals for v0.1

These are explicitly deferred and **not built** in v0.1:

- Offline-first sync / conflict resolution on mobile.
- Auth/authorization framework (v0.1 exposes the `IPulseAuthorizer` seam only; the default allows everything — see [Important caveats](#important-caveats)).
- Multi-database support (Mongo only for v0.1; interfaces must not leak Mongo-specific concepts).
- Horizontal scale-out across multiple server instances.
- Arbitrary pub/sub / message-queue behavior — this is DB-state-change subscription only.

---

## How it works

```
  Mongo (change streams)
        │  IChangeSource.WatchAsync / GetSnapshotAsync
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
  Pulse.Client/              PulseClient SDK (ASP.NET, desktop, MAUI)
/tests
  Pulse.Server.Tests/
  Pulse.Mongo.Tests/         real Mongo via Testcontainers
  Pulse.Client.Tests/
  Pulse.Integration.Tests/   end-to-end: Testcontainers Mongo + in-process SignalR + real PulseClient
/samples
  Pulse.Sample.Server/       ASP.NET Core minimal API host wiring Pulse in
  Pulse.Sample.MauiClient/   MAUI app (deferred until build step 10)
```

NuGet package IDs: `Pulse.Abstractions`, `Pulse.Server`, `Pulse.Mongo`, `Pulse.Client`.

## Filter DSL

Subscriptions use a small, provider-neutral expression tree — **not** raw MongoDB query syntax — so it maps cleanly onto both a BSON document (Mongo) and a SQL row (Postgres/SQL Server later).

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

builder.Services
    .AddPulse(options => { /* registry, matcher, resume-token store */ })
    .AddMongoSource(mongoConnectionString, databaseName);

var app = builder.Build();
app.MapHub<PulseHub>("/pulse");
app.Run();
```

`AddPulse()` is generic (hub, registry, matcher); `AddMongoSource()` is provider-specific. A future `.AddPostgresSource()` slots in without changing `AddPulse()`.

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
- [Docker](https://www.docker.com/products/docker-desktop/) (Docker Desktop or any Docker daemon) — required for the Testcontainers-based Mongo tests
- A shell with `dotnet` on `PATH` (Homebrew `dotnet@8` is keg-only: `export PATH="/usr/local/opt/dotnet@8/bin:$PATH"`)

Commands:

```bash
dotnet build Pulse.sln          # build everything
dotnet test Pulse.sln           # run all test suites (spins up Mongo via Testcontainers)
dotnet run --project samples/Pulse.Sample.Server   # run the sample server
```

Solution-wide settings (target framework, nullability, lang version) live in `Directory.Build.props`; the SDK version is pinned in `global.json`.

## Testing

- `Pulse.Abstractions` — `FilterExpr` JSON round-trip (serialize → deserialize → equal), including nested `And` / `Or` / `Not`.
- `Pulse.Server` — `DictionaryFilterMatcher` against a table of documents/filters/expected results (all operators, nested paths); registry match-transition logic with a mocked `IChangeSource`.
- `Pulse.Mongo` — a real Mongo instance via `Testcontainers.MongoDb` (change streams are not mocked): insert/update/delete detection, resume-after-token continuation, invalid-token exception path, snapshot-then-watch gap-free behavior.
- `Pulse.Client` — reconnect/resubscribe against an in-process test SignalR server.
- `Pulse.Integration.Tests` — end-to-end: real Mongo + in-process SignalR hub + real `PulseClient`; subscribe, snapshot, mutate Mongo directly, assert the change arrives client-side; kill and restart the connection and assert resubscribe.

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
| 7 | `Pulse.Client` basic `Subscribe<T>` | Pending |
| 8 | Client reconnect/resubscribe | Pending |
| 9 | Match-transition logic | Pending |
| 10 | `Pulse.Sample.MauiClient` (MAUI list bound to `Current`) | Pending |

## Roadmap / explicitly deferred

- **Horizontal scale-out** — multiple server instances sharing subscription state. SignalR already supports a Redis backplane for connection fan-out; filter state and match-transition tracking would need to be shared/sharded too. The registry is structured so this is a non-breaking addition.
- **Postgres / SQL Server providers** — additive via `IChangeSource` / `IFilterMatcher` / `IResumeTokenStore`.
- **Offline queue / conflict resolution** for mobile clients.
- **Row-level authorization** beyond the `IPulseAuthorizer` seam.
- **Richer filter DSL** — array-element matching, full-text search, etc. (v2).
