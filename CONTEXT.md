# Pulse

Reactive live-query sync for C#/.NET: a client subscribes to a logical source with a filter, receives an initial snapshot, then stays in sync as the database changes via SignalR.

## Language

**Source**:
A logical collection or table name a subscription targets (e.g. `orders` or `public.orders`). Provider-neutral; not yet qualified.

_Avoid_: collection, table, topic, channel, stream

**Resolved Source**:
A provider-qualified source after `schema.table` normalization (e.g. `public.orders` for Postgres, `dbo.orders` for SQL Server). Stable key for `ProviderIdFor`.

_Avoid_: qualified name, physical source

**ProviderId**:
A stable provider-qualified identifier for a source (e.g. `postgres:public.orders`, `mongo:mydb.orders`, `sqlserver:dbo.orders`). Carried in `ResumeToken.ProviderId` so tokens are never misinterpreted across sources.

_Avoid_: provider key, source id

**ChangeEvent**:
An immutable state change to a single document/row in a source, with `ChangeKind`, `DocumentId`, `FullDocument`, `UpdatedFields`, `ResumeToken`, `Timestamp`.

_Avoid_: event, message, notification, delta

**ChangeKind**:
One of `Insert`, `Update`, `Replace`, `Delete`. Mongo emits all four; Postgres and SQL Server emit `Insert`/`Update`/`Delete` only.

_Avoid_: operation type, op

**ResumeToken**:
An opaque provider-issued marker (`ProviderId` + `Opaque` bytes) marking the position after a change. Used to resume watching without gaps; stale tokens throw `ResumeTokenInvalidException`.

_Avoid_: cursor, offset, sequence, checkpoint, position (unless qualified as token position)

**IChangeSource**:
The seam that abstracts a database's change delivery (`WatchAsync`, `GetSnapshotAsync`, `ProviderIdFor`). Mongo change streams, Postgres triggers + `pulse._changes` log, SQL Server change tracking are adapters behind it.

_Avoid_: provider, source adapter (as noun for the interface)

**SharedWatch**:
A per-source poller shared across all subscriptions without a resume token. Fans out each `ChangeEvent` to multiple subscribers; resilient with capped exponential backoff and resume from last delivered token.

_Avoid_: shared subscription, broadcaster

**PrivateWatch**:
A dedicated watch resumed from an explicit `ResumeToken`. Not shareable; its position cannot be conflated with the shared watch's floor. Fails fast on stale token.

_Avoid_: resumed watch, dedicated subscription

**SubscriptionFilter**:
A provider-neutral `FilterExpr` tree (`FieldCompare` / `And` / `Or` / `Not` with `CompareOp`) applied to a source. Translated per provider for snapshot queries and evaluated in-memory for match-transition fanout.

_Avoid_: query, predicate, where clause (as domain term)

**Pruning Floor**:
The minimum `ResumeToken` position across active watchers for a source. Postgres change-log rows below the floor are safe to delete; a source with no active watcher is never pruned so persisted resume tokens survive restarts.

_Avoid_: watermark, low water mark, GC threshold

**Snapshot (AsOf)**:
The `GetSnapshotAsync` result: matching documents plus the `ResumeToken` marking the "as of" point captured _before_ the query (watch-first), so live watching picks up without gap or duplicate.

_Avoid_: initial load, seed, bootstrap snapshot (as domain term)
