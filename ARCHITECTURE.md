# Architecture

How the platform is built and why: what each component owns, how work splits across MongoDB, Elasticsearch, and Redis, what the queue guarantees, how the system fails, and what changes at scale. Setup and endpoints are in the [README](README.md).

## System diagram

```
                     writes                                reads
        ┌──────────────────────────────┐   ┌────────────────────────────────┐

 client ──POST /events──► API ────────────► SimulatedQueue ◄──receive_batch── worker
          202 {event_id}  │ validate        │ bounded (10k)                  │
          503 if full     │ rate limit      │ backoff redelivery             │ Mongo first,
                          │                 │ ack / nack / reject            │ then ES with
                          │                 └──► DLQ ◄── GET /admin/dlq      │ only what
                          │                                                  │ Mongo accepted
 client ◄─────────────────┤                                                  │
                          │                                            ┌─────┴─────┐
   GET /events ───────────┼──────────────────────────────────────► MongoDB ◄──────┘
   GET /events/stats ─────┼──────────────────────────────────────► (source of truth,
                          │                                          aggregations)
   GET /events/search ────┼──────────────────────────────────────► Elasticsearch
                          │                                          (full-text only)
   GET /events/stats/─────┼───► Redis (cache-aside, TTL) ──miss───► MongoDB
       realtime           │      also: rate-limit counters
```

The API and worker share a process on purpose. The queue lives in-process, so buffered events die with the process and the two can't scale apart. The queue and scaling sections cover the tradeoffs and what SQS changes.

## Component responsibilities

### API (`app/api`)

The API layer owns the HTTP surface: validation, error handling, and turning store results into responses. Ingest validates shape and domain rules and then enqueues. Read routes go through the repository, search index, or cache interfaces. Nothing in `app/api` holds a Mongo, ES, or Redis client directly.

A 422 always means the client was wrong, including unknown fields and query params. A 503 names a failed dependency class (`storage_unavailable`, `search_unavailable`). A 500 is our bug, logged in full and never leaked to the response.

Unknown inputs are rejected everywhere. Without that, a query that doesn't filter the way the caller intended, or an event with a misspelled metadata key, would fail silently and hand back a response that looks correct but isn't.

### Queue (`app/queue`)

The queue owns the handoff from ingest to processing. It lets POST /events return 202 without waiting on the worker or Mongo. It also owns the at-least-once bookkeeping that makes redelivery safe (receive counts, backoff scheduling, and DLQ redrive at the retry ceiling), over messages it treats as opaque. It never validates, deserializes, or writes to a store. That work belongs to the worker.

It's also bounded. `send` on a full queue raises, and the API turns that into the 503 backpressure response.

### Worker (`app/worker`)

The worker owns processing and writing. It consumes events from the queue, deserializes them, stamps `ingested_at`, and writes both stores. Per-message outcomes come from each store's per-item bulk results, so one bad document fails on its own.

### MongoDB (`app/storage/mongo.py`)

MongoDB is the source of truth. It owns event storage, the filtered list query, the `/stats` aggregation, and the realtime summary.

### Elasticsearch (`app/storage/es.py`)

Elasticsearch owns full-text search and nothing else. It's populated only with events Mongo has already accepted.

### Redis (`app/cache`, rate limiter in `app/core/middleware.py`)

Redis is an optimization and coordination point. It holds realtime snapshots (cache-aside with a TTL) and rate-limit counters.

### Rate limiting

Rate limiting is fixed-window counting in Redis, applied only to `/events*` routes. Writes get 120/min per client IP and reads get 600/min. A write fans out to the queue, the worker, and two stores, while a read is a single bounded query.

Health checks are exempt, because a 429'd liveness probe is a false "unhealthy" that flaps container state. Admin routes are exempt too, since their real access control is network isolation.

The limiter fails open on Redis errors. Failing closed would turn a Redis hiccup into a total ingestion outage just to protect a quota, which is backwards when dropped events are the expensive failure.

Rate limiting is abuse hygiene, not the real overload defense. That job belongs to the bounded queue and its 503. Counters live in shared Redis, so N replicas enforce one global budget per client, and scaling the API tier doesn't grow an abuser's allowance.

## Queue design and SQS drop-in

The queue is in-process but modeled on SQS.

### What it guarantees

- At-least-once while the process lives. A received message isn't gone until it's explicitly acked. Failure paths reject, and nacked messages redeliver with backoff and an incremented receive count.
- At-most-once across restarts. The queue lives in memory, so everything buffered dies with the process.
- Bounded depth (default 10,000). `send` on a full queue raises `QueueFullError` → 503 + `Retry-After`. Under a sustained outage the queue fills, ingestion backpressures, and memory stays bounded, in that order.
- Backoff with jitter on redelivery, so a struggling dependency isn't hammered in lockstep.
- No ordering guarantee. Redelivered messages re-enter behind ready ones, exactly as SQS standard queues reorder.

Duplicate delivery is the flip side of at-least-once, but because both stores are idempotent on `event_id`, a redelivered message just re-upserts and re-indexes the same document.

### Why there is no visibility timeout like SQS

SQS has a visibility timeout because its queue outlives its consumers. One consumer takes a message, crashes, and the message reappears for another. Here the queue and its only consumer share a process and die together, so if the process crashes, every message is lost regardless of visibility.

### The three failure verbs

The queue offers three ways for a consumer to settle a message:

- `ack` deletes on success.
- `nack` schedules backoff redelivery. At the max receive-count (default 5), nack routes to the DLQ.
- `reject` goes straight to the DLQ. This mirrors the SQS pattern for permanent failures, which sends the message to the DLQ and deletes it from the source. The worker uses it for poison and permanent store errors, where retrying an unrecoverable error only delays the DLQ signal.

### Divergences from real SQS

| This queue | Real SQS | Why deliberate |
|---|---|---|
| Nack schedules exponential backoff | Constant visibility timeout; consumers back off via `ChangeMessageVisibility` | Same behavior, simulated from the queue side; the worker loop stays clean |
| No receipt handles | Fresh handle per receive | Safe only because there's no visibility timeout, so the two omissions depend on each other |
| Bounded, `QueueFullError` | Effectively unbounded | Backpressure toward the 503 path; also the memory bound during outages |
| Explicit `reject` verb | No DLQ API; redrive policy only | Models the documented consumer pattern for permanent failures |

### Drop-in real SQS

The protocol maps one-to-one onto boto3: `send`→`send_message`, `receive_batch`→`receive_message` (the 10-cap is already enforced), `ack`→`delete_message`, `nack`→`change_message_visibility`, `reject`→`send_message` to the DLQ + `delete_message`.

The payoff is that a crash or deploy no longer loses events. SQS keeps those messages durable, so a restarted worker resumes with exactly what was in flight. It also lets the worker and API scale independently.

Nothing else changes. Both stores are already idempotent under redelivery, and SQS's occasional duplicates land on the same upserts. Redelivery becomes timeout-driven, which moves backoff to the consumer, where SQS expects it.

`GET /admin/dlq` gets deleted. It exists only because an in-process queue has no other inspection surface, and it's typed against `SimulatedQueue`.

## Storage rationale

MongoDB holds the events and answers every exact question: filtered lists, `/stats`, the realtime summary. It's the read-your-writes store once the worker has processed an event.

Documents use `_id = event_id`, which handles dedup, idempotent redelivery, and safe retries. Elasticsearch indexing uses this `event_id` so writes to it are idempotent.

Elasticsearch does one thing, relevance-scored full-text search over metadata. It indexes on the same `event_id` as Mongo, so those writes are idempotent too. Five facets on `/search` cost nothing at index time in Elasticsearch, but the same five on `/stats` would each need a Mongo index and a matching query pattern.

Redis is the third store, though not a system of record. It caches the realtime summary and holds rate-limit counters, and every path through it degrades gracefully to the store it fronts.

Writes go to MongoDB first, then Elasticsearch, and ES only ever gets the events Mongo accepted. The search index can never run ahead of the source of truth. When Mongo accepts an event but ES fails, the message nacks and redelivers. The Mongo re-upsert is a harmless no-op, and ES retries.

There's no fallback when ES is down. A regex scan in Mongo pretending to be full-text search would rank and match results differently from ES, and clients would learn not to trust the endpoint. So search fails with 503 `search_unavailable` while ingestion and Mongo reads keep working.

Timestamps are UTC, normalized once at ingestion (naive treated as UTC, aware converted). Without that, one event renders as different strings from `/events` (Mongo codec normalizes) and `/search` (ES stores what it was given).

### MongoDB indexing

Four indexes, each ending with the sort:

```
{event_type: 1, timestamp: -1, _id: -1}
{user_id: 1, timestamp: -1, _id: -1}
{source_url: 1, timestamp: -1, _id: -1}
{timestamp: -1, _id: -1}
```

Every list sorts by `(timestamp desc, _id desc)`, so each index is the filter then the sort key. Mongo can traverse it in order and never sorts in memory.

The `_id` is the pagination tiebreaker for documents that share a timestamp, so events don't shuffle between pages across requests.

The bare `{timestamp, _id}` index is for unfiltered lists, date-range-only queries, and the realtime `$gte` scan.

Indexes not added:

- No compound indexes for filter combinations (`user_id + event_type`, etc.). Each index taxes every write, and combinations grow combinatorially.
- No index on `metadata` and no text index. Its keys are client-controlled and unbounded, no API query filters on it, and Elasticsearch already handles full-text over metadata.

Deep pagination is capped for now, but the production answer is cursor pagination on `(timestamp, _id)`, which is already the sort key in every index.

### Elasticsearch mapping

The mapping is static and closed:

```
dynamic: false
event_type:  keyword
timestamp:   date
user_id:     keyword
source_url:  keyword  (+ .text subfield for fragment matching)
search_text: text
metadata:    object, dynamic: false
```

Full-text over a stringified metadata comes from `search_text`, a single field built in code.

Queries are a `multi_match` over `search_text` and `source_url.text`, with facets for filters. The full metadata object still lives in `_source`. Metadata keys come from whoever POSTs events, so dynamic mapping each key would allow all clients to fight for control over the schema. Three failure modes follow:

- Type conflict. The first `{"amount": 42}` fixes the field as `long`, and a later `{"amount": "42.50"}` throws a mapper exception. Which document fails depends on arrival order.
- Silent coercion. Once `amount` is `long`, `41.99` indexes as 41 with no error.
- Mapping explosion. Every distinct key becomes permanent cluster state, removable only by a reindex.

`dynamic: false` with `search_text` built in code makes all three impossible rather than merely mitigated. It also helps ingest, since novel keys no longer trigger synchronous cluster-state updates mid-bulk.

The tradeoffs are there are no per-field metadata queries (`metadata.browser: firefox`) and no numeric or date ranges over metadata, and changing what's searchable takes a code change plus a reindex rather than a mapping tweak.

The analyzer is `standard`, not `english`. Metadata values are device names, campaign labels, and product ids rather than prose. `iOS` and `ios` should match, which lowercasing handles, while stemming would corrupt tokens like `utm_source` for no gain.

Shards and replicas (1 and 0) are single-node topology facts, set at creation. Search is near-real-time by contract, with a roughly 1 s refresh, so tests call an explicit `refresh()` instead of sleeping.

### Caching

The realtime stats endpoint is cache-aside in Redis, ie. read the snapshot, on miss compute from MongoDB, store with a TTL, return. Four decisions carry the strategy.

The TTL is a staleness budget. The default 30 s (`REALTIME_CACHE_TTL`) answers two questions at once: how stale a "realtime" summary may be, and how often aggregations may hit the primary store. The response embeds `computed_at`, so callers can see the staleness instead of assuming it. The value is an env knob, but the reasoning behind choosing it is the point.

The window is an allowlist (60, 300, 900 s), which is what keeps the load math honest. Each window is its own key and its own aggregation on a miss, so the worst case is `windows * (1/TTL)`, at most 6 aggregations a minute regardless of request volume. A free-form window would break that, because every distinct value becomes a fresh key and `?window=301,302,303…` turns into trivial cache-busting. The valid set is API contract, not config, and adding a fourth window is one line.

There's no write-path invalidation, on purpose. Events arrive continuously, so invalidating on every write means the cache never survives a second under load, and it degrades into an uncached aggregation with extra steps. Invalidate-on-write suits mutable entities, but this is an append-only stream feeding an approximate endpoint, where bounded, disclosed staleness is the right model. The TTL is the invalidation strategy, and callers who need exact counts use `/events/stats`.

Concurrent misses on one window are single-flighted with a per-window `asyncio.Lock` and a re-read after acquiring, so one recompute serves all waiters in the process. A distributed lock (Redis `SET NX`) is deliberately unbuilt, since its failure modes aren't worth taking on to save a ~10 ms aggregation when the in-process miss is the only realistic stampede.

Degradation follows the Redis rule that the cache can never cause a 500. Read errors and corrupt payloads count as misses that the recompute overwrites, and a failed write after a good compute logs the error and returns the result anyway. Compute errors still propagate, so Mongo being down surfaces as an honest 503 instead of something the cache swallows. A `v1` key namespace versions the payload shape so a schema change can't mis-parse a stale entry.

Under higher write volume, the read-side recompute flips to write-side counters. The worker `INCR`s per-type, per-window keys as it processes, and the endpoint reads those counters instead of aggregating, so reads become O(1) and staleness turns into pipeline lag rather than TTL. It isn't built at this scale, because it adds a second write path and a worker Redis dependency to save an aggregation that already runs only six times a minute. The trigger is how Redis-down behaves under real traffic, and once per-request aggregation stops being acceptable, this (or request coalescing) is the move.

## Failure modes

### Failure scenarios

**MongoDB is unavailable.** Ingestion is unaffected. `POST /events` keeps returning 202 and the queue absorbs the load, because the request path never touches Mongo.

Worker writes fail as transient errors, so they nack and redeliver. If the outage outlasts the retry budget, the events land in the DLQ at with their error attached.

Reads (`GET /events`, `/stats`) return 503 `storage_unavailable`. Realtime serves cached snapshots until the TTL expires, then returns 503. Degradation stays per-dependency instead of taking everything down.

**The worker crashes mid-batch.** This splits into two cases.

Task death is an unhandled exception in a batch. The `finally` block nacks every unsettled message with the error, nothing leaks, and the loop survives to the next batch. If the worker itself dies, its done and callback logs the crash immediately instead of stalling silently.

Process death causes everything in memory to be lost and where a durable queue like SQS is valuable.

Shutdown is the controlled version of the same thing. The in-flight batch is shielded so that every message reaches ack, nack, or reject before the task exits.

### Per-dependency degradation

| Dependency down | Write path (`POST /events`) | Read paths | Detection |
|---|---|---|---|
| MongoDB | 202 continues; worker nacks → backoff → DLQ if the outage outlasts retries | `/events`, `/stats`: 503 `storage_unavailable`; realtime: cached until TTL, then 503 | `/health/ready` names it; worker logs |
| Elasticsearch | 202 continues; Mongo writes succeed; ES nacks and catches up on redelivery | `/search`: 503 `search_unavailable`; everything else unaffected | `/health/ready`; worker logs |
| Redis | Unaffected; rate limiter fails open (logged) | Realtime recomputes from Mongo per request, single-flighted, ≤1 s slower; everything else unaffected | `/health/ready` (Redis-down is deliberately loud there) |
| Queue full | 503 + `Retry-After` | Unaffected | The 503s are the signal; depth included in the message |

`/health/ready` returns the per-dependency map and a 503 when degraded. An orchestrator uses it to route, and an operator uses it to diagnose.

### Failure classification in the worker

Not all store errors deserve retries. Each per-item error is classified where the knowledge lives, in the storage layer:

| Class | Examples | Path | Where it lands |
|---|---|---|---|
| Poison | Body fails validation; missing `event_id` | `reject` immediately | DLQ, receive count 1, error prefixed `poison:` |
| Permanent | Mongo per-document write errors outside pymongo's retryable-code set; ES bulk item 4xx except 429 | `reject` immediately | DLQ with the store's error, prefixed `mongo:` / `es:` |
| Transient | Connection failures; ES 429/5xx; retryable Mongo codes | `nack` → backoff redelivery | Redelivered; DLQ after 5 receives |
| Unknown mid-batch exception | Anything else | `finally` nacks unsettled messages | Redelivered with the real error attached |

Misclassification is biased on purpose. An unknown Mongo code defaults to permanent and goes to the DLQ, where it's visible, rather than five silent retries that only delay the same outcome.

## Scaling considerations

### What breaks first at 10x

The single process breaks first. API and worker share one event loop, so ingest acceptance and pipeline throughput contend for the same CPU, and the buffer between them is just process memory.

At 10x sustained, the worker falls behind, the queue fills, and ingestion backpressures with 503s. That's the designed behavior, with bounded memory and loud degradation.

Scaling horizontally exposes the major constraint of the single process design. The app scales to N replicas cleanly in most respects. Rate limits stay globally correct through the shared-Redis counters, and the cache stampede stays bounded at ≤N recomputes per window per TTL. What doesn't scale is the queue. N replicas means N independent in-process queues, and each replica's buffered events die with it.

So the first 10x move is swapping the queue. A durable broker (aka SQS) separates durability from the process, makes crash recovery real, and lets the API and worker fleet scale independently. Everything downstream is already shaped for it. Writes are idempotent, failures are classified, and the DLQ maps onto redrive policies.

### The stores at 10x

- **MongoDB** takes 10x writes through the same four indexes. Past vertical scaling the answer is a shard key, and event data shards naturally on a hashed `_id`. `/stats` degrades with volume before writes do. The standard fix is pre-aggregation and materialized rollups per bucket.
- **Elasticsearch** runs a single shard with no replicas, which is correct for one node and wrong for production. The multi-tenant version also wants per-tenant indices, which simplifies retention and deletion.
- **Redis** is nowhere near a limit, holding a few snapshot keys plus one counter per active client per window. The real issue at scale is that cache and control data share an instance and an eviction policy. The compose file runs `allkeys-lru`, because under `noeviction` a full Redis errors on every `INCR` and fails the rate limiter, whereas LRU only evicts a few counters and resets a few clients, so the blast radius stays bounded instead of taking out all rate limit counts. At scale the two split into separate instances so eviction on one can't degrade the other.

## What I'd do differently

In production, or with more time, roughly in priority order:

- **Observability beyond logs.** The pieces exist, but nothing exports them. `ingested_at - timestamp` is per-event lag, queue depth and DLQ size are one property away, and request ids thread every log line. Queue depth, DLQ size, and pipeline lag are the three dashboard numbers, with an alert on DLQ growth. Request ids also stop at the queue boundary, since worker logs run outside request context, so production would carry a trace id inside the message and let one id span ingest through worker.
- **Authentication and authorization.**
- **Metadata guardrails at the API boundary.** The ES mapping already stops client metadata from damaging the schema, but a per-event key-count cap at ingest would make pathological senders fail fast with a 422 that names the problem.
- **Cursor pagination.**
- **A bulk ingest path.** The seed script surfaced this, since bulk loading through an organic-traffic rate limit takes minutes. The production answer is a batch endpoint or an authenticated exemption.
- **Client idempotency keys.** Event ids are server-assigned, because accepting client ids would let one client overwrite another's events, so replaying a bulk load double-ingests. An idempotency-key header is the standard fix, and it's a different concept from `event_id`.
- **Update the realtime counts as events are written, not on read.** The worker keeps a running per-type count in Redis instead of the endpoint re-aggregating MongoDB on each miss. Worth it once realtime traffic makes those aggregations too expensive.
- **Per-tenant Elasticsearch indices** in the multi-tenant version, for schema isolation, retention, and deletion.
- **Worker-task liveness in `/health/ready`.** A crashed worker logs, but readiness doesn't reflect it.
