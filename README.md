# Starly

A distributed event processing platform for high-volume web events. Like a starling murmuration, thousands of birds each flying on their own but read as one flock, Starly takes a flood of independent events and makes them queryable in aggregate: FastAPI ingestion in front of an in-process SQS-style queue, a background worker that writes to MongoDB (source of truth) and Elasticsearch (full-text search), and Redis serving cached realtime stats.

```
POST /events → validate → queue → worker → MongoDB → GET /events, /events/stats
                                         → Elasticsearch → GET /events/search
                                  Redis (cache-aside) → GET /events/stats/realtime
```

More detail on the architecture, design, and decisions for a project of this scope vs production is found in [ARCHITECTURE.md](ARCHITECTURE.md).

## Quick start

### Prerequisites

- Docker CLI
- Your favorite terminal

Navigate to the project directory and use the makefile command listed below.

### Makefile Commands

```sh
make up                   # build the app image, start mongo/elasticsearch/redis, wait until healthy
make down                 # stops the container
make down-reset-volumes   # removes volumes for a fresh start

make test                 # run full test pipeline (unit and integration tests)
make seed                 # push ~1000 sample events through the ingestion pipeline

make lint                 # run ruff lint and format check - does not apply changes
make fmt                  # run ruff lint and formatter - applies changes

### Example API Calls

```sh
curl 'localhost:8000/events?type=conversion&limit=3'
curl 'localhost:8000/events/stats?bucket=day'
curl 'localhost:8000/events/search?q=squirtle'
curl 'localhost:8000/events/stats/realtime'
```

### OpenAPI docs

Swagger UI: [http://localhost:8000/docs](http://localhost:8000/docs)

### Notes

- Docker compose commands run in the root of the project directory are also valid.
- [`docker-compose.yml`](docker-compose.yml) raises the event write rate limit to 5000/min to speed up `make seed`. The default in the [app configuration](src/app/core/config.py) is 120/min.
- `make seed` supports `ARGS="count=20000 concurrency=200"` to modify the number of events and workers used to seed the database.

## Endpoints

### General notes

- Errors use a standard structure:\
`{"error": {"code": ..., "message": ..., "details": ...}}`.
- Unknown query parameters and unknown body fields are rejected with 422 rather than ignored.
- Timestamps are UTC everywhere. Naive input is treated as UTC; timezone aware input is converted.
- Every response carries `X-Request-ID`.
- Rate limits, per client IP - return 429 with `Retry-After` header if hit:
  - 120 writes/min for `POST /events`
  - 600 reads/min across `GET /events*`
- Store outages return 503 with a code naming the dependency class (ie. `storage_unavailable`, `search_unavailable`); Redis being down will recompute from Mongo instead of failing the request.

### POST /events

Validates and enqueues an event for async processing. Returns `202 {"event_id": ..., "status": "queued"}`. If the queue is full (10,000 messages), the response is 503 with `Retry-After`.

| Field | Rules |
|---|---|
| `event_type` | required; trimmed and lowercased |
| `timestamp` | required; ISO 8601; naive treated as UTC; at most 300 s in the future |
| `user_id` | required |
| `source_url` | required |
| `metadata` | optional object, free-form keys, at most 16 KB |

Unknown top-level fields return 422.

`event_id`: ids are server-assigned (UUIDv7) because they are the dedup identity and prevents conflicting ids from different event sources.

```sh
curl -X POST localhost:8000/events -H 'Content-Type: application/json' -d '{
  "event_type": "pageview",
  "timestamp": "2026-07-10T12:00:00Z",
  "user_id": "trainer_red_0021",
  "source_url": "https://www.pokemart.example/",
  "metadata": {"browser": "Safari", "device": "mobile"}
}'
```

### GET /events

Filtered event list from MongoDB, sorted by timestamp descending. Returns `{"events": [...]}`.

| Param | Rules |
|---|---|
| `type` | exact event type |
| `user_id` | exact match |
| `source_url` | exact match |
| `from`, `to` | ISO 8601 datetime range |
| `limit` | 1-500, default 50 |
| `offset` | 0-10,000, default 0 |

```sh
curl 'localhost:8000/events?type=conversion&limit=3'
curl 'localhost:8000/events?user_id=trainer_red_0001&from=2026-07-01T00:00:00Z'
curl 'localhost:8000/events?source_url=https://shop.pokemart.example/products/master-ball'
```

### GET /events/stats

MongoDB aggregation: counts grouped by event type per time bucket.\
Buckets are UTC and weeks start Monday.

Returns `{"stats": [{"event_type", "bucket_start", "count"}]}`.\

| Param | Rules |
|---|---|
| `bucket` | required: `hour`, `day`, or `week` |
| `type` | exact event type |
| `from`, `to` | ISO 8601 datetime range |

```sh
curl 'localhost:8000/events/stats?bucket=day'
curl 'localhost:8000/events/stats?bucket=week&type=conversion'
curl 'localhost:8000/events/stats?bucket=hour&from=2026-07-09T00:00:00Z&to=2026-07-10T00:00:00Z'
```

`/stats` deliberately has no `user_id` or `source_url` facet; each aggregation filter is an index commitment, and the reasoning is in ARCHITECTURE.md. Sending one returns 422 rather than being silently ignored.

### GET /events/search

Full-text search over event metadata (and source URL fragments) via Elasticsearch, relevance-ordered.\
Search is near-real-time: new events become searchable within about a second of processing.

Returns `{"events": [...], "total": n}`, where `total` is the full match count even when results are capped.\

| Param | Rules |
|---|---|
| `q` | required; 1-1024 characters |
| `type`, `user_id`, `source_url` | exact-match facets |
| `from`, `to` | ISO 8601 datetime range |
| `limit` | 1-100, default 50 |

```sh
curl 'localhost:8000/events/search?q=pikachu'
curl 'localhost:8000/events/search?q=team-rocket-retargeting&type=conversion'
curl 'localhost:8000/events/search?q=safari+zone&user_id=trainer_blue_0002'
```

If Elasticsearch is unreachable this endpoint returns 503 `search_unavailable`.

### GET /events/stats/realtime

Lightweight summary served from Redis.\
Configurable TTL - default 30s - `realtime_cache_ttl` parameter in [config](src/app/core/config.py)

Returns `{"window_seconds", "total", "counts_by_type", "computed_at"}`.

| Param | Rules |
|---|---|
| `window` | `60`, `300`, or `900` seconds; default 300 |

The window values are a fixed allowlist, not free-form, because every distinct window is its own cache key and its own MongoDB aggregation.\
Two calls inside the TTL return the same snapshot and `computed_at` provides the timestamp for the last aggregation.

```sh
curl 'localhost:8000/events/stats/realtime'
curl 'localhost:8000/events/stats/realtime?window=60'
```

### Operational endpoints

```sh
curl localhost:8000/health          # liveness: process is up
curl localhost:8000/health/ready    # readiness: per-dependency status for mongo/es/redis; 503 when degraded
curl localhost:8000/admin/dlq       # read-only dead-letter queue inspection: {entries, total}
```

`/admin/dlq` exists because the queue is in-process and has no other inspection surface. It is unauthenticated, but production would put it behind auth or a separate internal port.

### Reproducible error handling calls

```sh
curl 'localhost:8000/events?event_type=pageview'           # 422: unknown param (it's `type`)
curl 'localhost:8000/events/stats?bucket=month'            # 422: bucket enum
curl 'localhost:8000/events/stats/realtime?window=120'     # 422: window allowlist
```

## Configuration

Environment variables read at process start. There are no config values that support hot-reloads.\

The full list is in [config.py](src/app/core/config.py).

| Variable | Default | What it controls |
|---|---|---|
| `MONGO_URL` / `ES_URL` / `REDIS_URL` | localhost URLs | store connections (compose overrides these) |
| `REALTIME_CACHE_TTL` | `30` | seconds a realtime snapshot is served from Redis |
| `QUEUE_MAX_DEPTH` | `10000` | queue bound; full queue means 503 at ingest |
| `RETRY_BASE_DELAY` | `1.0` | base for exponential redelivery backoff |
| `MAX_RECEIVE_COUNT` | `5` | deliveries before a message is dead-lettered |
| `WORKER_BATCH_SIZE` | `10` | messages per receive; capped at 10 to match the SQS batch limit |
| `RATE_LIMIT_WRITES_PER_WINDOW` | `120` | write budget per client IP per window |
| `RATE_LIMIT_READS_PER_WINDOW` | `600` | read budget per client IP per window |
| `RATE_LIMIT_WINDOW_SECONDS` | `60` | rate limit window length |
| `METADATA_MAX_BYTES` | `16384` | per-event metadata size cap |
| `TIMESTAMP_MAX_FUTURE_SKEW_SECONDS` | `300` | how far in the future a timestamp may be |
| `LOG_LEVEL` | `INFO` | app log level |

## Testing

```sh
make test-unit           # fast, no services required (default pytest run)
make test-integration    # requires `make up` first; runs tests marked `integration`
make test                # both
```

Unit tests cover the domain model, queue semantics, worker orchestration, every API route, the cache, and the rate limiter. Worker unit tests use the real `SimulatedQueue` with fake stores, so nack/DLQ assertions observe actual queue behavior instead of mock bookkeeping.

Integration tests run against the real MongoDB, Elasticsearch, and Redis from compose. They cover the full request lifecycles the assignment asks for, and then some:

- ingest → worker → `GET /events` returns the event and `/events/stats` buckets
- ingest with distinctive metadata → worker → `/events/search` finds it (and a type facet excludes it)
- a failing repository → retries with backoff → exhaustion → the event appears in `/admin/dlq` with its receive count and error origin
- the same event body delivered twice → exactly one document in MongoDB and one hit in Elasticsearch (dedup end to end)
- Redis unreachable → realtime stats still serve, computed from MongoDB

A few principles I held to throughout:

- Tests are written first and run to fail before implementation. The red phase caught a bug in a test fixture at least once in this project.
- Assert outcomes, not call counts. A retry test that asserts "called exactly 3 times" breaks when jittered redeliveries split a batch; asserting "both events eventually stored" doesn't.
- Unit tests use fakes behind the storage interfaces; anything that depends on real store behavior (index mappings, aggregations, bulk error shapes) is an integration test.
- No sleeps. Search tests call an explicit index refresh; time-dependent tests (backoff, rate limit windows) inject a fake clock and advance it.

With more time I would prioritize load testing the backpressure path (sustained volume against the bounded queue and rate limiter, which the current suite only exercises functionally), compose-level failure injection (killing a store container mid-run rather than pointing clients at closed ports), and raising WORKER_CONCURRENCY above 1 to load-test parallel queue drain.

## AI in My Workflow

### Which AI tools I used

Claude Code was the primary tool, used in three distinct modes: as a rubber duck for design decisions, as an implementer working test-first inside constraints I set, and as a reviewer with fresh context. I loaded domain skills (FastAPI, MongoDB, Elasticsearch, Redis patterns, Docker best practices) for the tasks, and used fresh-context subagents for review checkpoints so the reviewer wasn't graded on its own homework. I did not allow Claude to be in control of Git staging or commit commands. I reviewed the code myself at each the end of each task which allowed me to challenge decisions we had made during planning. I also used Gemini Pro and Codex via VS Code Copilot for second sets of eyes on some design decisions.

### Specific examples of how AI helped

Taking advatage of strong training data: the MongoDB and Elasticsearch storage layers are mostly standard CRUD, aggregation pipelines, and bulk indexing, so I had Claude write them along with their integration tests.

Review checkpoints caught bugs I would have shipped. Two final reviews, code standards and the assignment spec as a rubric found 15 issues, including the one that highlighted a case of me over-engineering the spec. The ES metadata mapping used dynamic mapping with a strings-only template, so an event with `{"amount": 42}` would fix that field's type forever and later senders of `"42.50"` would end up in the DLQ. The fix and what the spec call for was to stringify `metadata` and store it in a `search_text` field. Some other major issues fixed thanks to review process:

- an app-wide validation handler that would have turned a corrupt search-index document into a client-facing 422 on a later task's endpoint,
- a worker task that died silently and then broke shutdown cleanup
- 500 responses were missing the request-id header (the exception unwinds through the middleware, so the header line never ran).

I also used Claude for the domain research I would otherwise have sent a clarifying email on: whether marketing platforms enforce metadata schemas on senders. The answer (enforce with caps and guardrails, not contractually) prevented me from being blocked and waiting for an email response. I also verified this with Gemini.

### Where I pushed back on or corrected AI output, and why

- Queue design: Claude initially recommended a faithful SQS simulation with visibility timeouts and receipt handles. I pushed back that the bonus asks for design notes on a real SQS swap, not a reimplementation of SQS. Instead an in-process queue and its worker die together. The queue keeps the SQS semantics that transfer (explicit ack, nack with backoff redelivery, receive-count DLQ) and the omissions became the SQS design notes.
- Poison-pill handling: Claude recommended adding a `retryable=False` flag to nack. I dug into the SQS documentation to see which option is most SQS-native to find SQS has no nack at all, and the consumer pattern is send-to-DLQ plus delete, so a `reject()` call was added to the queue implementation.
- Timezone handling: a review flagged that naive datetimes were only handled correctly by a pymongo default, and rated it minor. I pushed back as it being minor because timezone-aware input with a non-UTC offset would render the same event with different timestamp strings from `/events` (Mongo normalizes) and `/events/search` (ES stores what it's given). The fix moved normalization at ingestion so every layer speaks UTC. The offset could be stored in a separate column if needed later.
- The realtime window: a design review flagged that the `window` parameter I'd approved was free-form, and since its value becomes the cache key, that's a cache-busting DoS. A client walking `?window=301, 302, 303...` never reuses a key, so every request misses the cache and runs a fresh Mongo aggregation, while Redis fills with single-use keys that only clear on TTL. I capped it to a three-value allowlist (60/300/900), which bounds the cache to three keys and holds the aggregation load to what the TTL allows no matter what clients send.

### How AI shaped my overall approach and development speed

I split the work deliberately, and the split moved as I learned where Claude could be trusted. I built the judgment-heavy core with myself (queue semantics, worker orchestration, the ingest contract), making every design decision, with AI arguing alternatives before delegating code and tests to an agent. The later ops tasks (health endpoints, Dockerfile, rate limiting, the seed script) I handed off almost 100%, tweaking them after built and manual testing and code review. While my AI use and opinion is constantly evolving right now my philosophy is delegate what has well-trodden answers and keep the parts where the tradeoffs define the system.

The speed didn't come mainly from typing. It came from every design decision getting stress-tested with multiple opinions before I wrote any code, at a depth I wouldn't reach alone under a deadline. Each task started with the plan re-verified in a fresh context against the assignment, and each significant choice came with counterarguments attached. Several sections of the architecture document exist because a question I asked led to a better answer or exposed a gap in the working design. About 5 days covered the whole assignment, bonus scope included, which I think would be faster than a small team could accomplish without AI. AI output reads as plausible whether or not it's right, so my review discipline (fresh-context reviews, watched-red tests, manually verifying live behavior, code review and small refactors) is where most of my time actually went, and skipping it to move faster would have produced something with issues throughout the entire architecture.

## Project structure

```
src/app/
  api/        HTTP layer: ingest route, query/stats/search routes, admin + health, response schemas
  queue/      EventQueue protocol and the in-process SimulatedQueue (ack/nack/reject, backoff, DLQ)
  worker/     queue consumer: batches to MongoDB then Elasticsearch, transient/permanent failure classification
  storage/    mongo.py (repository, aggregations, indexes) and es.py (search index, static mapping)
  cache/      Redis cache-aside for realtime stats, with in-process single-flight
  domain/     the Event model and its validation rules
  core/       settings, logging, request-id and rate-limit middleware
scripts/seed.py   themed sample-data generator (drives the real HTTP API)
tests/unit        no services required; fakes at module seams
tests/integration real MongoDB/Elasticsearch/Redis via docker compose
```

- ingestion validates and enqueues but never touches a store
- the worker is the only writer
- queries read from the store that owns each query type
- the cache is an optimization in front of MongoDB, not a dependency
