# ARCHITECTURE_AND_CHOICES

## Scope

This document summarizes the delivered implementation and the main tradeoffs.

1. Deliverable A: HTTP-driven event generation with >10k bids and >10k impressions.
2. Deliverable B: Kafka/Redpanda ingestion into a queryable fact store with asynchronous projection.
3. Deliverable C: low-latency dashboard at `:8082` with a recent Redis path and historical Postgres path.
4. Deliverable D: reproducible multi-service setup in Docker Compose.

## Deliverable A

### Objective

Generate events only through:

1. `POST /v1/bid`
2. `POST /v1/billing`

### Implemented workload

1. matched bid + billing flow
2. no-fill flow
3. unknown billing flow
4. duplicate billing flow
5. delayed billing flow
6. malformed and invalid billing requests
7. burst pressure stages: steady, burst1, burst2, recovery

### Verification

1. `cmd/loadgen` drives the traffic.
2. `cmd/verify_topic_counts` validates both topics by summing partition offsets.
3. `deliverable_a_report.json` records counters, stage stats, and pass/fail results.

## Deliverable B/C Architecture

### Final design

The implemented design is:

`API -> Redpanda -> Ingestor -> Postgres facts + projection_outbox -> Projector -> Redis read model -> Dashboard API`

This is intentionally layered.

1. Postgres is the fact store.
2. Redis is the low-latency read model.
3. Dashboard does not make the front-end choose data sources.
4. Dashboard API routes requests by time range.

### Why Postgres for facts

1. deduplication is simple with primary keys
2. replay safety is easy to reason about
3. the read model can be rebuilt from facts if Redis is lost
4. SQL remains the easiest way to verify matched vs unknown logic during debugging

### Why keep Redis for dashboard reads

1. recent queries are much faster when they do not scan fact tables
2. campaign-level summary reads become cheap and predictable
3. this mirrors a production-friendly separation between source of truth and serving model

### Why use `projection_outbox`

I kept `projection_outbox` instead of making the ingestor write Redis directly.

1. Postgres and Redis cannot be updated in one real shared transaction
2. if the DB write succeeds and Redis update fails, the projection intent would be lost without an outbox
3. outbox makes projection retryable and recoverable after crashes
4. projector lag is acceptable for a dashboard as long as freshness is exposed

### Current metric semantics

1. `bid_requests` = unique `bid_id` in bids
2. `deduped_impressions` = unique `bid_id` impressions that ultimately have a matching bid
3. `unknown_impressions` = unique `bid_id` impressions that currently do not have a matching bid
4. `view_rate = deduped_impressions / bid_requests`
5. segmentation dimension = `campaign_id`

### Homework-specific simplification

I did **not** implement an unknown waiting window for the homework version.

Instead:

1. if an impression is projected before its bid, it is counted as unknown immediately
2. the projector stores a Redis correction marker for that `bid_id`
3. if the bid arrives later, the projector moves that count from `UNKNOWN` to the real campaign and from `unknown_impressions` to `deduped_impressions`

This keeps the implementation small while still tolerating out-of-order arrival.

## Redis read model

### Keys

1. `rm:global`
2. `rm:campaigns`
3. `rm:campaign:<campaign_id>`
4. `rm:summary:min:<YYYYMMDDHHmm>`
5. `rm:campaign:<campaign_id>:min:<YYYYMMDDHHmm>`
6. `rm:bid:<bid_id>`
7. `rm:unknown:<bid_id>`

### Why minute buckets

1. they are simple to implement
2. they support recent arbitrary time-range queries at minute precision
3. they are enough for this homework scale

Current retention:

1. 30-day query window
2. 31-day TTL on Redis read-model keys

## Dashboard query routing

Dashboard API accepts:

1. `GET /api/metrics/summary?from=<RFC3339>&to=<RFC3339>`
2. `GET /api/metrics/by-campaign?from=<RFC3339>&to=<RFC3339>`

Routing rule:

1. if the entire range is within the last 30 days, read Redis
2. if the entire range is older than 30 days, read Postgres
3. if the range crosses the 30-day boundary, split the range and merge results in the API layer

The API returns a `source` field with `redis`, `postgres`, or `mixed`.

## Rebuild and recovery

1. Postgres facts remain the source of truth.
2. `cmd/backfill_redis` rebuilds the recent Redis read model from the last 30 days of Postgres facts.
3. projector replays any unprocessed `projection_outbox` rows after restart.

## Main tradeoffs

### Chosen now

1. prioritize correctness and recoverability over the absolute lowest latency
2. keep the API path unchanged because Deliverable A already passed and was stable
3. accept eventual consistency between Postgres facts and Redis dashboard reads

### Not chosen now

1. Redis as the only source of truth
2. direct dashboard reads from Postgres for recent traffic
3. stream-processing frameworks such as Flink or Kafka Streams
4. delayed unknown finalization window in the homework implementation

## 100x Scale Evolution

At higher scale I would change the design in these ways.

1. increase Kafka partitions and scale consumers horizontally
2. batch DB writes and batch Redis projections
3. replace minute-only Redis buckets with multi-resolution aggregates
4. add a delayed unmatched finalization window, for example 1 to 5 minutes
5. consider moving long-range analytics from Postgres facts to an OLAP store such as ClickHouse

The most important semantic upgrade at 100x is the unknown policy.

For the homework version, immediate unknown plus later correction is acceptable.
At 100x scale, delayed finalization is the better design because it reduces correction churn and gives more stable metrics under reordering and late events.
