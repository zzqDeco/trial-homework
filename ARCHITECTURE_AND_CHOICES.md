# ARCHITECTURE_AND_CHOICES

## Purpose

This document explains how the final submission implements Deliverables A, B, C, and D, and why the main architectural choices were made. It focuses on the delivered design and its tradeoffs rather than on step-by-step runtime instructions; operational commands live in `README.md`.

## Final Delivered Architecture

The delivered system is:

`HTTP API -> Redpanda -> ingestor -> Postgres facts + projection_outbox -> projector -> Redis read model -> dashboard-api -> dashboard-web`

Key properties of the final design:

1. Postgres is the fact store.
2. Redis is the recent 30-day read model.
3. `dashboard-api` owns query routing and time-series aggregation.
4. `dashboard-web` is a separate frontend container.
5. The public dashboard entrypoint is `http://<VM_IP>:8082`.

## Metric Semantics

The dashboard metrics use the following semantics:

1. `bid_requests` = unique `bid_id` values stored in `bids`
2. `deduped_impressions` = unique `bid_id` impressions that ultimately have a matching bid
3. `unknown_impressions` = unique `bid_id` impressions that currently do not have a matching bid
4. `view_rate = deduped_impressions / bid_requests`
5. the segmentation dimension is `campaign_id`

Homework-specific simplification:

1. the homework version does not implement a waiting window before classifying unknown impressions
2. an impression that arrives before its bid is counted as unknown immediately
3. if the matching bid arrives later, the projector corrects the counts by moving the impression from `UNKNOWN` into the real campaign and from `unknown_impressions` into `deduped_impressions`

## Deliverable A

### Objective

Deliverable A generates data only through:

1. `POST /v1/bid`
2. `POST /v1/billing`

### Implemented workload

The generated traffic includes:

1. matched bid + billing flow
2. no-fill flow
3. unknown billing flow
4. duplicate billing flow
5. delayed billing flow
6. malformed and invalid billing requests
7. burst stages: steady, burst1, burst2, recovery

### Verification mechanism

1. `cmd/loadgen` drives HTTP traffic into the provided bidding API
2. `cmd/verify_topic_counts` validates both Redpanda topics by summing partition offsets
3. `deliverable_a_report.json` records counters, stage stats, and pass/fail results

### Final clean-room run

The final validated run used:

1. `./scripts/reset_data.sh`
2. `WAIT_TIMEOUT_SECONDS=300 PROJECTION_TIMEOUT_SECONDS=120 ./scripts/run_e2e.sh`

Observed result:

1. `targets_met=true`
2. `burst_slo_met=true`
3. `bid-requests=33825`
4. `impressions=28487`

This result came from a clean reset rather than from accumulated historical topic data.

## Deliverables B and C: Choice Analysis

### Why facts in Postgres and reads in Redis

The delivered system keeps facts in Postgres and serves recent dashboard queries from Redis.

1. facts need replayability, historical lookup, and rebuild semantics
2. the dashboard needs a low-latency and predictable serving path
3. this leads to a facts-first architecture:
   - Postgres stores durable event facts
   - Redis stores recent read-model aggregates
   - `dashboard-api` routes between them

### Why not Redis-only

**Alternative:** Redis-only facts plus dashboard serving.

Why it looks attractive:

1. reads are fast
2. counters are easy to update
3. the dashboard can read directly from Redis

Why it was not chosen:

1. Redis-only is weak for historical lookup
2. Redis-only makes unmatched correction auditing harder
3. Redis-only makes read-model rebuild semantics worse after data loss
4. facts do not need to live permanently in memory for this homework

Quantitative support:

1. the final clean-room run produced `33825 bids + 28487 impressions = 62312 facts`
2. if that level of facts were retained for 30 days, the system would hold `62312 * 30 = 1,869,360 facts`
3. if each fact consumes roughly `300B ~ 500B` of effective Redis memory overhead, that is approximately `561MB ~ 935MB`
4. this is only an order-of-magnitude estimate and does not include AOF/RDB cost, fragmentation, or future scale growth

Conclusion:

1. Redis is a good serving/read-model layer
2. Redis is not the right single source of truth for the delivered design

### Why not direct dual write from ingestor to Postgres and Redis

**Alternative:** have `ingestor` write Postgres and Redis directly for each consumed event.

Why it was not chosen:

1. Postgres and Redis do not share a transaction boundary
2. this is a distributed dual-write problem
3. split-write recovery semantics are weak

Quantitative support:

1. if DB success probability is `p_db` and Redis success probability is `p_redis`, the single-sided success risk is approximately:
   - `p_db * (1 - p_redis) + p_redis * (1 - p_db)`
2. if both are estimated at `99.9%`, the split-write risk is about `0.1998%`
3. over `62312` facts, that corresponds to roughly `124` split-write opportunities
4. this is a simplified failure model, used to show magnitude rather than exact real-world rates

Chosen design:

1. write facts to Postgres first
2. persist projection intent in `projection_outbox`
3. let `projector` update Redis asynchronously with retry and replay semantics

Conclusion:

1. outbox was chosen for recovery correctness, not for architectural novelty

### Why not write Redis first

**Alternative:** write Redis first so that recent dashboard values become visible earlier, then persist Postgres afterwards.

Why it looks attractive:

1. recent dashboard updates could appear slightly sooner

Why it was not chosen:

1. Redis-first places the read model ahead of the fact store
2. if Redis succeeds and Postgres fails, recovery semantics get worse
3. `backfill_redis`, outbox replay, and historical rebuild all depend on Postgres being the source of truth
4. Redis-first makes the first visible state non-authoritative, which makes audit and rebuild semantics more awkward

Quantitative support:

1. the same split-write risk model applies here as well
2. the key issue is not that mixed queries fail immediately, but that facts-first gives cleaner rebuild semantics

Conclusion:

1. the delivered design is explicitly facts-first, then projection

### Why the recent dashboard path prefers Redis

For routing analysis, define:

1. `M = ceil((to - from) / 1 minute)`
2. `C = campaign count`
3. `P = returned timeseries points`
4. `N_b = bid fact rows in range`
5. `N_i = impression fact rows in range`

Current query paths are:

#### Redis hit

When the full range is within the recent 30-day window, `source=redis`.

Current complexity:

1. `Summary(redis) = O(M)`
2. `ByCampaign(redis) = O(C * M)`
3. `TimeSeries(redis) = O(M)`

This is because Redis stores only minute buckets. Even when the API returns `hour` or `day`, the current implementation still reads minute buckets and aggregates them in the API layer.

#### Redis partial hit / mixed

When the range crosses the 30-day cutoff, `source=mixed`.

The current implementation now uses concurrent fan-out:

1. Postgres handles the historical segment
2. Redis handles the recent segment
3. `dashboard-api` merges the two results after both return

This means:

1. total work is still approximately:
   - `T_pg(old-range) + T_redis(recent-range) + T_merge`
2. wall-clock latency is now closer to:
   - `max(T_pg, T_redis) + T_merge`

This is intentionally different from the earlier serial implementation.

#### Redis miss / Postgres only

When the full range is older than the recent 30-day window, `source=postgres`.

Using order-of-magnitude expressions:

1. `Summary(postgres) ≈ O(N_b + N_i)`
2. `ByCampaign(postgres) ≈ O(N_b + N_i + C)`
3. `TimeSeries(postgres) ≈ O(N_b + N_i + P)`

Conclusion:

1. the recent dashboard path uses Redis so that recent query cost is bounded by bucket count rather than fact row count

### Why minute buckets

**Alternatives:** no buckets, hour-only buckets, or minute buckets.

Quantitative support:

1. 30 days of minute buckets means `30 * 24 * 60 = 43200` minute buckets
2. the current campaign set is approximately:
   - `campaign1`
   - `campaign2`
   - `campaign_default`
   - `UNKNOWN`
3. this leads to a bucket-series order of magnitude of:
   - `43200 * (1 global + 4 campaign series) = 216000`

Read implications:

1. a `1h` query touches about `60` minute buckets
2. a `24h` query touches about `1440` minute buckets
3. minute-aligned dashboard refresh semantics match the bucket granularity naturally

Current tradeoff:

1. `hour/day` responses are currently recomputed by the API from minute buckets
2. this keeps the write path simple
3. the tradeoff is that long-range Redis reads still scale with minute-bucket count

Conclusion:

1. minute buckets are the right homework-scale compromise
2. at 100x scale, this should evolve into multi-resolution aggregates

### Why the frontend was designed this way

Why separate frontend from `dashboard-api`:

1. UI concerns are decoupled from the Go API
2. charts, bilingual UI, and design control are easier in a dedicated web app
3. the public dashboard entrypoint still remains `:8082`

Why keep the UI intentionally simple:

1. the assignment is about clear metrics and low-latency serving, not about a complex product frontend
2. a single-page dashboard is enough to show totals, trends, and segmentation

Why bilingual:

1. English default is reviewer-friendly
2. Chinese is useful for local validation and demos
3. i18n stays entirely in the frontend, so the backend remains simple

Why minute-aligned auto refresh:

1. Redis is updated as minute-bucket read-model state
2. aligning refreshes to minute boundaries avoids refreshing mid-bucket when the aggregate has not materially changed

Why monochrome, borderless presentation:

1. it emphasizes metrics and trends rather than decoration
2. it reduces visual noise for dashboard reading

Why charts plus table:

1. KPIs give the overview
2. line and bar charts show trend and anomaly shape
3. the campaign table provides precise side-by-side comparison

## Dashboard API Surface

The dashboard API remains:

1. `GET /api/metrics/summary?from=<RFC3339>&to=<RFC3339>`
2. `GET /api/metrics/by-campaign?from=<RFC3339>&to=<RFC3339>`
3. `GET /api/metrics/timeseries?from=<RFC3339>&to=<RFC3339>&resolution=auto|minute|hour|day`

Important behavior notes:

1. mixed queries are now concurrent fan-out rather than serial fan-out
2. Redis stores minute buckets only; it does not store hour/day buckets explicitly
3. the frontend talks only to `dashboard-api`, never directly to Postgres or Redis

## Rebuild and Recovery

1. Postgres facts remain the source of truth
2. `projection_outbox` ties fact persistence to projection intent
3. `cmd/backfill_redis` rebuilds the recent 30-day Redis read model from Postgres facts
4. this recovery path exists precisely because the system does not use Redis-only facts, direct dual write, or Redis-first persistence

## 100x Scale Evolution

At higher scale, the main evolution path is:

1. increase Kafka partitions and scale consumers horizontally
2. batch DB writes and batch Redis projections
3. evolve minute-only buckets into multi-resolution aggregates
4. upgrade unknown handling from immediate unknown plus later correction to delayed finalization
5. move long-range analytics from Postgres facts to an OLAP store such as ClickHouse

The most important semantic upgrade at 100x scale is the unknown policy.

1. the homework version accepts immediate unknown plus later correction
2. a larger production-grade system should prefer delayed finalization to reduce correction churn and metric instability under reordering and late events

## Final Validation

The final stable proof point is the latest clean-room full run.

1. `./scripts/reset_data.sh`
2. `WAIT_TIMEOUT_SECONDS=300 PROJECTION_TIMEOUT_SECONDS=120 ./scripts/run_e2e.sh`
3. `targets_met=true`
4. `burst_slo_met=true`
5. `bid-requests=33825`
6. `impressions=28487`
7. `projection_outbox backlog=0`
8. dashboard summary returned non-zero Redis-backed metrics

Representative stable dashboard summary values after the system caught up:

1. `source=redis`
2. `bid_requests=33825`
3. `deduped_impressions=27487`
4. `unknown_impressions=1000`
5. `view_rate≈0.8126`
