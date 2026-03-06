# ARCHITECTURE_AND_CHOICES

## Scope

This document summarizes implementation and tradeoffs for the Zarli trial homework.

1. Deliverable A: HTTP-driven event generation with >10k bids and >10k impressions.
2. Deliverable B/C/D: high-level choices and future expansion notes.

## Deliverable A Implementation

### Objective

Generate events **only via HTTP APIs**:

1. `POST /v1/bid`
2. `POST /v1/billing`

Target outcomes:

1. `bid-requests > 10,000`
2. `impressions > 10,000`

### Implemented components

1. `cmd/loadgen/main.go`
2. `internal/loadgen/scenario.go`
3. `internal/loadgen/runner.go`
4. `internal/loadgen/report.go`
5. `scripts/run_deliverable_a.sh`
6. `cmd/verify_topic_counts/main.go`
7. `scripts/verify_topic_counts.sh`

### Workload model

Implemented workload includes both volume generation and edge-case injection.

1. Normal matched flow: bid success followed by billing.
2. No-fill flow: `user_idfv=789` causing bid `204`.
3. Unknown billing: billing on non-existent `bid_id` values.
4. Duplicate billing: repeated billing calls for already-billed `bid_id`.
5. Delayed billing: part of matched billing delayed by 1 to 3 seconds.
6. Invalid protocol requests:
   1. Missing `bid_id`
   2. Malformed JSON
7. Burst pressure stages:
   1. steady baseline
   2. burst-1 (mixed)
   3. burst-2 (bid-only)
   4. recovery

### Burst stages

1. steady: `workers=50`, `120s`
2. burst1: `workers=200`, `30s`, mixed bid+billing with 5% duplicate billing attempts
3. burst2: `workers=300`, `20s`, bid-only, 10% no-fill ratio
4. recovery: `workers=50`, `120s`

### Reporting

`loadgen` writes `deliverable_a_report.json` including:

1. global counters (`bid_attempts`, `bid_success`, `billing_attempts_total`, `billing_success_2xx`, `http_4xx`, `http_5xx`, `network_errors`, `retries_total`)
2. edge-case counters (`nofill_count`, `unknown_billing_unique`, `duplicate_billing_attempts`, `invalid_billing_attempts`)
3. stage metrics (`steady`, `burst1`, `burst2`, `recovery`)
4. pass/fail booleans (`targets_met`, `burst_slo_met`)

### Deliverable A run commands

```bash
docker compose up -d --build
./scripts/run_deliverable_a.sh http://localhost:8080
./scripts/verify_topic_counts.sh
```

### Topic counting approach

`cmd/verify_topic_counts` uses Kafka protocol requests via `franz-go` (`kgo` + `kmsg`):

1. query metadata for partitions
2. query earliest and latest offsets
3. sum `(latest - earliest)` across partitions

This avoids dependence on shell parsing and works with dynamic partition counts.

## Why this Deliverable A design

1. Uses only required HTTP interfaces.
2. Produces a realistic mix of successful, duplicate, unknown, delayed, and invalid traffic.
3. Separates execution and verification for reproducibility.
4. Outputs machine-readable evidence (`deliverable_a_report.json`) for review.

## Deliverable B/C high-level choices

1. Primary storage choice: Postgres.
2. Reasoning:
   1. dedup and matching logic are easy to verify with SQL
   2. lower implementation risk for the timebox
3. Dashboard reads from queryable storage and computes:
   1. view rate
   2. bid count
   3. deduped impression count
   4. unknown/unmatched impression count

## Tradeoffs

### Chosen now

1. prioritize correctness and reproducibility over extreme throughput
2. keep architecture simple for a 16-hour timebox

### Not chosen now

1. Redis-only as source of truth
2. complex stream processing frameworks

## 100x Scale Evolution

At 100x scale, evolve to layered design:

1. fact storage layer (DB/OLAP) for replay and audit
2. real-time aggregation cache (Redis) for low-latency dashboard reads
3. batch/stream ingestion for throughput and pre-aggregation

Expected upgrades:

1. batch writes and partition-aware consumers
2. delayed unmatched finalization window (1 to 5 minutes)
3. pre-aggregated minute buckets for dashboard endpoints
4. optional OLAP migration (for example ClickHouse) when query volume grows
