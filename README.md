# zzqDeco Zarli Take-Home Submission

This repository delivers the complete event pipeline and dashboard implementation for the Zarli take-home assignment. Bid and impression traffic is generated through the provided HTTP APIs, transported through Redpanda, persisted as facts in Postgres, projected asynchronously into Redis, and served through an independent dashboard frontend exposed on `:8082`.

## What Was Built

1. HTTP-driven load generation for bids and impressions
2. Redpanda ingestion into Postgres fact tables
3. Asynchronous Redis projection via `projection_outbox`
4. Dashboard API with Redis/Postgres source routing
5. Independent dashboard web frontend on `:8082`

## Architecture Overview

1. `POST /v1/bid -> Redpanda(bid-requests)`
2. `POST /v1/billing -> Redis request-side idempotency -> Redpanda(impressions)`
3. `Redpanda -> ingestor -> Postgres facts + projection_outbox -> projector -> Redis read model -> dashboard-api -> dashboard-web`

1. Postgres is the fact store.
2. Redis serves two roles:
   - request-side billing idempotency
   - recent dashboard read model
3. `dashboard-api` routes between Redis, Postgres, and mixed queries.
4. `dashboard-web` is a separate frontend container.
5. The public dashboard entrypoint is `http://<VM_IP>:8082`.

## Deliverables Mapping

### Deliverable A

1. `cmd/loadgen`
2. `cmd/verify_topic_counts`
3. `scripts/run_deliverable_a.sh`

### Deliverable B

1. `cmd/ingestor`
2. Postgres fact tables
3. `projection_outbox`

### Deliverable C

1. `cmd/projector`
2. `cmd/dashboard`
3. `dashboard-web`
4. Dashboard exposed on `:8082`

### Deliverable D

1. `scripts/reset_data.sh`
2. `scripts/run_e2e.sh`

## Quick Start

```bash
docker compose up -d --build
./scripts/reset_data.sh
WAIT_TIMEOUT_SECONDS=300 PROJECTION_TIMEOUT_SECONDS=240 ./scripts/run_e2e.sh
```

If your environment only provides the legacy binary, replace `docker compose` with `docker-compose`.

Access points:

1. API health: `http://<VM_IP>:8080/healthz`
2. Dashboard: `http://<VM_IP>:8082`

## Manual Verification

API health:

```bash
curl http://localhost:8080/healthz
```

Dashboard health:

```bash
curl http://localhost:8082/healthz
```

Summary:

```bash
curl 'http://localhost:8082/api/metrics/summary?from=2026-03-06T00:00:00Z&to=2026-03-07T00:00:00Z'
```

By campaign:

```bash
curl 'http://localhost:8082/api/metrics/by-campaign?from=2026-03-06T00:00:00Z&to=2026-03-07T00:00:00Z'
```

Timeseries:

```bash
curl 'http://localhost:8082/api/metrics/timeseries?from=2026-03-06T00:00:00Z&to=2026-03-07T00:00:00Z&resolution=auto'
```

Pipeline behavior checks:

```bash
./scripts/verify_pipeline_behavior.sh
```

`verify_pipeline_behavior.sh` validates HTTP side effects, late bid correction, and Redis rebuild consistency.

`run_e2e.sh` first validates request-side and downstream pipeline behavior, then runs the full load, topic threshold checks, and full convergence checks across topics, Postgres, Redis, and dashboard summary.

## Latest Validated Result

The latest clean-room full run used:

1. `./scripts/reset_data.sh`
2. `WAIT_TIMEOUT_SECONDS=300 PROJECTION_TIMEOUT_SECONDS=240 ./scripts/run_e2e.sh`

Observed result:

1. pipeline behavior verification passed
2. `targets_met=true`
3. `burst_slo_met=true`
4. `bid-requests=32022`
5. `impressions=26789`
6. full convergence check passed
7. Dashboard summary returned non-zero Redis-backed metrics

## Additional Documentation

1. [ARCHITECTURE_AND_CHOICES.md](./ARCHITECTURE_AND_CHOICES.md)
