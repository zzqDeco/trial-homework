#!/bin/bash
# Run the full clean-room pipeline: validate behavior, generate load, then wait for end-to-end convergence.
set -euo pipefail

BASE_URL=${BASE_URL:-"http://127.0.0.1:8080"}
DASHBOARD_URL=${DASHBOARD_URL:-"http://127.0.0.1:8082"}
BROKERS=${BROKERS:-"localhost:9092"}
MIN_BIDS=${MIN_BIDS:-10001}
MIN_IMPRESSIONS=${MIN_IMPRESSIONS:-10001}
GO_IMAGE=${GO_IMAGE:-"golang:1.24"}
WAIT_TIMEOUT_SECONDS=${WAIT_TIMEOUT_SECONDS:-180}
PROJECTION_TIMEOUT_SECONDS=${PROJECTION_TIMEOUT_SECONDS:-120}

if [[ $# -ge 1 ]]; then
  BASE_URL="$1"
fi
if [[ $# -ge 2 ]]; then
  DASHBOARD_URL="$2"
fi
if [[ $# -ge 3 ]]; then
  BROKERS="$3"
fi
EXTRA_LOADGEN_ARGS=()
if [[ $# -gt 3 ]]; then
  EXTRA_LOADGEN_ARGS=("${@:4}")
fi

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)

pick_compose_cmd() {
  if docker compose version >/dev/null 2>&1; then
    echo "docker compose"
    return
  fi
  if command -v docker-compose >/dev/null 2>&1; then
    echo "docker-compose"
    return
  fi
  echo "Neither 'docker compose' nor 'docker-compose' is available." >&2
  exit 1
}

COMPOSE_CMD=$(pick_compose_cmd)

wait_for_http() {
  local url=$1
  local name=$2
  local deadline=$((SECONDS + WAIT_TIMEOUT_SECONDS))

  echo "Waiting for ${name} at ${url}"
  until curl -fsS "$url" >/dev/null 2>&1; do
    if (( SECONDS >= deadline )); then
      echo "Timed out waiting for ${name} at ${url}" >&2
      exit 1
    fi
    sleep 2
  done
}

require_running_container() {
  local name=$1
  local running
  running=$(docker inspect -f '{{.State.Running}}' "$name" 2>/dev/null || true)
  if [[ "$running" != "true" ]]; then
    echo "Required container is not running: ${name}" >&2
    docker ps -a --format 'table {{.Names}}\t{{.Status}}\t{{.Image}}' >&2
    exit 1
  fi
}

postgres_scalar() {
  local sql=$1
  docker exec postgres psql -U postgres -d bidsrv -tAc "$sql"
}

redis_hash_get() {
  local key=$1
  local field=$2
  docker exec redis redis-cli HGET "$key" "$field" 2>/dev/null || true
}

topic_count() {
  local topic=$1
  docker exec redpanda rpk topic describe "$topic" -p | awk 'NR > 1 {sum += $NF} END {print sum + 0}'
}

summary_window_bounds() {
  python3 - <<'PY'
from datetime import datetime, timedelta, timezone
now = datetime.now(timezone.utc)
start = (now - timedelta(days=1)).replace(microsecond=0)
end = (now + timedelta(hours=1)).replace(microsecond=0)
print(start.isoformat().replace("+00:00", "Z"))
print(end.isoformat().replace("+00:00", "Z"))
PY
}

fetch_summary_info() {
  local from_ts=$1
  local to_ts=$2
  local response
  if ! response=$(curl -fsS "${DASHBOARD_URL}/api/metrics/summary?from=${from_ts}&to=${to_ts}" 2>/dev/null); then
    cat <<'EOF'
unknown
0
0
0
EOF
    return 0
  fi

  SUMMARY_JSON="$response" python3 - <<'PY'
import json
import os

data = json.loads(os.environ["SUMMARY_JSON"])
print(data.get("source", "unknown"))
print(int(data.get("bid_requests", 0)))
print(int(data.get("deduped_impressions", 0)))
print(int(data.get("unknown_impressions", 0)))
PY
}

wait_for_full_convergence() {
  local deadline=$((SECONDS + PROJECTION_TIMEOUT_SECONDS))
  local from_ts
  local to_ts
  local topic_bids=$1
  local topic_impressions=$2
  local postgres_bids=0
  local postgres_impressions=0
  local outbox_backlog=0
  local redis_bid_requests
  local redis_deduped_impressions
  local redis_unknown_impressions
  local summary_bid_requests=0
  local summary_deduped_impressions=0
  local summary_unknown_impressions=0
  local summary_source=unknown

  mapfile -t window_bounds < <(summary_window_bounds)
  from_ts=${window_bounds[0]}
  to_ts=${window_bounds[1]}

  echo "Waiting for full convergence from topics to Postgres, Redis, and dashboard summary"
  while (( SECONDS < deadline )); do
    require_running_container "postgres"
    require_running_container "ingestor"
    require_running_container "projector"

    postgres_bids=$(postgres_scalar "select count(*) from bids;" | tr -d '[:space:]')
    postgres_impressions=$(postgres_scalar "select count(*) from impressions;" | tr -d '[:space:]')
    outbox_backlog=$(postgres_scalar "select count(*) from projection_outbox where processed_at is null;" | tr -d '[:space:]')
    redis_bid_requests=$(redis_hash_get "rm:global" "bid_requests" | tr -d '[:space:]')
    redis_deduped_impressions=$(redis_hash_get "rm:global" "deduped_impressions" | tr -d '[:space:]')
    redis_unknown_impressions=$(redis_hash_get "rm:global" "unknown_impressions" | tr -d '[:space:]')
    [[ -n "$postgres_bids" ]] || postgres_bids=0
    [[ -n "$postgres_impressions" ]] || postgres_impressions=0
    [[ -n "$outbox_backlog" ]] || outbox_backlog=0
    [[ -n "$redis_bid_requests" ]] || redis_bid_requests=0
    [[ -n "$redis_deduped_impressions" ]] || redis_deduped_impressions=0
    [[ -n "$redis_unknown_impressions" ]] || redis_unknown_impressions=0

    mapfile -t summary_info < <(fetch_summary_info "$from_ts" "$to_ts")
    summary_source=${summary_info[0]:-unknown}
    summary_bid_requests=${summary_info[1]:-0}
    summary_deduped_impressions=${summary_info[2]:-0}
    summary_unknown_impressions=${summary_info[3]:-0}

    if (( postgres_bids == topic_bids )) \
      && (( postgres_impressions == topic_impressions )) \
      && (( outbox_backlog == 0 )) \
      && (( redis_bid_requests == topic_bids )) \
      && (( redis_deduped_impressions + redis_unknown_impressions == topic_impressions )) \
      && [[ "$summary_source" == "redis" ]] \
      && (( summary_bid_requests == redis_bid_requests )) \
      && (( summary_deduped_impressions == redis_deduped_impressions )) \
      && (( summary_unknown_impressions == redis_unknown_impressions )); then
      echo "Full convergence check passed: topic_bids=${topic_bids}, topic_impressions=${topic_impressions}, outbox_backlog=${outbox_backlog}"
      return 0
    fi

    sleep 5
  done

  echo "Full convergence check failed." >&2
  echo "  topic_bids=${topic_bids}" >&2
  echo "  topic_impressions=${topic_impressions}" >&2
  echo "  postgres_bids=${postgres_bids}" >&2
  echo "  postgres_impressions=${postgres_impressions}" >&2
  echo "  outbox_backlog=${outbox_backlog}" >&2
  echo "  redis_bid_requests=${redis_bid_requests}" >&2
  echo "  redis_deduped_impressions=${redis_deduped_impressions}" >&2
  echo "  redis_unknown_impressions=${redis_unknown_impressions}" >&2
  echo "  summary_source=${summary_source}" >&2
  echo "  summary_bid_requests=${summary_bid_requests}" >&2
  echo "  summary_deduped_impressions=${summary_deduped_impressions}" >&2
  echo "  summary_unknown_impressions=${summary_unknown_impressions}" >&2
  docker ps -a --format 'table {{.Names}}\t{{.Status}}\t{{.Image}}' >&2
  exit 1
}

run_go_tool() {
  local package_path=$1
  shift

  if command -v go >/dev/null 2>&1; then
    (cd "$ROOT_DIR" && go run "$package_path" "$@")
    return
  fi

  docker run --rm --network host -v "$ROOT_DIR":/app -w /app "$GO_IMAGE" \
    go run "$package_path" "$@"
}

echo "Using compose command: ${COMPOSE_CMD}"
echo "Bringing up services"
(cd "$ROOT_DIR" && ${COMPOSE_CMD} up -d --build --remove-orphans)

wait_for_http "${BASE_URL}/healthz" "API"
wait_for_http "${DASHBOARD_URL}/healthz" "Dashboard"

echo "Verifying pipeline behavior against ${BASE_URL}"
(cd "$ROOT_DIR" && ./scripts/verify_pipeline_behavior.sh "${BASE_URL}")

echo "Running Deliverable A load generation against ${BASE_URL}"
run_go_tool ./cmd/loadgen --base-url "${BASE_URL}" "${EXTRA_LOADGEN_ARGS[@]}"

echo "Verifying topic counts via ${BROKERS}"
run_go_tool ./cmd/verify_topic_counts --brokers "${BROKERS}" --min-bids "${MIN_BIDS}" --min-impressions "${MIN_IMPRESSIONS}"

topic_bids=$(topic_count "bid-requests")
topic_impressions=$(topic_count "impressions")
wait_for_full_convergence "${topic_bids}" "${topic_impressions}"

cat <<SUMMARY

E2E run completed.

API:        ${BASE_URL}
Dashboard:  ${DASHBOARD_URL}
Brokers:    ${BROKERS}
Thresholds: bids>${MIN_BIDS}, impressions>${MIN_IMPRESSIONS}

Useful endpoints:
  ${DASHBOARD_URL}
  ${DASHBOARD_URL}/api/metrics/summary?from=<RFC3339>&to=<RFC3339>
  ${DASHBOARD_URL}/api/metrics/by-campaign?from=<RFC3339>&to=<RFC3339>
  ${DASHBOARD_URL}/api/metrics/timeseries?from=<RFC3339>&to=<RFC3339>&resolution=auto
SUMMARY
