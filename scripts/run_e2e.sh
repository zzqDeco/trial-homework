#!/bin/bash
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
  curl -fsS "${DASHBOARD_URL}/api/metrics/summary?from=${from_ts}&to=${to_ts}" | python3 -c '
import json, sys
data = json.load(sys.stdin)
print(data.get("bid_requests", 0))
print(data.get("source", "unknown"))
'
}

wait_for_projection_health() {
  local deadline=$((SECONDS + PROJECTION_TIMEOUT_SECONDS))
  local initial_backlog=""
  local last_backlog=""
  local backlog_decreased=false
  local from_ts
  local to_ts
  local redis_bid_requests
  local backlog
  local summary_bid_requests
  local summary_source

  mapfile -t window_bounds < <(summary_window_bounds)
  from_ts=${window_bounds[0]}
  to_ts=${window_bounds[1]}

  echo "Waiting for ingestion and projection health"
  while (( SECONDS < deadline )); do
    require_running_container "postgres"
    require_running_container "ingestor"
    require_running_container "projector"

    backlog=$(postgres_scalar "select count(*) from projection_outbox where processed_at is null;" | tr -d '[:space:]')
    redis_bid_requests=$(redis_hash_get "rm:global" "bid_requests" | tr -d '[:space:]')
    [[ -n "$redis_bid_requests" ]] || redis_bid_requests=0

    mapfile -t summary_info < <(fetch_summary_info "$from_ts" "$to_ts")
    summary_bid_requests=${summary_info[0]:-0}
    summary_source=${summary_info[1]:-unknown}

    if [[ -z "$initial_backlog" ]]; then
      initial_backlog=$backlog
    fi
    if [[ -n "$last_backlog" ]] && (( backlog < last_backlog )); then
      backlog_decreased=true
    fi

    if (( initial_backlog == 0 )); then
      backlog_decreased=true
    fi

    if [[ "$summary_source" == "redis" ]] && (( redis_bid_requests > MIN_BIDS )) && (( summary_bid_requests > 0 )) && [[ "$backlog_decreased" == "true" ]]; then
      echo "Projection health check passed: backlog=${backlog}, redis_bid_requests=${redis_bid_requests}, summary_bid_requests=${summary_bid_requests}"
      return 0
    fi

    last_backlog=$backlog
    sleep 5
  done

  echo "Projection health check failed." >&2
  echo "  initial_backlog=${initial_backlog:-unknown}" >&2
  echo "  last_backlog=${last_backlog:-unknown}" >&2
  echo "  redis_bid_requests=${redis_bid_requests:-unknown}" >&2
  echo "  summary_bid_requests=${summary_bid_requests:-unknown}" >&2
  echo "  summary_source=${summary_source:-unknown}" >&2
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

echo "Running Deliverable A load generation against ${BASE_URL}"
run_go_tool ./cmd/loadgen --base-url "${BASE_URL}" "${EXTRA_LOADGEN_ARGS[@]}"

echo "Verifying topic counts via ${BROKERS}"
run_go_tool ./cmd/verify_topic_counts --brokers "${BROKERS}" --min-bids "${MIN_BIDS}" --min-impressions "${MIN_IMPRESSIONS}"

wait_for_projection_health

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
