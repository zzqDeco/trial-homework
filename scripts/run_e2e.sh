#!/bin/bash
set -euo pipefail

BASE_URL=${BASE_URL:-"http://127.0.0.1:8080"}
DASHBOARD_URL=${DASHBOARD_URL:-"http://127.0.0.1:8082"}
BROKERS=${BROKERS:-"localhost:9092"}
MIN_BIDS=${MIN_BIDS:-10001}
MIN_IMPRESSIONS=${MIN_IMPRESSIONS:-10001}
GO_IMAGE=${GO_IMAGE:-"golang:1.24"}
WAIT_TIMEOUT_SECONDS=${WAIT_TIMEOUT_SECONDS:-180}

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
