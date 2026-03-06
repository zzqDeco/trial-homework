#!/bin/bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
BASE_URL=${BASE_URL:-"http://127.0.0.1:8080"}
DASHBOARD_URL=${DASHBOARD_URL:-"http://127.0.0.1:8082"}
WAIT_TIMEOUT_SECONDS=${WAIT_TIMEOUT_SECONDS:-180}
START_SERVICES=false

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

for arg in "$@"; do
  case "$arg" in
    --start)
      START_SERVICES=true
      ;;
    *)
      echo "Unknown argument: $arg" >&2
      echo "Usage: ./scripts/reset_data.sh [--start]" >&2
      exit 1
      ;;
  esac
done

COMPOSE_CMD=$(pick_compose_cmd)

echo "Using compose command: ${COMPOSE_CMD}"
echo "Stopping services and removing data volumes"
(cd "$ROOT_DIR" && ${COMPOSE_CMD} down -v --remove-orphans)

rm -f "$ROOT_DIR/deliverable_a_report.json"

if [[ "$START_SERVICES" == "true" ]]; then
  echo
  echo "Restarting services"
  (cd "$ROOT_DIR" && ${COMPOSE_CMD} up -d --build --remove-orphans)
  wait_for_http "${BASE_URL}/healthz" "API"
  wait_for_http "${DASHBOARD_URL}/healthz" "Dashboard"
  SERVICE_STATE="Services restarted and healthy."
else
  SERVICE_STATE="Containers are stopped. Run ./scripts/run_e2e.sh next."
fi

cat <<SUMMARY

Runtime data reset completed.

Removed data:
  - Redpanda topic data (via redpanda-data volume)
  - Redis data (via redis-data volume)
  - Postgres data (via postgres-data volume)
  - deliverable_a_report.json

Current state:
  - Named volumes have been removed
  - ${SERVICE_STATE}
SUMMARY
