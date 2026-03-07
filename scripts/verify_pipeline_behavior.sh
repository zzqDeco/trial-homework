#!/bin/bash
# Verify request-level side effects plus the downstream projection and rebuild behavior.
set -euo pipefail

BASE_URL=${1:-"http://127.0.0.1:8080"}
DASHBOARD_URL=${DASHBOARD_URL:-"http://127.0.0.1:8082"}
GO_IMAGE=${GO_IMAGE:-"golang:1.24"}
WAIT_TIMEOUT_SECONDS=${WAIT_TIMEOUT_SECONDS:-60}

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)

WINDOW_FROM=${WINDOW_FROM:-""}
WINDOW_TO=${WINDOW_TO:-""}
if [[ -z "$WINDOW_FROM" || -z "$WINDOW_TO" ]]; then
  mapfile -t WINDOW_BOUNDS < <(python3 - <<'PY'
from datetime import datetime, timedelta, timezone
now = datetime.now(timezone.utc)
start = (now - timedelta(days=1)).replace(microsecond=0)
end = (now + timedelta(hours=1)).replace(microsecond=0)
print(start.isoformat().replace("+00:00", "Z"))
print(end.isoformat().replace("+00:00", "Z"))
PY
)
  WINDOW_FROM=${WINDOW_BOUNDS[0]}
  WINDOW_TO=${WINDOW_BOUNDS[1]}
fi

pass() {
  echo "PASS: $*"
}

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

section() {
  echo
  echo "=== $* ==="
}

require_running_container() {
  local name=$1
  local running
  running=$(docker inspect -f '{{.State.Running}}' "$name" 2>/dev/null || true)
  if [[ "$running" != "true" ]]; then
    fail "required container is not running: ${name}"
  fi
}

wait_for_http() {
  local url=$1
  local name=$2
  local deadline=$((SECONDS + WAIT_TIMEOUT_SECONDS))
  until curl -fsS "$url" >/dev/null 2>&1; do
    if (( SECONDS >= deadline )); then
      fail "timed out waiting for ${name} at ${url}"
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
  docker run --rm --network host -v "$ROOT_DIR":/app -w /app "$GO_IMAGE" go run "$package_path" "$@"
}

topic_count() {
  local topic=$1
  docker exec redpanda rpk topic describe "$topic" -p | awk 'NR > 1 {sum += $NF} END {print sum + 0}'
}

redis_exists() {
  local key=$1
  docker exec redis redis-cli EXISTS "$key" 2>/dev/null | tr -d '[:space:]\r'
}

postgres_scalar() {
  local sql=$1
  docker exec postgres psql -U postgres -d bidsrv -tAc "$sql" | tr -d '[:space:]'
}

fetch_summary() {
  curl -fsS "${DASHBOARD_URL}/api/metrics/summary?from=${WINDOW_FROM}&to=${WINDOW_TO}"
}

fetch_by_campaign() {
  curl -fsS "${DASHBOARD_URL}/api/metrics/by-campaign?from=${WINDOW_FROM}&to=${WINDOW_TO}"
}

json_field() {
  local json=$1
  local field=$2
  JSON_INPUT="$json" python3 - "$field" <<'PY'
import json
import os
import sys

field = sys.argv[1]
raw = os.environ.get("JSON_INPUT", "").strip()
if not raw:
    print("")
    raise SystemExit(0)
try:
    data = json.loads(raw)
except json.JSONDecodeError:
    print("")
    raise SystemExit(0)
value = data.get(field, "")
if value is None:
    print("")
elif isinstance(value, bool):
    print("true" if value else "false")
else:
    print(value)
PY
}

campaign_metric() {
  local json=$1
  local campaign_id=$2
  local field=$3
  JSON_INPUT="$json" python3 - "$campaign_id" "$field" <<'PY'
import json
import os
import sys

campaign_id = sys.argv[1]
field = sys.argv[2]
raw = os.environ.get("JSON_INPUT", "")
data = json.loads(raw)
for item in data.get("campaigns", []):
    if item.get("campaign_id") == campaign_id:
        value = item.get(field, 0)
        print(value if value is not None else 0)
        raise SystemExit(0)
print(0)
PY
}

canonical_summary() {
  JSON_INPUT="$1" python3 - <<'PY'
import json
import os

data = json.loads(os.environ["JSON_INPUT"])
out = {
    "bid_requests": int(data.get("bid_requests", 0)),
    "deduped_impressions": int(data.get("deduped_impressions", 0)),
    "unknown_impressions": int(data.get("unknown_impressions", 0)),
}
print(json.dumps(out, sort_keys=True))
PY
}

canonical_campaigns() {
  JSON_INPUT="$1" python3 - <<'PY'
import json
import os

data = json.loads(os.environ["JSON_INPUT"])
items = []
for item in data.get("campaigns", []):
    items.append({
        "campaign_id": item.get("campaign_id"),
        "bid_requests": int(item.get("bid_requests", 0)),
        "deduped_impressions": int(item.get("deduped_impressions", 0)),
        "unknown_impressions": int(item.get("unknown_impressions", 0)),
    })
items.sort(key=lambda x: x["campaign_id"])
print(json.dumps(items, sort_keys=True))
PY
}

assert_equal() {
  local expected=$1
  local actual=$2
  local message=$3
  if [[ "$expected" != "$actual" ]]; then
    fail "${message} (expected=${expected}, actual=${actual})"
  fi
  pass "${message}"
}

assert_non_empty() {
  local actual=$1
  local message=$2
  if [[ -z "$actual" ]]; then
    fail "${message} (value is empty)"
  fi
  pass "${message}"
}

assert_http_code() {
  local expected=$1
  local actual=$2
  local message=$3
  assert_equal "$expected" "$actual" "$message"
}

assert_topic_delta() {
  local before=$1
  local after=$2
  local expected_delta=$3
  local message=$4
  local actual_delta=$((after - before))
  if (( actual_delta != expected_delta )); then
    fail "${message} (expected_delta=${expected_delta}, actual_delta=${actual_delta})"
  fi
  pass "${message}"
}

http_request() {
  local method=$1
  local url=$2
  local content_type=$3
  local body=$4
  local tmp
  tmp=$(mktemp)
  HTTP_CODE=$(curl -sS -o "$tmp" -w "%{http_code}" -X "$method" "$url" -H "Content-Type: ${content_type}" --data-binary "$body")
  HTTP_BODY=$(cat "$tmp")
  rm -f "$tmp"
}

bid_request() {
  local user_id=$1
  local app_bundle=$2
  local placement_id=$3
  local ts
  ts=$(date +%s)
  http_request "POST" "${BASE_URL}/v1/bid" "application/json" "{\"user_idfv\":\"${user_id}\",\"app_bundle\":\"${app_bundle}\",\"placement_id\":\"${placement_id}\",\"timestamp\":${ts}}"
}

billing_request() {
  local bid_id=$1
  local ts
  ts=$(date +%s)
  http_request "POST" "${BASE_URL}/v1/billing" "application/json" "{\"bid_id\":\"${bid_id}\",\"timestamp\":${ts}}"
}

produce_kafka_value() {
  local topic=$1
  local key=$2
  local payload=$3
  printf '%s\n' "$payload" | docker exec -i redpanda rpk topic produce "$topic" -k "$key" >/dev/null
}

wait_for_idle_projection() {
  local deadline=$((SECONDS + WAIT_TIMEOUT_SECONDS))
  while (( SECONDS < deadline )); do
    local backlog
    backlog=$(postgres_scalar "select count(*) from projection_outbox where processed_at is null;")
    [[ -n "$backlog" ]] || backlog=0
    if (( backlog == 0 )); then
      return 0
    fi
    sleep 2
  done
  fail "projection backlog did not drain before verification"
}

wait_for_http_chain_convergence() {
  local base_pg_bids=$1
  local base_pg_impressions=$2
  local base_summary_bids=$3
  local base_summary_dedup=$4
  local base_summary_unknown=$5
  local base_c1_bid=$6
  local base_c1_dedup=$7
  local base_c2_bid=$8
  local base_c2_dedup=$9
  local base_cd_bid=${10}
  local base_cd_dedup=${11}
  local base_unknown_unknown=${12}
  local deadline=$((SECONDS + WAIT_TIMEOUT_SECONDS))

  while (( SECONDS < deadline )); do
    local backlog pg_bids pg_impressions summary_json summary_source summary_bids summary_dedup summary_unknown
    local by_campaign_json c1_bid c1_dedup c2_bid c2_dedup cd_bid cd_dedup unknown_unknown

    backlog=$(postgres_scalar "select count(*) from projection_outbox where processed_at is null;")
    pg_bids=$(postgres_scalar "select count(*) from bids;")
    pg_impressions=$(postgres_scalar "select count(*) from impressions;")

    summary_json=$(fetch_summary)
    summary_source=$(json_field "$summary_json" "source")
    summary_bids=$(json_field "$summary_json" "bid_requests")
    summary_dedup=$(json_field "$summary_json" "deduped_impressions")
    summary_unknown=$(json_field "$summary_json" "unknown_impressions")

    by_campaign_json=$(fetch_by_campaign)
    c1_bid=$(campaign_metric "$by_campaign_json" "campaign1" "bid_requests")
    c1_dedup=$(campaign_metric "$by_campaign_json" "campaign1" "deduped_impressions")
    c2_bid=$(campaign_metric "$by_campaign_json" "campaign2" "bid_requests")
    c2_dedup=$(campaign_metric "$by_campaign_json" "campaign2" "deduped_impressions")
    cd_bid=$(campaign_metric "$by_campaign_json" "campaign_default" "bid_requests")
    cd_dedup=$(campaign_metric "$by_campaign_json" "campaign_default" "deduped_impressions")
    unknown_unknown=$(campaign_metric "$by_campaign_json" "UNKNOWN" "unknown_impressions")

    if [[ "$summary_source" == "redis" ]] &&
       (( backlog == 0 )) &&
       (( pg_bids == base_pg_bids + 3 )) &&
       (( pg_impressions == base_pg_impressions + 3 )) &&
       (( summary_bids == base_summary_bids + 3 )) &&
       (( summary_dedup == base_summary_dedup + 2 )) &&
       (( summary_unknown == base_summary_unknown + 1 )) &&
       (( c1_bid == base_c1_bid + 1 )) &&
       (( c1_dedup == base_c1_dedup + 1 )) &&
       (( c2_bid == base_c2_bid + 1 )) &&
       (( c2_dedup == base_c2_dedup + 1 )) &&
       (( cd_bid == base_cd_bid + 1 )) &&
       (( cd_dedup == base_cd_dedup + 0 )) &&
       (( unknown_unknown == base_unknown_unknown + 1 )); then
      pass "HTTP-driven facts converged into Postgres and Redis"
      return 0
    fi
    sleep 2
  done
  fail "HTTP contract checks did not converge through facts and read model"
}

wait_for_late_unknown_state() {
  local late_bid_id=$1
  local base_pg_impressions=$2
  local base_summary_unknown=$3
  local base_unknown_unknown=$4
  local deadline=$((SECONDS + WAIT_TIMEOUT_SECONDS))

  while (( SECONDS < deadline )); do
    local backlog pg_impressions summary_json summary_unknown unknown_exists unknown_campaign_unknown
    backlog=$(postgres_scalar "select count(*) from projection_outbox where processed_at is null;")
    pg_impressions=$(postgres_scalar "select count(*) from impressions;")
    summary_json=$(fetch_summary)
    summary_unknown=$(json_field "$summary_json" "unknown_impressions")
    unknown_exists=$(redis_exists "rm:unknown:${late_bid_id}")
    unknown_campaign_unknown=$(campaign_metric "$(fetch_by_campaign)" "UNKNOWN" "unknown_impressions")

    if [[ "$(json_field "$summary_json" "source")" == "redis" ]] &&
       (( backlog == 0 )) &&
       (( pg_impressions == base_pg_impressions + 1 )) &&
       (( summary_unknown == base_summary_unknown + 1 )) &&
       [[ "$unknown_exists" == "1" ]] &&
       (( unknown_campaign_unknown == base_unknown_unknown + 1 )); then
      pass "late impression is first counted as UNKNOWN"
      return 0
    fi
    sleep 2
  done
  fail "late bid correction precondition did not converge to UNKNOWN state"
}

wait_for_late_correction() {
  local late_bid_id=$1
  local late_campaign_id=$2
  local base_pg_bids=$3
  local base_pg_impressions=$4
  local base_summary_bids=$5
  local base_summary_dedup=$6
  local base_summary_unknown=$7
  local base_campaign_bid=$8
  local base_campaign_dedup=$9
  local base_unknown_unknown=${10}
  local deadline=$((SECONDS + WAIT_TIMEOUT_SECONDS))

  while (( SECONDS < deadline )); do
    local backlog pg_bids pg_impressions summary_json summary_source summary_bids summary_dedup summary_unknown
    local unknown_exists bid_exists by_campaign_json late_bid_count late_dedup_count unknown_unknown

    backlog=$(postgres_scalar "select count(*) from projection_outbox where processed_at is null;")
    pg_bids=$(postgres_scalar "select count(*) from bids;")
    pg_impressions=$(postgres_scalar "select count(*) from impressions;")

    summary_json=$(fetch_summary)
    summary_source=$(json_field "$summary_json" "source")
    summary_bids=$(json_field "$summary_json" "bid_requests")
    summary_dedup=$(json_field "$summary_json" "deduped_impressions")
    summary_unknown=$(json_field "$summary_json" "unknown_impressions")

    unknown_exists=$(redis_exists "rm:unknown:${late_bid_id}")
    bid_exists=$(redis_exists "rm:bid:${late_bid_id}")
    by_campaign_json=$(fetch_by_campaign)
    late_bid_count=$(campaign_metric "$by_campaign_json" "$late_campaign_id" "bid_requests")
    late_dedup_count=$(campaign_metric "$by_campaign_json" "$late_campaign_id" "deduped_impressions")
    unknown_unknown=$(campaign_metric "$by_campaign_json" "UNKNOWN" "unknown_impressions")

    if [[ "$summary_source" == "redis" ]] &&
       (( backlog == 0 )) &&
       (( pg_bids == base_pg_bids + 1 )) &&
       (( pg_impressions == base_pg_impressions + 1 )) &&
       (( summary_bids == base_summary_bids + 1 )) &&
       (( summary_dedup == base_summary_dedup + 1 )) &&
       (( summary_unknown == base_summary_unknown )) &&
       [[ "$unknown_exists" == "0" ]] &&
       [[ "$bid_exists" == "1" ]] &&
       (( late_bid_count == base_campaign_bid + 1 )) &&
       (( late_dedup_count == base_campaign_dedup + 1 )) &&
       (( unknown_unknown == base_unknown_unknown )); then
      pass "late bid correction moved UNKNOWN into the real campaign"
      return 0
    fi
    sleep 2
  done
  fail "late bid correction did not converge to the corrected state"
}

wait_for_backfill_rebuild() {
  local expected_summary=$1
  local expected_campaigns=$2
  local expected_pg_bids=$3
  local expected_pg_impressions=$4
  local deadline=$((SECONDS + WAIT_TIMEOUT_SECONDS))

  while (( SECONDS < deadline )); do
    local backlog pg_bids pg_impressions summary_json summary_source actual_summary actual_campaigns
    backlog=$(postgres_scalar "select count(*) from projection_outbox where processed_at is null;")
    pg_bids=$(postgres_scalar "select count(*) from bids;")
    pg_impressions=$(postgres_scalar "select count(*) from impressions;")
    summary_json=$(fetch_summary)
    summary_source=$(json_field "$summary_json" "source")
    actual_summary=$(canonical_summary "$summary_json")
    actual_campaigns=$(canonical_campaigns "$(fetch_by_campaign)")

    if [[ "$summary_source" == "redis" ]] &&
       (( backlog == 0 )) &&
       (( pg_bids == expected_pg_bids )) &&
       (( pg_impressions == expected_pg_impressions )) &&
       [[ "$actual_summary" == "$expected_summary" ]] &&
       [[ "$actual_campaigns" == "$expected_campaigns" ]]; then
      pass "backfill rebuilt the Redis read model without changing facts"
      return 0
    fi
    sleep 2
  done
  fail "backfill did not restore the expected Redis snapshots"
}

run_http_contract_checks() {
  local bids_before
  local imps_before
  local bid_id_123
  local bid_id_456

  section "HTTP contract checks"

  bids_before=$(topic_count "bid-requests")
  imps_before=$(topic_count "impressions")
  bid_request "123" "com.test" "p1"
  assert_http_code "200" "$HTTP_CODE" "bid(123) returns 200"
  assert_non_empty "$(json_field "$HTTP_BODY" "request_id")" "bid(123) returns request_id"
  bid_id_123=$(json_field "$HTTP_BODY" "bid_id")
  assert_non_empty "$bid_id_123" "bid(123) returns bid_id"
  assert_equal "campaign1" "$(json_field "$HTTP_BODY" "campaign_id")" "bid(123) selects campaign1"
  assert_non_empty "$(json_field "$HTTP_BODY" "billing_url")" "bid(123) returns billing_url"
  assert_topic_delta "$bids_before" "$(topic_count "bid-requests")" 1 "bid(123) appends one bid event"
  assert_topic_delta "$imps_before" "$(topic_count "impressions")" 0 "bid(123) does not append an impression"

  imps_before=$(topic_count "impressions")
  billing_request "$bid_id_123"
  assert_http_code "200" "$HTTP_CODE" "matched billing returns 200"
  assert_topic_delta "$imps_before" "$(topic_count "impressions")" 1 "matched billing appends one impression"
  assert_equal "1" "$(redis_exists "billing:${bid_id_123}")" "matched billing creates the Redis idempotency key"

  imps_before=$(topic_count "impressions")
  billing_request "$bid_id_123"
  assert_http_code "200" "$HTTP_CODE" "duplicate billing still returns 200"
  assert_topic_delta "$imps_before" "$(topic_count "impressions")" 0 "duplicate billing does not append another impression"
  assert_equal "1" "$(redis_exists "billing:${bid_id_123}")" "duplicate billing leaves the Redis idempotency key in place"

  bids_before=$(topic_count "bid-requests")
  bid_request "456" "com.test" "p2"
  assert_http_code "200" "$HTTP_CODE" "bid(456) returns 200"
  bid_id_456=$(json_field "$HTTP_BODY" "bid_id")
  assert_non_empty "$bid_id_456" "bid(456) returns bid_id"
  assert_equal "campaign2" "$(json_field "$HTTP_BODY" "campaign_id")" "bid(456) selects campaign2"
  assert_topic_delta "$bids_before" "$(topic_count "bid-requests")" 1 "bid(456) appends one bid event"

  imps_before=$(topic_count "impressions")
  billing_request "$bid_id_456"
  assert_http_code "200" "$HTTP_CODE" "second matched billing returns 200"
  assert_topic_delta "$imps_before" "$(topic_count "impressions")" 1 "second matched billing appends one impression"

  bids_before=$(topic_count "bid-requests")
  bid_request "999" "com.test" "p3"
  assert_http_code "200" "$HTTP_CODE" "bid(999) returns 200"
  assert_equal "campaign_default" "$(json_field "$HTTP_BODY" "campaign_id")" "bid(999) selects campaign_default"
  assert_topic_delta "$bids_before" "$(topic_count "bid-requests")" 1 "bid(999) appends one bid event"

  bids_before=$(topic_count "bid-requests")
  imps_before=$(topic_count "impressions")
  bid_request "789" "com.test" "p4"
  assert_http_code "204" "$HTTP_CODE" "bid(789) returns 204 for no-fill"
  assert_topic_delta "$bids_before" "$(topic_count "bid-requests")" 0 "no-fill does not append a bid event"
  assert_topic_delta "$imps_before" "$(topic_count "impressions")" 0 "no-fill does not append an impression"

  local unknown_bid_id
  unknown_bid_id=$(python3 - <<'PY'
import uuid
print(str(uuid.uuid4()))
PY
)
  imps_before=$(topic_count "impressions")
  billing_request "$unknown_bid_id"
  assert_http_code "200" "$HTTP_CODE" "unknown billing returns 200"
  assert_topic_delta "$imps_before" "$(topic_count "impressions")" 1 "unknown billing appends one impression"
  assert_equal "1" "$(redis_exists "billing:${unknown_bid_id}")" "unknown billing still creates the Redis idempotency key"

  imps_before=$(topic_count "impressions")
  http_request "POST" "${BASE_URL}/v1/billing" "application/json" "{\"timestamp\":$(date +%s)}"
  assert_http_code "400" "$HTTP_CODE" "billing without bid_id returns 400"
  assert_topic_delta "$imps_before" "$(topic_count "impressions")" 0 "missing bid_id does not append an impression"

  imps_before=$(topic_count "impressions")
  http_request "POST" "${BASE_URL}/v1/billing" "application/json" "{bad-json"
  assert_http_code "400" "$HTTP_CODE" "malformed billing JSON returns 400"
  assert_topic_delta "$imps_before" "$(topic_count "impressions")" 0 "malformed billing JSON does not append an impression"
}

main() {
  require_running_container "redpanda"
  require_running_container "redis"
  require_running_container "postgres"
  require_running_container "ingestor"
  require_running_container "projector"

  wait_for_http "${BASE_URL}/healthz" "API"
  wait_for_http "${DASHBOARD_URL}/healthz" "Dashboard"
  wait_for_idle_projection

  local base_pg_bids base_pg_impressions base_summary_json base_summary_bids base_summary_dedup base_summary_unknown
  local base_by_campaign_json base_c1_bid base_c1_dedup base_c2_bid base_c2_dedup base_cd_bid base_cd_dedup base_unknown_unknown
  base_pg_bids=$(postgres_scalar "select count(*) from bids;")
  base_pg_impressions=$(postgres_scalar "select count(*) from impressions;")
  base_summary_json=$(fetch_summary)
  base_summary_bids=$(json_field "$base_summary_json" "bid_requests")
  base_summary_dedup=$(json_field "$base_summary_json" "deduped_impressions")
  base_summary_unknown=$(json_field "$base_summary_json" "unknown_impressions")
  base_by_campaign_json=$(fetch_by_campaign)
  base_c1_bid=$(campaign_metric "$base_by_campaign_json" "campaign1" "bid_requests")
  base_c1_dedup=$(campaign_metric "$base_by_campaign_json" "campaign1" "deduped_impressions")
  base_c2_bid=$(campaign_metric "$base_by_campaign_json" "campaign2" "bid_requests")
  base_c2_dedup=$(campaign_metric "$base_by_campaign_json" "campaign2" "deduped_impressions")
  base_cd_bid=$(campaign_metric "$base_by_campaign_json" "campaign_default" "bid_requests")
  base_cd_dedup=$(campaign_metric "$base_by_campaign_json" "campaign_default" "deduped_impressions")
  base_unknown_unknown=$(campaign_metric "$base_by_campaign_json" "UNKNOWN" "unknown_impressions")

  run_http_contract_checks
  wait_for_http_chain_convergence "$base_pg_bids" "$base_pg_impressions" "$base_summary_bids" "$base_summary_dedup" "$base_summary_unknown" \
    "$base_c1_bid" "$base_c1_dedup" "$base_c2_bid" "$base_c2_dedup" "$base_cd_bid" "$base_cd_dedup" "$base_unknown_unknown"

  section "Late bid correction check"
  local late_bid_id late_campaign_id late_user_id late_ts imp_topic_before bid_topic_before
  local late_base_pg_bids late_base_pg_impressions late_base_summary_json late_base_summary_bids late_base_summary_dedup late_base_summary_unknown
  local late_base_campaign_json late_base_campaign_bid late_base_campaign_dedup late_base_unknown_unknown
  late_bid_id=$(python3 - <<'PY'
import uuid
print(str(uuid.uuid4()))
PY
)
  late_campaign_id="campaign_late_test"
  late_user_id="late_user_test"
  late_ts=$(date +%s)

  late_base_pg_bids=$(postgres_scalar "select count(*) from bids;")
  late_base_pg_impressions=$(postgres_scalar "select count(*) from impressions;")
  late_base_summary_json=$(fetch_summary)
  late_base_summary_bids=$(json_field "$late_base_summary_json" "bid_requests")
  late_base_summary_dedup=$(json_field "$late_base_summary_json" "deduped_impressions")
  late_base_summary_unknown=$(json_field "$late_base_summary_json" "unknown_impressions")
  late_base_campaign_json=$(fetch_by_campaign)
  late_base_campaign_bid=$(campaign_metric "$late_base_campaign_json" "$late_campaign_id" "bid_requests")
  late_base_campaign_dedup=$(campaign_metric "$late_base_campaign_json" "$late_campaign_id" "deduped_impressions")
  late_base_unknown_unknown=$(campaign_metric "$late_base_campaign_json" "UNKNOWN" "unknown_impressions")

  imp_topic_before=$(topic_count "impressions")
  produce_kafka_value "impressions" "$late_bid_id" "{\"bid_id\":\"${late_bid_id}\",\"campaign_id\":\"\",\"user_idfv\":\"\",\"timestamp\":${late_ts}}"
  assert_topic_delta "$imp_topic_before" "$(topic_count "impressions")" 1 "direct impression injection appends one impression event"
  wait_for_late_unknown_state "$late_bid_id" "$late_base_pg_impressions" "$late_base_summary_unknown" "$late_base_unknown_unknown"

  bid_topic_before=$(topic_count "bid-requests")
  produce_kafka_value "bid-requests" "$late_bid_id" "{\"request_id\":\"late-bid-request\",\"bid_id\":\"${late_bid_id}\",\"user_idfv\":\"${late_user_id}\",\"campaign_id\":\"${late_campaign_id}\",\"placement_id\":\"late-placement\",\"timestamp\":${late_ts}}"
  assert_topic_delta "$bid_topic_before" "$(topic_count "bid-requests")" 1 "late bid injection appends one bid event"
  wait_for_late_correction "$late_bid_id" "$late_campaign_id" "$late_base_pg_bids" "$late_base_pg_impressions" "$late_base_summary_bids" "$late_base_summary_dedup" "$late_base_summary_unknown" "$late_base_campaign_bid" "$late_base_campaign_dedup" "$late_base_unknown_unknown"

  section "Redis backfill rebuild check"
  wait_for_idle_projection
  local expected_summary_json expected_campaigns_json expected_pg_bids expected_pg_impressions
  expected_summary_json=$(canonical_summary "$(fetch_summary)")
  expected_campaigns_json=$(canonical_campaigns "$(fetch_by_campaign)")
  expected_pg_bids=$(postgres_scalar "select count(*) from bids;")
  expected_pg_impressions=$(postgres_scalar "select count(*) from impressions;")
  run_go_tool ./cmd/backfill_redis
  wait_for_backfill_rebuild "$expected_summary_json" "$expected_campaigns_json" "$expected_pg_bids" "$expected_pg_impressions"

  echo
  echo "Pipeline behavior verification passed: HTTP side effects, late bid correction, and Redis backfill rebuild."
}

main "$@"
