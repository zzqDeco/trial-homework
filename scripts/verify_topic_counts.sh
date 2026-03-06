#!/bin/bash
set -euo pipefail

BROKERS=${BROKERS:-"localhost:9092"}
MIN_BIDS=${MIN_BIDS:-10001}
MIN_IMPRESSIONS=${MIN_IMPRESSIONS:-10001}

if [[ $# -ge 1 ]]; then
  BROKERS="$1"
fi

if [[ $# -ge 2 ]]; then
  MIN_BIDS="$2"
fi

if [[ $# -ge 3 ]]; then
  MIN_IMPRESSIONS="$3"
fi

echo "Verifying topic counts with brokers=${BROKERS}, min_bids=${MIN_BIDS}, min_impressions=${MIN_IMPRESSIONS}"

go run ./cmd/verify_topic_counts \
  --brokers "${BROKERS}" \
  --min-bids "${MIN_BIDS}" \
  --min-impressions "${MIN_IMPRESSIONS}"
