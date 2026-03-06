#!/bin/bash
set -euo pipefail

BASE_URL=${1:-"http://localhost:8080"}
shift || true

echo "Running Deliverable A load generation against ${BASE_URL}"

go run ./cmd/loadgen \
  --base-url "${BASE_URL}" \
  "$@"

echo "Deliverable A load generation completed."
