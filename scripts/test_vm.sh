#!/bin/bash
set -e

BASE_URL=${1:-"http://34.143.249.40:8080"}


# We can reuse the same test script logic by delegating to test_local.sh
# just passing the base URL
./scripts/test_local.sh "$BASE_URL"
