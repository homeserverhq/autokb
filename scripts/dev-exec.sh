#!/usr/bin/env bash
# Run a command inside the dev manager container, e.g.:
#   scripts/dev-exec.sh python /src/testing/test_runner.py
set -euo pipefail
cd "$(dirname "$0")/.."
SERVICE="${AUTOKB_DEV_EXEC_SERVICE:-autokb-dev-manager}"
exec docker exec -i \
  -e MANAGER_URL="${MANAGER_URL:-http://autokb-dev-manager:80}" \
  "$SERVICE" "$@"
