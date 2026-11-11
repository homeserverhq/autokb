#!/usr/bin/env bash
# Stop the isolated development stack. Never removes volumes.
set -euo pipefail
cd "$(dirname "$0")/.."
exec docker compose -p autokb-dev -f docker-compose.dev.yml \
  --env-file stack.dev.env down "$@"
