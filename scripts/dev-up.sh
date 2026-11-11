#!/usr/bin/env bash
# Start the isolated development stack. Always uses the explicit project name
# and dev compose file so it can never touch the production `autokb` project.
set -euo pipefail
cd "$(dirname "$0")/.."
export AUTOKB_DEV_HOST_ROOT="${AUTOKB_DEV_HOST_ROOT:-/home/bolt/hshq/data/stacks/opencode/repos/autokb}"
mkdir -p .dev/output .dev/logs .dev/plugins .dev/sinks
exec docker compose -p autokb-dev -f docker-compose.dev.yml \
  --env-file stack.dev.env up -d --build "$@"
