#!/usr/bin/env bash
# DESTRUCTIVE: remove the dev stack's containers, network, and DB volume.
# This only affects resources labelled com.docker.compose.project=autokb-dev.
set -euo pipefail
cd "$(dirname "$0")/.."
exec docker compose -p autokb-dev -f docker-compose.dev.yml \
  --env-file stack.dev.env down -v --remove-orphans "$@"
