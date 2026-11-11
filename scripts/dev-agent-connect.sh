#!/usr/bin/env bash
# Attach the agent container to the private dev network so it can reach
# autokb-dev-web/manager by DNS without host port mappings or shared networks.
set -euo pipefail
SELF="$(hostname)"
docker network connect autokb-dev-net "$SELF" 2>/dev/null || true
echo "agent container '$SELF' attached to autokb-dev-net"
