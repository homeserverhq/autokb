"""Shared constants used across Manager, Worker, Web UI, and MCP.

All constants can be overridden through environment variables. See the
DesignSpecification.md for the canonical default values.
"""

import os

# ---------------------------------------------------------------------------
# Timeouts (seconds)
# ---------------------------------------------------------------------------
HEARTBEAT_TIMEOUT = int(os.environ.get("AUTOKB_HEARTBEAT_TIMEOUT", "300"))
"""Maximum seconds between plugin progress_callback() calls before the watcher
thread terminates the child process."""

MONITOR_ERROR_SLEEP = int(os.environ.get("AUTOKB_MONITOR_ERROR_SLEEP", "10"))
"""Seconds to sleep after a monitor() raises a non-NotImplementedError
exception before retrying the loop."""

LOCK_TTL = int(os.environ.get("AUTOKB_LOCK_TTL", "3600"))
"""Redis safety-lock TTL (seconds) used to ensure one worker per subscription.
Also refreshed to this full value on every progress_callback heartbeat."""

DEBOUNCE_SECONDS = int(os.environ.get("AUTOKB_DEBOUNCE_SECONDS", "2"))
"""File-watcher debounce delay before reloading a modified plugin."""

DEBOUNCE_PHASE_SECONDS = int(os.environ.get("AUTOKB_DEBOUNCE_PHASE_SECONDS", "5"))
"""Worker 'debounce phase' sleep after a successful execution before re-eval."""

WATCHDOG_INTERVAL = int(os.environ.get("AUTOKB_WATCHDOG_INTERVAL", "5"))
"""Manager watchdog poll interval (seconds)."""

SSE_KEEPALIVE_SECONDS = int(os.environ.get("AUTOKB_SSE_KEEPALIVE", "30"))
"""SSE keepalive comment interval (seconds)."""

STARTUP_RETRY_SLEEP = int(os.environ.get("STARTUP_RETRY_SLEEP", "1"))
MAX_STARTUP_RETRIES = int(os.environ.get("MAX_STARTUP_RETRIES", "100"))

# ---------------------------------------------------------------------------
# Derived
# ---------------------------------------------------------------------------
WATCHDOG_TIMEOUT_S = HEARTBEAT_TIMEOUT * 3
"""Computed watchdog timeout. Used by the Manager to detect stale
IN_PROGRESS subscriptions (stuck executions whose worker died). ENQUEUED
rows are intentionally not flagged — they are waiting on a free worker and
have no heartbeat by construction."""

# ---------------------------------------------------------------------------
# Sink operation types (queue items)
# ---------------------------------------------------------------------------
OPERATION_FULL = "FULL"
OPERATION_SINK_ONLY = "SINK_ONLY"
ALL_OPERATIONS = (OPERATION_FULL, OPERATION_SINK_ONLY)

# ---------------------------------------------------------------------------
# Migration / startup
# ---------------------------------------------------------------------------
WORKER_STARTUP_DELAY_S = int(os.environ.get("WORKER_STARTUP_DELAY_S", "5"))
"""Delay before worker initializes Sink components (to let Manager run migrations)."""

# ---------------------------------------------------------------------------
# Queue keys
# ---------------------------------------------------------------------------
P_QUEUE_KEY = "autokb:p_queue"
S_QUEUE_KEY = "autokb:s_queue"
LOCK_KEY_PREFIX = "autokb:lock:"
NOTIFY_CHANNEL = "subscription_updated"
DELETE_PUSH_CHANNEL = "autokb:delete_push"

# ---------------------------------------------------------------------------
# Subscription states
# ---------------------------------------------------------------------------
STATE_ENABLED = "ENABLED"
STATE_ENQUEUED = "ENQUEUED"
STATE_IN_PROGRESS = "IN_PROGRESS"
STATE_ERROR = "ERROR"
STATE_DISABLED = "DISABLED"
STATE_DELETED = "DELETED"

ALL_STATES = (
    STATE_ENABLED,
    STATE_ENQUEUED,
    STATE_IN_PROGRESS,
    STATE_ERROR,
    STATE_DISABLED,
    STATE_DELETED,
)

# States a subscription may be in to be *enqueued* (cleanup allowed for DELETED).
ENQUEUEABLE_STATES = (STATE_ENABLED, STATE_ENQUEUED, STATE_IN_PROGRESS, STATE_DELETED)

# States a subscription may be in to be *triggered* by a user.
TRIGGERABLE_STATES = (STATE_ENABLED, STATE_ENQUEUED, STATE_IN_PROGRESS)

# States the system may legally transition to via update_status without
# being blocked by a user-paused or system-set state.
ERROR_SAFE_STATES = ("ENQUEUED", "IN_PROGRESS", "ENABLED")

# ---------------------------------------------------------------------------
# Exit codes (EventLog)
# ---------------------------------------------------------------------------
EXIT_SUCCESS = 0
EXIT_RUNTIME_ERROR = 1
EXIT_TIMEOUT = 2
EXIT_SCHEMA_VALIDATION = 3

# ---------------------------------------------------------------------------
# Sub-types
# ---------------------------------------------------------------------------
SUB_TYPE_SCHEDULED = "SCHEDULED"
SUB_TYPE_EVENT_BASED = "EVENT_BASED"

# ---------------------------------------------------------------------------
# Access levels
# ---------------------------------------------------------------------------
ACCESS_PRIVATE = "PRIVATE"
ACCESS_PUBLIC = "PUBLIC"

# ---------------------------------------------------------------------------
# Misc
# ---------------------------------------------------------------------------
EXTRA_PARAM_KEYS = ("_extra_param_1", "_extra_param_2", "_extra_param_3")

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

# ---------------------------------------------------------------------------
# Reserved plugin names (AUTOKB_RESERVED_DSN)
# ---------------------------------------------------------------------------
_RESERVED_RAW = os.environ.get("AUTOKB_RESERVED_DSN", "")
AUTOKB_RESERVED_NAMES: set = {s.strip() for s in _RESERVED_RAW.split(",") if s.strip()}

# ---------------------------------------------------------------------------
# Connection URLs (re-exported from environment for convenience)
# ---------------------------------------------------------------------------
REDIS_URL = os.environ.get("REDIS_URL", "redis://autokb-redis:6379/0")
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://autokb:autokb@autokb-db:5432/autokb")
