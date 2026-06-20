# Design Specification

**Project Name:** AutoKB
**Version:** 2.8.0

---

### 1. Project Vision & Core Objective

**AutoKB** is a distributed, event-driven ETL (Extract, Transform, Load) orchestration engine. Its primary purpose is to connect to diverse data sources, pull data, and write it to files (or chunks) for consumption by external Knowledge Base importers.

The system is unique in its **Pluggable Architecture** (adding a source is as simple as dropping a file) and its **Two-Tiered Interface**:

1. **Human Interface (Web UI):** A highly reactive dashboard for manual management.
2. **AI Interface (MCP Server):** A semantic proxy allowing AI assistants to act as remote administrators.

### 2. System Architecture & Infrastructure

#### 2.1 Repository & Container Directory Structure

```
/ (repo root)
├── DesignSpecification.md                # This document
├── docker-compose.yml                    # Pre-generated
├── Dockerfile                            # Unified base image (Manager, Worker, Web UI)
├── requirements.txt                      # Python requirements
├── mcp/
│   ├── Dockerfile                        # MCP-specific image
│   ├── pyproject.toml                    # MCP-specific dependencies
│   ├── logs/                             # MCP log directory
│   └── src/
│       ├── main.py                       # FastMCP entrypoint & tool definitions
│       └── client.py                     # Authenticated API client
├── src/
│   ├── manager/
│   │   ├── manager.py                    # FastAPI entrypoint & plugin loader
│   │   ├── scheduler.py                  # Cron & trigger coordination
│   │   ├── registry.py                   # Manager-specific registry wrapper
│   │   ├── routes.py                     # Standard CRUD routes
│   │   ├── dynamic_mount.py              # Logic to mount plugin endpoints
│   │   └── alembic/
│   │       ├── alembic.ini
│   │       ├── env.py
│   │       └── versions/                 # Migration scripts
│   ├── worker/
│   │   ├── worker.py                     # Multiprocessing entrypoint
│   │   └── execution_engine.py           # The Managed Execution Wrapper with heartbeat timeout
│   ├── web/
│   │   ├── app.py                        # aiohttp application with /api/ reverse proxy
│   │   ├── templates/                    # SPA components/HTML
│   │   └── static/                       # JS/CSS
│   ├── plugins/
│   │   ├── happyPathPlugin.py            # Test: normal success path
│   │   ├── eventHappyPlugin.py           # Test: EVENT_BASED success
│   │   ├── noHeartbeatPlugin.py          # Test: heartbeat timeout → ERROR
│   │   ├── longRunningSuccessPlugin.py   # Test: long execution with regular heartbeats → success
│   │   ├── longRunningFailurePlugin.py   # Test: long execution → runtime error
│   │   ├── crashPlugin.py                # Test: immediate exception → ERROR
│   │   ├── cancellationPlugin.py         # Test: graceful cancellation mid-execution
│   │   ├── schemaBreakingPlugin.py       # Test: breaking schema change (modified in-place by test runner)
│   │   ├── passwordPlugin.py             # Test: password field encryption
│   │   ├── emptyOutputPlugin.py          # Test: no output files
│   │   ├── largeOutputPlugin.py          # Test: large file generation and cleanup
│   │   ├── delayedInitPlugin.py          # Test: long initialization before first heartbeat
│   │   ├── customRoutePlugin.py          # Test: custom API routes
│   │   ├── invalidNamePlugin.py          # Test: sanitize_name rejection
│   │   ├── monitorNeverTriggerPlugin.py  # Test: silent event failure → fallback cron
│   │   ├── monitorErrorPlugin.py         # Test: monitor exceptions → retry loop
│   │   ├── configValidationPlugin.py     # Test: all schema field types
│   │   ├── nonZeroExitPlugin.py          # Test: sys.exit(1) without Python exception
│   │   ├── zombiePlugin.py               # Test: ignores cancellation, force-killed by watcher
│   │   ├── moveToDestErrorPlugin.py      # Test: invalid output path → ValueError
│   │   ├── imapFolderWatchPlugin.py      # Production: IMAP IDLE email folder watcher
│   │   ├── editMatchPlugin.py            # Test: Edit Plugin schema-stability (match/mismatch)
│   │   ├── eventOftenPlugin.py           # Test: EVENT_BASED, fires on enable + every 42s
│   │   └── longNamePlugin32CharNameForUITes.py  # Test: 32-char plugin name UI layout
│   ├── testing/
│   │   ├── test_runner.py                # Automated test suite (confirm/--reset guard)
│   │   └── plugins/                      # Self-contained test plugin copies (synced to /src/plugins/ at test startup)
│   │       ├── happyPathPlugin.py
│   │       ├── eventHappyPlugin.py
│   │       ├── ...                       # 25 test plugins total (mirrors /src/plugins/ test subset)
│   │       └── zombiePlugin.py
│   └── utils/
│       ├── database.py                   # SQLAlchemy/Postgres models & logic
│       ├── queue_utils.py                # Redis P-Queue/S-Queue/Locking logic
│       ├── constants.py                  # Shared constants (HEARTBEAT_TIMEOUT, MONITOR_ERROR_SLEEP, etc.)
│       ├── misc_utils.py                 # File I/O, sanitization, SMTP logic, TOON
│       ├── plugin_base.py                # The abstract base class (The Contract)
│       └── registry.py                   # Shared plugin validation & registry engine
├── assets/                               # Icons (pre-populated; default_icon.png included)
├── output/                               # Final output (mounted volume)
└── logs/                                 # System logs (mounted volume)
```

**Python path:** `PYTHONPATH=/src` is set in the unified Dockerfile at build time (Manager, Worker, Web UI). The MCP server uses its own image with `PYTHONPATH=/mcp/src`.

**Container naming convention:** Container names use the `autokb-` prefix consistently across all services.

#### 2.2 Container Orchestration

The project runs across **six (6) Docker containers**. The Manager, Worker, and Web UI share a single unified base image with all heavy dependencies (e.g., langchain, beautifulsoup4, imapclient, etc.) to ensure environment parity. The MCP Server uses a separate, lighter image. Redis and PostgreSQL use their official images.

**Startup Retry Logic:** On startup, the Manager and Worker each retry connecting to both Redis and PostgreSQL until successful, using `MAX_STARTUP_RETRIES` (default: 100) with `STARTUP_RETRY_SLEEP` (default: 1 second) between attempts. Each failed attempt is logged to the container's log file. If all retries are exhausted, the container exits with a non-zero status. The Web UI polls `http://autokb-manager:80/api/health` using the same retry pattern before serving external requests — it does not start accepting connections until the Manager is reachable. The MCP Server is exempt from this startup sequencing (it retries on demand).

All HTTP containers listen on port **80** internally. External ports are mapped via docker-compose. Docker bridge networking isolates each container — port 80 is the internal container port, not a host port conflict.

| Container | Container Name | Role | Startup Command | Image |
|---|---|---|---|---|
| **Subscription Manager** | `autokb-manager` | The "Brain" — API, Plugin Discovery, Scheduling, Routing | `python /src/manager/manager.py` | Unified |
| **Worker** | `autokb-worker` | The "Muscle" — executes plugin logic using multiprocessing | `python /src/worker/worker.py` | Unified |
| **MCP Server** | `autokb-mcp` | The "AI Proxy" — exposes management tools to AI Assistants | `python -m main` | Separate |
| **Web UI** | `autokb-web` | The "Dashboard" — aiohttp SPA with proxied API calls | `gunicorn web.app:app --bind 0.0.0.0:${WEBUI_PORT} --worker-class aiohttp.worker.GunicornWebWorker` | Unified |
| **Redis** | `autokb-redis` | The "Message Bus" — Primary/Secondary queues, Locks | `redis-server` | Official |
| **PostgreSQL** | `autokb-db` | The "Memory" — persistent storage for all configs and logs | `postgres` | Official |

The Manager, Worker, and Web UI containers all share the same base Docker image. The Worker builds its own `PluginRegistry` independently at startup using the same shared utility code as the Manager — it never calls the Manager API. The Worker's registry build performs identical schema-hash comparison as the Manager (computes `sha256` of the augmented `get_schema()` output and compares against `PluginRegistryState.schema_hash` in the database). If a breaking change is detected, the Worker refuses to load the plugin. SMTP notifications for registry-level schema-breaking changes are skipped at startup — the Manager's file watcher handles those. Execution-time SMTP (heartbeat timeout, runtime error, schema validation failure) is sent by the Worker's Level-1 parent process, which receives exception details from the child process via the `exception_queue`.

#### 2.3 Port Convention

| Env Variable | Default | Container |
|---|---|---|
| `MANAGER_PORT` | `80` | Manager (FastAPI) |
| `WEBUI_PORT` | `80` | Web UI (gunicorn) |
| `MCP_PORT` | `80` | MCP Server (uvicorn) |

The Worker has no HTTP layer and does not require a port.

#### 2.4 Docker-Managed Named Volumes

All mounted volumes use docker-managed named volumes (not bind mounts). This ensures that pre-existing files in the image are auto-populated into the volume on first use.

| Volume Mount | Pre-Populated Contents | Used By |
|---|---|---|
| `/src/plugins` | `happyPathPlugin.py`, `eventHappyPlugin.py`, etc | Manager, Worker |
| `/assets` | Plugin icons + `default_icon.png` | Web UI, Manager |
| `/output` | (empty) | Manager, Worker |
| `/logs` | (empty) | Manager, Worker, MCP, Web UI |

Note: The MCP server mounts `/logs` for its own log file but does not mount `/src/plugins`, `/assets`, or `/output`. Plugins always reside at `/src/plugins/` within the Manager and Worker containers. The dev_lab save endpoint writes to `/src/plugins/{filename}`.

#### 2.5 Logging Per Container

Each container writes to its own dedicated log file:

| Container | Log File |
|---|---|
| Manager | `/logs/manager.log` |
| Worker | `/logs/worker.log` |
| MCP | `/logs/mcp.log` |
| Web UI | `/logs/web.log` |

`/logs` captures all debugging, system errors, and operational messages. The Event Log database table is used exclusively for counting subscription execution events over time (24h activity monitor).

### 3. The Plugin Architecture (The "Contract")

Every data source must be a single Python file in `/src/plugins/`.

#### 3.1 Naming Conventions

- Plugin filenames use **camelCase** (e.g., `biblePlugin.py`, `emailPlugin.py`)
- `plugin.metadata["name"]` must match the filename (without `.py` extension)
- `plugin.metadata["name"]` is capped at **32 characters** (`MAX_PLUGIN_NAME_LEN`). This keeps plugin grid cards from overflowing on standard viewport widths. The Dev Lab rejects names longer than this with HTTP 400.
- `plugin_id` = `sanitize_name(metadata["name"])` — used consistently everywhere (API paths, directory names, registry keys)
- All strings used for directory creation and output data filenames must pass through `sanitize_name`

#### 3.2 The `sanitize_name()` Function

Strips all characters except `[a-zA-Z0-9.\-]`. If a period is present, the first and last characters must be non-period (alphanumeric). Consecutive periods are not permitted. Raises `ValueError` if the input has no valid content after sanitization or if the period constraints are violated. Callers must catch `ValueError`.

The function is idempotent (applying it twice yields the same result). All stored names must pass the idempotency check: `sanitize_name(name) == name`.

#### 3.3 The `BaseSubscription` Abstract Class

To minimize boilerplate, the base class handles the heavy lifting.

**A. Mandatory Implementation (Abstract Methods & Required Variables)**

1. **`metadata`**: `dict` containing `{"name", "icon", "description", "sub_type"}`. Defined as a **class-level** variable override in the subclass. `sub_type` must be either `"SCHEDULED"` or `"EVENT_BASED"` and determines the subscription type for all subscriptions using this plugin. `metadata["description"]` is the **plugin description** — it describes the data source type (e.g., "Fetches emails from IMAP"). This is displayed in the plugin grid cards and as a separate column in the subscription list. The subscription's own description is stored independently in the subscription config's `description` field (see §7.5).

2. **`DEFAULT_ACCESS_LEVEL`**: `str` — class-level variable. Must be either `"PRIVATE"` or `"PUBLIC"`. Determines the default access control level for all subscriptions created under this plugin when no explicit `access_level` is provided at creation time. The base class provides a safe default of `"PRIVATE"`, but plugin authors **should** set it explicitly to reflect the nature of their data source (e.g., `biblePlugin.py` would set `"PUBLIC"` since biblical texts are public domain, while `emailPlugin.py` would set `"PRIVATE"` since emails are user-specific). Validated at plugin load time — invalid values cause the plugin to be rejected with a clear error message.

3. **`get_schema()`**: Returns a JSON Schema (defining text fields, combo boxes, radio buttons, checkboxes) which the UI uses to build dynamic forms. The system automatically appends three reserved string fields (`_extra_param_1`, `_extra_param_2`, `_extra_param_3`) to every plugin's schema before any validation occurs. Plugin authors do not need to declare these. **The schema returned by this method is treated as a contract. Any change to this schema is treated as a breaking change — existing subscriptions using this plugin are DISABLED, an SMTP notification is sent, and the plugin is refused loading.**

   **Extra Parameters invariant (system-enforced):** every subscription's persisted `config` JSONB must contain `_extra_param_1`, `_extra_param_2`, and `_extra_param_3` as string-typed keys. The persistence layer (`DatabaseManager.create_subscription` / `update_subscription` in `src/utils/database.py`) auto-injects any missing key with the default empty string `""` at write time, and `DatabaseManager.backfill_extra_params()` runs once on manager startup to bring legacy rows into compliance. The Create / Edit forms never require the user to provide these values — they are rendered as a separate "Extra Parameters" group (see §7.6) but submitting the form without touching them is fine; the backend fills them in. Existing values are preserved, so a plugin that later adopts one of these fields for a new credential keeps the value the user (or a future migration script) assigned to it. **Purpose:** the 3 extra params are placeholders for future data-source schema evolution. If a data provider later changes its requirements (e.g. adds a mandatory API key), the new credential can be assigned to one of these fields instead of forcing every operator to delete and recreate their subscriptions, plugins, and output data. Because plugin schemas are immutable contracts (§3.3 point 3), the extra params are the only forward-compatible extension point for stored subscription config.

   **Schema validation scope:** All JSON Schema constraints are validated at every write (create, edit, and execution-time re-validation), including both structural types (`type`, `format`, `enum`) and value-level constraints (`minLength`, `maxLength`, `pattern`, `minimum`, `maximum`, `required`). For password-format fields, validation is performed on the plaintext value at create and edit time (before encryption) and on the decrypted value at execution time.

   Schema fields with `"format": "password"` are encrypted at rest (see §9.1). Password-format fields are **excluded from all API GET, SSE, and Edit GET responses** — they are never present in any JSON payload returned by the system. The Edit form renders blank password inputs.
4. **`get_custom_routes()`**: Returns a list of `PluginRoute` dataclass instances defining unique API endpoints for the plugin. Each `PluginRoute` has `path` (str), `method` (str), and `handler` (callable). **A concrete method returning `[]` by default.** Plugin authors only override if they need custom routes.

5. **`getData(config, progress_callback=None)`**: The core logic. It must:
   - Fetch/Scrape/Stream data.
   - Perform custom chunking/parsing.
   - Write results to temporary files (recommended location: `/tmp/` — see `move_to_destination` below).
   - For incremental updates, call `self.get_destination_path()` to read existing output files from previous runs and compare against remote source data. Only write files that have changed rather than regenerating everything on every pass.
   - Call `self.move_to_destination(temp_file_path)`.
   - This method is **synchronous**. It runs inside a child `multiprocessing.Process` spawned by the worker.
   - The `config` parameter is the raw JSONB from the database (pre-validated against `get_schema()` at creation/update time).
   - The `progress_callback` is a **mandatory** callable accepting an integer percentage (0–100). Plugin authors **must** call it periodically throughout execution to prevent heartbeat timeout. The system automatically calls it with 0 before execution and 100 upon successful completion, but the plugin must call it with intermediate values during long-running operations to signal liveness. When called, it updates both the database heartbeat timestamp and the shared memory event for the watcher thread. Failure to call `progress_callback` within the heartbeat timeout window (default: 300s) will result in the process being terminated and the subscription marked as ERROR. **Additionally, frequent `progress_callback` calls (at least once per `HEARTBEAT_TIMEOUT / 10` seconds) ensure the watcher can detect cancellation within seconds.** If the plugin only calls `progress_callback` rarely, the watcher's DB status check still catches DISABLED/DELETED on its ~30s tick, but the child process may continue executing until the next tick. Plugin authors should call `progress_callback` in their main processing loop to keep cancellation latency minimal.
   - **Cancellation contract (CRITICAL):** The `progress_callback` may raise `SubscriptionCancelledError` (imported from `utils.misc_utils`) if the subscription has been disabled or deleted while executing. This exception is the mechanism by which the system signals the plugin to halt gracefully. **Plugin authors MUST NOT catch `SubscriptionCancelledError`** (or catch it and re-raise it). If a plugin's `except Exception` handler catches this exception and suppresses it, the child process will continue executing instead of exiting, causing the subscription to become stuck — the watcher thread will eventually force-terminate the process after a full heartbeat timeout, but by then the subscription may be unresponsive to user actions (disable/update/delete). Any broad `except` clause in plugin code must either exclude `SubscriptionCancelledError` or explicitly re-raise it:

     ```python
     from utils.misc_utils import SubscriptionCancelledError

     # CORRECT — SubscriptionCancelledError propagates naturally:
     try:
         result = external_api.call()
     except ValueError as e:
         logger.warning(f"API warning: {e}")
         continue

     # CORRECT — explicit re-raise after broad catch:
     try:
         result = external_api.call()
     except Exception as e:
         if isinstance(e, SubscriptionCancelledError):
             raise
         logger.warning(f"API warning: {e}")
         continue

     # WRONG — SubscriptionCancelledError is swallowed:
     try:
         result = external_api.call()
     except Exception as e:
         logger.warning(f"API warning: {e}")  # child continues executing, stuck
         continue
     ```

**B. Optional Implementation**

1. **`monitor(config, cancel_token) -> bool`**: An `async` method for **EVENT_BASED** plugins. The scheduler calls this in an asyncio task per event-based subscription. The plugin watches for new data availability (e.g., IMAP IDLE, websocket, file watcher) and, when triggered, returns `True` to signal the coordinator to enqueue a re-run. Returns `False` to continue waiting. The `cancel_token` is an `asyncio.Event` set by the scheduler when the subscription is deleted, disabled, or edited — the plugin should use `cancel_token.is_set()` or `wait()` to interrupt long-blocking operations. The default implementation in the base class raises `NotImplementedError`.

   **Critical constraint:** Because all `monitor()` tasks run as coroutines in the Manager's single-threaded asyncio event loop, the `monitor()` method **must never call blocking synchronous I/O**. Only asyncio-native libraries (e.g., `aioimaplib` instead of `imaplib`, `aiohttp` instead of `requests`, `asyncssh` instead of `paramiko`) are permitted. Calling a blocking function will stall ALL other coroutines (API handlers, SSE keepalives, other monitors, watchdog). Plugin authors are responsible for verifying their libraries are asyncio-compatible before deploying an EVENT_BASED plugin.

   **Exception handling:** The monitor loop wraps the call in try/except. If `monitor()` raises a non-`NotImplementedError` exception, it is logged, and the loop sleeps for `MONITOR_ERROR_SLEEP` seconds (defined in `src/utils/constants.py`, default: 10) before retrying to prevent tight-loop spinning.

**C. Provided Utility (Base Class Method)**

- **`move_to_destination(temp_file_path)`**: Moves files to the correct directory in `/output`. The plugin name, the internally tracked subscription name (`self._subscription_name`), AND output filename are each passed through `sanitize_name`. If `sanitize_name` raises `ValueError` (e.g., plugin wrote to a path with no name-bearing characters), the exception propagates to the child process, causing it to exit with a non-zero status. The Worker's parent process catches this as an error and sets status to ERROR. Target path: `output/{sanitized_plugin_name}/{sanitized_subscription_name}/{sanitized_filename}`. The target directory is created via `os.makedirs(target_dir, exist_ok=True)`.

   **Plugin author guidance:** Write output files to `/tmp/` during generation and only call `move_to_destination()` after the file is fully written and ready for consumption. This prevents partially-written files from appearing in `/output/` in the event of a cancellation or crash. The container filesystem manages `/tmp` lifecycle.

- **`get_destination_path() -> str`**: Returns the output directory path for this subscription: `/output/{sanitized_plugin_name}/{sanitized_subscription_name}/`. Plugin authors call this inside `getData()` to list and inspect existing output files from prior executions. Useful for incremental sync patterns where only changed files need to be written. The path is valid only after the Managed Execution Wrapper has set `_subscription_name` (i.e., during `getData()`).

**D. Internal Attributes (Set by the Managed Execution Wrapper)**

The base class declares the following internal attributes for use by `move_to_destination()` and `get_destination_path()`:

```python
_heartbeat_event: Optional[multiprocessing.Event] = None
_subscription_id: str
_subscription_name: str
```

The Managed Execution Wrapper (running inside the child `multiprocessing.Process`) sets these attributes before calling `getData()`. `move_to_destination()` uses `self._subscription_name` to construct the output path. Plugin authors do not need to interact with them directly.

#### 3.4 Dependency Management

All likely dependencies (e.g., `imapclient`, `aioimaplib`, `tiktoken`, `langchain_text_splitters`, `httpx`, `beautifulsoup4`, `uuid7`, `asyncpg`) are pre-installed in the unified base Docker image to maintain a single-image architecture for Manager, Worker, and Web UI. The MCP Server has its own image with its own set of dependencies. `aioimaplib` provides asyncio-compatible IMAP IDLE support (used by `imapFolderWatchPlugin`); `tiktoken` (pinned to `==0.13.0`) provides token counting for the chunking logic.

**No external network calls at runtime.** The `int-autokb-net` Docker network is `internal: true` (no internet access). The tiktoken BPE vocabulary (`cl100k_base`, ~10MB) is pre-cached into `/data/tiktoken-cache/` during `docker build` via `TIKTOKEN_CACHE_DIR`, so `tiktoken.get_encoding()` never attempts a download at runtime. `langsmith` telemetry (transitive dep of `langchain-text-splitters`) is explicitly disabled via `LANGCHAIN_TRACING_V2=false` and `LANGCHAIN_API_KEY=` in `stack.env`. All outbound network access is limited to user-configured SMTP (via `dock-internalmail-net`) and IMAP connections.

### 4. Subscription Types & States

#### 4.1 Subscription Types

Two subscription types, defined by the plugin author in the plugin's `metadata["sub_type"]`:

| Type | Behavior |
|---|---|
| `SCHEDULED` | Polled via cron expression. Default cron: `0 * * * *` (every hour). Uses the cron loop in the trigger coordinator. |
| `EVENT_BASED` | Event-driven via `monitor()` method. Can optionally include a `cron` field as a **fallback polling mechanism** — if the eventing mechanism fails, the subscription is still guaranteed to update at a lower frequency. |

`sub_type` is inherited from the plugin; only `cron` and optionally `access_level` are specified at creation:
```json
{
  "name": "My Subscription",
  "config": { "...": "..." },
  "cron": "0 * * * *",
  "access_level": "PUBLIC"
}
```

If `access_level` is not provided at creation, it defaults to the plugin's `DEFAULT_ACCESS_LEVEL`. Valid values are `"PRIVATE"` and `"PUBLIC"`. The `access_level` can be updated after creation via the Edit endpoint.

- `sub_type` is determined by the plugin's `metadata["sub_type"]` — all subscriptions for a plugin share the same type.
- For `SCHEDULED`: cron defaults to `0 * * * *` if not provided.
- For `EVENT_BASED`: cron defaults to `0 0 * * *` (daily) if not provided. If provided, it acts as a backup polling interval.
- **Cron Randomization:** When the cron expression matches a default pattern (`0 * * * *` for SCHEDULED or `0 0 * * *` for EVENT_BASED), the Manager automatically randomizes it at creation time to spread subscription load. The `0 * * * *` pattern becomes `{M} * * * *` (random minute 0-59), and `0 0 * * *` becomes `{M} {H} * * *` (random hour 0-23, random minute 0-59). This only applies to new subscriptions; editing preserves the stored cron value. The default strings `0 * * * *` and `0 0 * * *` are never stored in the database.
- When both event and cron triggers fire, the Aggressive Collapsing strategy (P-Queue/S-Queue) deduplicates automatically.

**Editing:** `cron` can be updated after creation via the Edit endpoint and the Web UI. Config values can be freely changed via Edit, but the JSON Schema structure cannot be altered. Editing an EVENT_BASED subscription cancels the current monitor loop and restarts it with the new config.

**Cron Validation:** All cron expressions are validated at submission time. The `POST /api/subscriptions/{plugin_id}` and `PUT /api/subscriptions/{sub_id}` endpoints reject invalid cron expressions with HTTP 400 and body `{"error": "Invalid cron expression: {cron}"}`. The Web UI also performs client-side validation before form submission. At Manager startup, the scheduler validates all cron expressions loaded from the database — any subscription with an invalid cron expression is automatically set to `ERROR` with `last_error` set to `"Invalid cron expression: {cron}"`. An SMTP notification is sent for each subscription set to ERROR.

Additionally, when a user transitions a subscription from `ERROR` or `DISABLED` to `ENABLED` via `PUT /api/subscriptions/{sub_id}/status`, the cron expression is re-validated before the transition is accepted. If the cron expression is invalid, the transition is rejected with HTTP 400 and body `{"error": "Invalid cron expression: {cron}"}`.

**Per-minute cron guard:** The scheduler uses a `_same_minute()` helper to debounce cron fallback paths so a cron that matches the current minute (e.g., `* * * * *`) fires at most once per minute, not once per monitor tick. Timezones are normalized to UTC before comparison to prevent cross-timezone false negatives.

#### 4.2 Subscription States

| State | Description |
|---|---|
| `ENABLED` | Baseline active state. Subscription is eligible for triggering. |
| `ENQUEUED` | Subscription is in a queue awaiting execution. |
| `IN_PROGRESS` | Subscription is currently being executed by a Worker. |
| `ERROR` | Subscription encountered an error. Logically identical to DISABLED — system prevents execution. Only user can re-enable. |
| `DISABLED` | User-paused. Not eligible for triggering. Only user can re-enable. |
| `DELETED` | **Final/transient state.** Subscription is pending cleanup. Manager only sets this state; no method or function may transition out of `DELETED`. Worker cleans up the output directory and removes the DB row. |

**ERROR and DISABLED invariants:**
- ERROR and DISABLED are logically identical in every way with respect to execution gating.
- Aside from a few special cases, the only difference is **who sets the state**: the system sets ERROR (automatically on failure), and the user sets DISABLED (manually). Schema-breaking changes (§5.3) are the recognized special case where the system sets DISABLED to signal a structural incompatibility.
- Only user-generated actions (API call via Web UI or MCP) can move a subscription out of either ERROR or DISABLED state.
- Both states prevent the subscription from being enqueued, triggered, or executed.
- Both states are naturally excluded from `try_enqueue()` via its `WHERE status IN ('ENABLED', 'ENQUEUED', 'IN_PROGRESS')` clause.

**State transition rules:**
- Enqueuing is allowed if status is in `(ENABLED, ENQUEUED, IN_PROGRESS, DELETED)`.
- If status is `ERROR`, `DISABLED`, or `DELETED`, trigger requests are discarded.
- `PUT /api/subscriptions/{sub_id}/status` accepts `"ENABLED"` or `"DISABLED"`. Setting `ENABLED` while in `ERROR` transitions to `ENABLED` and clears `last_error`. Setting status on a `DELETED` subscription is rejected with HTTP 400 — the DELETED state is terminal and cannot be modified.
- **DISABLED/ERROR invariant:** System-generated status transitions are unconditionally rejected when the current status is `DISABLED` or `ERROR`. The invariant is enforced via two distinct WHERE clause patterns depending on the transition type:
  - **Error-setting transitions** (heartbeat timeout, runtime exception, schema validation failure, watchdog force-release, outer exception handler) use `WHERE status NOT IN ('DELETED', 'DISABLED')`.
  - **The success-to-ENABLED transition** (Re-eval Phase after successful execution) uses `WHERE status IN ('ENQUEUED', 'IN_PROGRESS')`.
  Work claiming uses `WHERE status IN ('ENQUEUED', 'IN_PROGRESS')`.
  These WHERE clauses ensure automated processes never override a user's pause or a system-set error state. The broader IN clause on success-to-ENABLED ensures that if a user re-enqueued a subscription while it was executing (setting status to ENQUEUED), the worker can still complete its lifecycle when the queues are empty.
- `DELETED` is set by the Manager delete endpoint. Once `DELETED`, no state transition out is possible. The delete endpoint returns HTTP 409 if the subscription is already `DELETED`.

> **Distinction: Enqueuing vs. Triggering:**
> *Enqueuing* (adding a subscription_id to the Primary Queue for the Worker) and *Triggering* (the `POST /api/subscriptions/{sub_id}/trigger` endpoint) are distinct operations governed by different gate conditions:
> - **Enqueuing** is permitted for `DELETED` subscriptions so the Worker can execute cleanup. It is also permitted for `ENABLED`, `ENQUEUED`, and `IN_PROGRESS`.
> - **Triggering** is a user-facing API action that only applies to active subscriptions (`ENABLED`, `ENQUEUED`, `IN_PROGRESS`). Trigger requests for `DELETED`, `ERROR`, or `DISABLED` are rejected with 400.
> - The `try_enqueue()` function handles both paths: it returns `True` for `DELETED` (allowing cleanup) without modifying the DB status, and performs an atomic `UPDATE ... WHERE status IN (...)` for active states. ERROR and DISABLED are naturally excluded.

### 5. Execution Engine (The Worker & Queue Logic)

To handle high-frequency events while maintaining the **Invariant (One Worker per Subscription)**, the system uses an **Aggressive Collapsing** strategy.

#### 5.1 The Two-Tier Queue System

1. **Primary Queue (P-Queue):** The main entry point for all triggers.
2. **Secondary Queue (S-Queue):** A buffer for tasks that occur while a subscription is "In-Flight."

#### 5.2 The Worker Workflow (Multiprocessing + Managed Execution Wrapper)

The system achieves parallelism by spawning N worker processes (configurable via `WORKER_COUNT` env var, default 4). Each Level-1 process runs a persistent loop (outer). Inside a given Level-1 process, `getData()` is spawned as a child Level-2 `multiprocessing.Process` for true parallel execution — CPU-bound and I/O-bound plugins are handled uniformly.

1. **Dequeue & Collapse (outer loop):** Pop from P-Queue. Immediately remove **all** instances of that `subscription_id` from the **P-Queue**.
2. **Lock Attempt (Safety Lock):**
   - Acquire a Redis lock with a safety TTL (`LOCK_TTL`, default: 3600s, defined in `src/utils/constants.py`).
   - **If Lock Fails:** (Another worker is currently processing this ID). Push **one** instance of the ID to the **S-Queue** and move to the next task.
   - **If Lock Succeeds (inner loop):** The Worker enters an inner loop (`while True`) for this subscription_id. All phases below (Drain through Re-eval) execute inside this loop. The loop continues until the Re-eval Phase confirms both queues are empty:      - **Drain Phase:** Pull **all** instances of that ID from **both** the P-Queue and the S-Queue and discard them.
      - **Check DELETED:** If the subscription's current status is `DELETED`, skip execution and proceed directly to cleanup: remove the output directory via `shutil.rmtree`, then delete the DB row. The DB row deletion retries up to 3 times with 1s sleep between attempts. On permanent failure, an error is logged and a P1 SMTP notification is sent so operators can intervene manually. After cleanup, release the lock and continue to the next task.
      - **Check DISABLED:** If the subscription's current status is `DISABLED`, skip execution and proceed to release the lock — DISABLED subscriptions require manual re-enable via the API.
      - **Check ERROR:** If the subscription's current status is `ERROR`, skip execution and proceed to release the lock — ERROR subscriptions require manual re-enable via the API (same as DISABLED).
      - **State Transition:** Immediately update the subscription status in PostgreSQL to `IN_PROGRESS`. Uses `WHERE status IN ('ENQUEUED', 'IN_PROGRESS')` to prevent claiming a subscription whose status was changed concurrently (e.g., to `DISABLED` by a user). Set `last_heartbeat` to `NOW()` at the same time (non-NULL initial value).
      - **Execution Phase:** Fetch config from Postgres → Validate config against plugin's `get_schema()` using `augment_schema()` from `utils/misc_utils` (augmented with `_extra_param_1/2/3`) → Load Plugin. **The Worker loads the plugin fresh for each job using `importlib.util.spec_from_file_location()` + `importlib.util.module_from_spec()` (bypassing Python's `sys.modules` cache).** This ensures every execution uses the latest plugin code on disk. If loading fails validation (syntax error, missing `BaseSubscription` subclass, missing/invalid `DEFAULT_ACCESS_LEVEL`, metadata/filename mismatch, schema hash mismatch), the subscription is set to `ERROR` (using `WHERE status NOT IN ('DELETED', 'DISABLED' )`), an Event Log entry is recorded (exit_code=1), the Worker's Level-1 parent sends an SMTP notification, the lock is released, and the Worker continues to the next task. **If the plugin loads successfully:** Spawn `getData()` as a Level-2 `multiprocessing.Process` with a shared `multiprocessing.Event` for heartbeat signaling and a `multiprocessing.Queue` (`exception_queue`) for propagating exception details from child to parent. The child process runs the **Managed Execution Wrapper**, which:
           1. Calls `engine.dispose(close=False)` to mark pooled connections as stale.
           2. Creates a new database session via `DatabaseManager.get_session()` (scoped session backed by `DATABASE_URL`) for heartbeats, progress updates, and Event Log entries within the child process.
           3. Sets `_subscription_id`, `_subscription_name`, and `_heartbeat_event` on the plugin instance.
           4. Defines a `progress_callback` that checks the current subscription status. If status is `DISABLED` or `DELETED`, it raises `SubscriptionCancelledError` to halt execution gracefully. Otherwise, it calls `db.update_heartbeat_and_progress()` and sets the `heartbeat_event`.
           5. Calls `progress_callback(0)` to initialize the heartbeat.
           6. Invokes `plugin.getData(config, progress_callback)`.
           7. Upon successful return, calls `progress_callback(100)` for completion signaling.
           8. On `SubscriptionCancelledError`: calls `sys.exit(0)` silently — no Event Log entry, no SMTP notification, no status change, no exception details placed on the `exception_queue`. The parent handles the exit gracefully.
           9. On non-cancellation exception: serializes the exception into a dict (`{"exception_type": "<type name>", "exception_message": "<str(e)>", "traceback": "<traceback.format_exc()>"}`) and places it on the `exception_queue`. Then calls `sys.exit(1)` — **does NOT record Event Log, send SMTP, or update status** (the parent handles all three using the queued exception details).
           10. **Connection cleanup (finally block):** Before the child process exits on any path (success, cancellation, or error), it calls `engine.dispose(close=True)` in a `finally` block to close all pooled connections and prevent Postgres connection leaks. If `engine.dispose()` itself raises, the exception is caught and logged locally to avoid masking the original exception or corrupting the `exception_queue`.
      - **Heartbeat Timeout Monitoring:** The Level-1 process starts a **daemon watcher thread** that runs a continuous loop, sleeping for `tick_s = max(HEARTBEAT_TIMEOUT / 10.0, 10)` seconds per iteration (~30s at the default 300s timeout). On each tick the watcher performs two independent checks:
         1. **Heartbeat staleness:** If `heartbeat_event.wait(timeout=tick_s)` returns False (timeout, no heartbeat), the watcher measures the age of the subscription's `last_heartbeat` in the database. If the age exceeds `HEARTBEAT_TIMEOUT`, the watcher sets a shared `multiprocessing.Value('b')` flag, calls `proc.terminate()` (and `proc.kill()` as a hard fallback), marks the subscription **`ERROR`** — but **only if the status is not `DISABLED` nor `DELETED`** (uses `WHERE status NOT IN ('DELETED', 'DISABLED')`) — and the Worker's Level-1 parent sends an SMTP notification. If the heartbeat event is set before the tick expires (normal case), the watcher **refreshes the Redis lock TTL to the full `LOCK_TTL` value** and continues the loop.
         2. **DB status check:** On every tick (regardless of heartbeat state), the watcher queries the subscription's current status from PostgreSQL. If the status is `DISABLED` or `DELETED`, the watcher calls `_kill_child()` immediately — this provides fast cancellation even when the child process is stuck in a blocking network call and not calling `progress_callback`. The child is terminated (SIGTERM, then SIGKILL), and the subscription is not marked ERROR (the status change was user-initiated).
         The watcher thread is given a `threading.Event` stop signal that the main thread sets after `proc.join()` completes (wrapped in try/except for timeout race safety), and also sets the `heartbeat_event` to immediately wake the watcher thread — ensuring prompt cleanup in all exit scenarios with no zombie accumulation.
      - **Completion State — Success:** Upon successful execution (`exitcode == 0`), the worker checks the current subscription status. If the status is `DISABLED` or `DELETED` (indicating the subscription was cancelled mid-execution), the worker skips Event Log entry and status update — the subscription remains unchanged. Otherwise, it records an Event Log entry (exit_code=0, exit_string=""), signals the watcher stop event and sets heartbeat_event for immediate wake.
      - **Completion State — Non-Timeout Error:** If `getData()` exits with a non-zero exit code that was NOT caused by the watcher (i.e., the shared watcher-killed flag is False), the worker reads from the `exception_queue` (non-blocking). If the queue contains an exception detail dict, it uses those values for: `exit_string` formatted as `"<exception_type>: <exception_message>"`, the SMTP notification body (including type, message, and traceback), and the full stack trace logged to `/logs/worker.log`. If the queue is empty (bare `sys.exit(1)` with no Python exception — e.g., `nonZeroExitPlugin`), the worker uses a generic fallback `"Subscription failed with exit code 1"` for `exit_string` and omits the stack trace. In either case, the worker records the Event Log entry (exit_code=1), the Worker's Level-1 parent sends an SMTP notification, logs the error, updates status to `ERROR` (using `WHERE status NOT IN ('DELETED', 'DISABLED' )`), signals the watcher stop event. The subscription is paused until the user manually re-enables it.
      - **Completion State — Schema Validation Error:** If config validation against the augmented schema fails before execution begins, the worker records an Event Log entry (exit_code=3, exit_string=validation error message), sets status to `ERROR` (using `WHERE status NOT IN ('DELETED', 'DISABLED' )`), logs the error, the Worker's Level-1 parent sends an SMTP notification. The subscription must be manually re-enabled by the user after fixing the configuration.
      - **Completion State — Timeout:** The watcher already set `ERROR`, recorded an Event Log entry (exit_code=2, exit_string="Heartbeat timeout — process terminated after 300s"), and sent SMTP.
      - **Outer exception handler:** If an unexpected exception occurs in the outer worker loop, the worker records an Event Log entry (exit_code=1) before updating status to `ERROR` (using `WHERE status NOT IN ('DELETED', 'DISABLED' )`).
      - **Debounce Phase:** Sleep for **5 seconds**.
       - **Re-eval Phase:** Check both the P-Queue and S-Queue for new instances of this `subscription_id`. If any are found, continue the inner loop from the Drain Phase. If both queues are empty: update the subscription status to `ENABLED` (using `WHERE status IN ('ENQUEUED', 'IN_PROGRESS')` to respect the DISABLED/ERROR invariant while also handling re-enqueue during execution), release the lock, and exit the inner loop.

3. **Process & Recovery Management:**
   - **Worker-Side:** The watcher thread manages local heartbeat timeouts using a shared `multiprocessing.Value('b')` flag. On each tick (~30s), the watcher checks both heartbeat staleness and the subscription's DB status. If the heartbeat is stale (age exceeds `HEARTBEAT_TIMEOUT`), the subscription is marked **`ERROR`** (only if status is not `DISABLED` nor `DELETED`), the Level-2 process is terminated, the lock is released, and the Worker's Level-1 parent sends an **SMTP Notification** containing full subscription metadata (ID, Name, Description). If the DB status is `DISABLED` or `DELETED`, the child is terminated immediately without marking ERROR — the watcher does not wait for heartbeat timeout in this case. The watcher thread accepts a `threading.Event` stop signal — the main thread sets it after `proc.join()` returns (wrapped in try/except for timeout race safety) and also sets the `heartbeat_event` to immediately wake the watcher, ensuring the watcher exits promptly in all exit scenarios.
   - **Manager-Side (Watchdog):** The Subscription Manager monitors `last_heartbeat` timestamps. The watchdog timeout is computed as `WORKER_HEARTBEAT_TIMEOUT * 3` at startup (worker heartbeat default is 300s, so default watchdog timeout is 900s / 15 minutes). If a subscription is `IN_PROGRESS` or `ENQUEUED` but `last_heartbeat` is more than the computed timeout old, the Manager:
      1. Force-releases the Redis lock via `QueueManager.release_lock(sub_id)`.
      2. Marks the subscription as **`ERROR`** (using `WHERE status NOT IN ('DELETED', 'DISABLED' )` to respect concurrent user actions).
      3. Sends a detailed **SMTP Notification** (ID, Name, Description) regarding the detected crash/hang.
   - **Startup Recovery:** Upon container start, the Manager scans for any subscriptions stuck in **`IN_PROGRESS`, `ENQUEUED`, or `DELETED`** (due to a prior crash/shutdown). It re-enqueues them to the P-Queue via `try_enqueue`. For `DELETED` subscriptions, the enqueue pushes to Redis without modifying the DB status. The Manager also starts `monitor()` loops for all **EVENT_BASED** subscriptions whose status is `ENABLED`, `IN_PROGRESS`, or `ENQUEUED`. The recovery scan's `try_enqueue()` naturally respects the DISABLED and ERROR invariants via `WHERE status IN ('ENABLED', 'ENQUEUED', 'IN_PROGRESS')` — it never matches `DISABLED` or `ERROR`.

#### 5.3 Error Handling & Notifications

- **Heartbeat Timeout:** Status set to **`ERROR`** (only if not `DELETED` nor `DISABLED`). Worker Level-1 parent terminates child process, sends SMTP notification. (Worker)
- **Schema Validation Error (pre-execution):** Status set to **`ERROR`** (only if not `DELETED` nor `DISABLED`), Event Log entry with exit_code=3, SMTP notification sent. Subscription requires manual re-enable. (Worker)
- **Non-Timeout Exception:** Status set to **`ERROR`** (only if not `DELETED` nor `DISABLED`), Event Log entry recorded (exit_code=1), SMTP notification sent, error logged. Subscription requires manual re-enable. (Worker)
- **System/Worker Crash:** Docker restart policy. SMTP notification sent on detection. (Manager)
- **Schema Breaking Change:** All affected subscriptions set to **`DISABLED`** (only if not `DELETED` nor already `DISABLED`), SMTP notification sent. (Manager)
- **Outer Exception Handler:** Status set to **`ERROR`** (only if not `DELETED` nor `DISABLED`), Event Log entry recorded.

#### 5.4 Exit Codes

| Code | Meaning |
|---|---|
| 0 | Success |
| 1 | Runtime error (non-timeout exception in getData) |
| 2 | Heartbeat timeout (process terminated) |
| 3 | Schema validation failure (config rejected before execution) |

### 6. Authentication & Security

#### 6.1 Architecture Overview

The Web UI and MCP Server are the two externally accessible containers.

```
Browser ──[AUTOKB_ADMIN_USER/PASS]──→ Web UI (port 80, external)
                                           │
MCP Server ──[Bearer AUTOKB_API_KEY]──→ Web UI (port 80, external)
                                           │
MCP Client (AI Assistant) ──[Bearer AUTOKB_API_KEY]──→ MCP Server (port 80, external)
                                           │
                              [URL: http://autokb-web:80]
                                           │
                              ──[AUTOKB_BACKEND_API_KEY]──→ Manager (internal only)
                              [URL: http://autokb-manager:80]
```

All external access is gated by authentication: browser via session login, MCP via Bearer token validation. No unauthenticated action is possible on any endpoint.

#### 6.2 Credential Definitions

| Credential | Purpose | Used By |
|---|---|---|
| `AUTOKB_ADMIN_USERNAME` | Web UI login (session-based) | Browser → Web UI |
| `AUTOKB_ADMIN_PASSWORD` | Web UI login (session-based) | Browser → Web UI |
| `AUTOKB_API_KEY` | MCP → Web UI authentication | MCP Server → Web UI (Bearer header) |
| `AUTOKB_BACKEND_API_KEY` | Web UI → Manager internal auth | Web UI → Manager (header) |

#### 6.3 Flow Details

- **MCP Server:** Sends `Authorization: Bearer <AUTOKB_API_KEY>` to the Web UI. The Web UI validates this against its configured `AUTOKB_API_KEY`.
- **Web UI → Manager:** All proxied requests include `AUTOKB_BACKEND_API_KEY` as an internal header. The Manager validates this on every request. The Web UI connects to the Manager using `AUTOKB_MANAGER_URL` (default `http://autokb-manager:80`), separate from `AUTOKB_BASE_URL`.
- **MCP's `AUTOKB_BASE_URL`:** Set to `http://autokb-web:80` (not `autokb-manager`).
- **Browser flow:** See §7.4 for login, session, and proxied-request details.

### 7. The Web UI (User Experience)

#### 7.1 Architecture

- The Web UI container (`autokb-web`) is the **primary front-facing access point** for browser traffic. The MCP Server (`autokb-mcp`) is a secondary external access point for AI Assistant traffic.
- Framework: **aiohttp** with gunicorn (using `aiohttp.worker.GunicornWebWorker`).
- It serves static files (SPA HTML/JS/CSS) and reverse-proxies `/api/*` requests to the Manager API at `AUTOKB_MANAGER_URL` (default `http://autokb-manager:80`).
- The proxy injects the `AUTOKB_BACKEND_API_KEY` header on every outgoing request to the Manager. The Manager validates this key on every request.
- **Auth route exclusion:** Requests to `/auth/*` are handled locally by the Web UI (login, logout, session management) and are **not** proxied to the Manager. All other `/api/*` paths are proxied.
- No CORS configuration is needed — all API calls flow through the Web UI's own origin via the reverse proxy.
- **Catch-all routing:** Any request path that does not match `/api/*`, a static file, or an asset path serves `index.html` with a 200 status. This enables SPA client-side routing for deep links and direct navigation.

#### 7.2 Visual Design Language

**Theme:** Full dark mode. The application background is `#0A0A0A` throughout. Cards, modals, and panels use a slightly lighter surface (`#1A1A1A`) with subtle border separation (`#2A2A2A`). Text is white (`#FFFFFF`) for primary content and light gray (`#9E9E9E`) for secondary labels and metadata.

**Buttons:**

| Intent | Background | Text | Usage |
|---|---|---|---|
| Primary / Standard | `#3D5AFE` | `#FFFFFF` | Update Now, Edit, Save, Create, Enable, Test |
| Destructive / Caution | `#FF5252` | `#FFFFFF` | Delete, Clear All History, Disable |
| Secondary / Ghost | Transparent, `#3D5AFE` border | `#3D5AFE` | Cancel, Dismiss |

All buttons use consistent `8px` border-radius and `12px` horizontal / `8px` vertical padding. Buttons are `display: inline-flex` with center-aligned content. Destructive actions are always preceded by a confirmation modal.

**Icons:** Each plugin icon is served from `/assets/{plugin_id}.png` (from the docker-managed named volume). If the file does not exist, `default_icon.png` is rendered as a fallback. Icons must be `.png` files with a maximum resolution of 512×512 pixels. Icons render at `48x48` in the plugin grid and `32x32` next to subscription rows.

**Brand assets:** The `assets/` directory also ships two static brand files baked into the image via `COPY assets/ /assets/` in the Dockerfile:
- `autokb.png` — 512×512 logo. Served at `/assets/autokb.png` and rendered in the top-left of the app header at 32×32 px (height-matched to the "AutoKB" wordmark).
- `favicon.ico` — browser tab icon. Served at `/assets/favicon.ico` and referenced from the HTML `<head>` via `<link rel="icon" type="image/x-icon" href="/assets/favicon.ico">`.

**Typography:** System font stack (`-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif`). Mono font (`SFMono-Regular, Consolas, "Liberation Mono", monospace`) for code blocks and configuration JSON previews.

#### 7.3 Layout & Navigation

- **Structure:** Single Page Application (SPA) composed of three permanent regions:
  - **Header** (full-width top bar): Application logo (`/assets/autokb.png`, rendered at 48×48 px to the left of the wordmark) and the wordmark "AutoKB" on the left (32px, `font-weight: 600`, white). The logo image is 1.5× the wordmark text height. The browser tab icon is `favicon.ico` served from `/assets/favicon.ico`. Authenticated username and a Logout button sit on the right.
  - **Left Sidebar** (fixed-width vertical nav): Contains navigation links — **Dashboard**, **Data Sources**, **Subscriptions**, **Recent Activity**, and **Developer Lab**. The currently active page is highlighted.
  - **Main Area** (fluid, fills remaining space): Dynamically swapped content based on the active navigation selection.
- **Default view:** "Dashboard" (`#/dashboard`) — the entry point after login. See §7.3.1 for the full layout (readme in the main area, summary statistics panel docked on the right).
- **Page transitions:** Navigation is client-side only (no full-page reloads). State is managed entirely in the browser; the Manager API is the single source of truth for data.

#### 7.3.1 Dashboard

The Dashboard is the post-login landing page (`#/dashboard`). Its purpose is to orient the operator: explain what the system is and how to use it, while showing the live state of subscriptions, plugins, errors, and infrastructure at a glance — without forcing a navigation to a different page.

**Layout:**

The `#view-dashboard` section is a two-column flex container filling the main area (no inner scroll on desktop; the body scrolls if content overflows the viewport):

- **Readme column (main, left):** `flex: 1; min-width: 0; max-width: 880px`. The primary content. Holds a static explanatory document in three sections (see "Readme content" below). When the viewport is narrow (below ~960px), this column shrinks and the right column stacks below it.
- **Stats column (right sidebar, docked):** `flex: 0 0 320px`. Docked to the right edge of the main area. Holds a single dark card (matching the rest of the app — `#1A1A1A` background, `#2A2A2A` border, `12px` radius) with a vertical stack of statistic rows. On narrow viewports it collapses to a single column below the readme.

The page heading "Dashboard" is rendered above the two-column layout, in the same uppercase-styled `<h2>` as the rest of the app.

**Readme content:**

Static HTML rendered into the readme column at page load (no per-fetch re-render). Three sections separated by `<h3>` headers in white, with body text in `#FFFFFF` and supporting labels (`#9E9E9E`):

1. **What is AutoKB?** — A short elevator pitch covering: it is a distributed, event-driven ETL orchestration engine; it connects to diverse data sources via a pluggable plugin architecture; each plugin pulls data, transforms it, and writes chunks to a per-subscription output directory; downstream Knowledge Base importers consume those files. Notes that plugins are plain Python files dropped into a directory — no recompile, no restart — and that the same engine can drive cron-based and event-based (IDLE/webhook/long-poll) sources.

2. **How to use an existing plugin** — A numbered three-step flow that mirrors the actual UI clicks:
   1. Open **Data Sources** in the sidebar. Each tile is a registered plugin.
   2. Click a plugin tile to open its subscription list.
   3. Click **+ Create New Subscription**, fill in the dynamic form (driven by the plugin's `get_schema()`), and save.
   
   Followed by two short notes: (a) live status is streamed to the UI via SSE — no manual refresh required; (b) the same flow is used to edit or delete a subscription after creation.

3. **How to create your own plugin** — A four-step flow that walks the user through the Developer Lab:
   1. Open **Developer Lab** in the sidebar.
   2. Paste the plugin source into the code editor, set a name (camelCase, ≤32 chars), and optionally attach a `.png` icon (max 512×512).
   3. Click **Test** to validate syntax, inheritance from `BaseSubscription`, and presence of the required `getData()` and `get_schema()` methods. Errors are surfaced inline.
   4. Click **Save** to deploy. The file is atomically written to `/src/plugins/{name}.py` and hot-swapped into the running registry within ~2 seconds (the file-watcher's debounce). Existing subscriptions are unaffected; the new plugin appears in **Data Sources** immediately.
   
   Followed by a one-line pointer to §3 ("The Plugin Architecture") for the full `BaseSubscription` contract.

**Stats panel — content and data sources:**

The stats card is rendered on entry to `#/dashboard` and refreshed reactively on SSE `subscription_update` and `subscription_deleted` events. Seven rows, each with an uppercase label (`#9E9E9E`, 11px, letter-spacing 0.5px — same micro-typography as the table headers) and a value in `#FFFFFF`, 14px. Rows that are zero / empty render their value as a muted em-dash ("—") rather than hiding the row, so the panel's height is stable across deployments with no activity.

| Row | Label | Value | Data source |
|---|---|---|---|
| 1 | **Total Subscriptions** | Count of non-`DELETED` subscriptions. | `GET /api/subscriptions` (length of returned array, excluding any `DELETED` row). |
| 2 | **Total Plugins** | Count of loaded plugins. | `GET /api/plugins` (length of returned array). |
| 3 | **By Status** | A horizontal mini-bar showing the count of subscriptions in each of `ENABLED`, `ENQUEUED` / `IN_PROGRESS` (combined, blue/amber), `ERROR` (red), and `DISABLED` (gray). Each segment's width is proportional to the count of subscriptions in that status, and a numeric count is overlaid on the segment when it has room. A tooltip on each segment shows the status name and count. The bar's total width represents the total subscription count. | Same as row 1. |
| 4 | **By Type** | `N scheduled · M event-based` (e.g. `12 scheduled · 3 event-based`). | Same as row 1, grouped by `sub_type`. |
| 5 | **Last Error** | Relative time of the most recent execution that ended with `exit_code != 0`, or the most recent subscription transition into the `ERROR` state, whichever is more recent. Format: "12m ago". If no error has ever been recorded, the value is "never". Clicking the row navigates to `#/activity` filtered to error rows (the filter UI is a future enhancement; the row is currently informational with a hover state signalling it is clickable). | `GET /api/logging?limit=1` (front-end slices for `exit_code != 0` from a small in-memory cache populated from `GET /api/subscriptions`). |
| 6 | **24h Activity** | Total executions in the last 24 hours, summed across all subscriptions. | Sum of the values returned by `GET /api/subscriptions/activity?hours=24`. |
| 7 | **Health** | Three small status pills, one each for `Database`, `Redis`, and `Plugin Registry`, in that order. Each pill is a 6px-diameter colored dot followed by the subsystem name. Green (`#00C853`) for healthy, red (`#FF5252`) for unhealthy, gray (`#616161`) for "not yet checked" (only on the very first paint before the health call returns). The Plugin Registry pill follows the `registry_loaded` boolean from `/api/health` — if false, the pill turns red and the label appends " (not loaded)". | `GET /api/health`. Refreshed alongside the other stats on entry, **and once every 30 seconds** while the dashboard is the active view (the only stat with its own timer, since `/api/health` has no SSE channel). |

All stats are derived from the same two endpoints (`/api/subscriptions` and `/api/subscriptions/activity?hours=24`) plus a one-shot `/api/health` (with one minor exception: the "Last Error" row needs a third endpoint, `/api/logging`, to find the most recent non-zero exit). There is no per-row fetch and no polling for any non-health stat — the SSE event stream drives all live updates (see "Reactivity" below).

**Reactivity:**

The stats panel is re-rendered (not re-fetched — the in-memory subscription and activity caches are reused) on every `subscription_update` and `subscription_deleted` SSE event, regardless of which view the user is currently looking at. This means switching from another page back to `#/dashboard` after a flurry of SSE events shows the up-to-date values without a network round-trip. The health pill is the only exception: it has its own `setInterval(loadHealth, 30_000)` that fires only while the dashboard is the visible view; the interval is cleared on `hashchange` to a different view and re-armed on return.

**Data freshness on first paint:** On entry to `#/dashboard`, the four data sources (`/api/plugins`, `/api/subscriptions`, `/api/subscriptions/activity?hours=24`, `/api/health`) are fetched in parallel via `Promise.all`. Each stat row renders as "—" until its source data has arrived, then re-renders with the real value. The readme text is static and renders immediately, so the operator sees the explanatory content first while the counts fill in.

**Empty / error states:**

- **No subscriptions yet** — Total Subscriptions shows `0`; the "By Status" mini-bar is empty (no segments); By Type shows `0 scheduled · 0 event-based`. Readme and stats card are both rendered (readme is the primary content; the empty stats are expected on a fresh install).
- **No plugins loaded** — Total Plugins shows `0`; the Health row's Plugin Registry pill is red with "(not loaded)". This is treated as a configuration error and surfaces prominently.
- **API fetch failure** — A single-line muted error appears at the bottom of the stats card: "Could not load stats. Reload?" with a "Reload" link that re-runs the four parallel fetches. The readme remains visible and unaffected.

**Why a side panel and not a top row of cards?** The readme is a long, line-wrapped document; placing the stats above it would push the first paragraph below the fold on most viewports. Docking the stats on the right keeps the readme at the top of the page (above the fold) and gives the stats a stable column that does not reflow as the readme text grows. The right-column width (320px) is wide enough for a label and a value but narrow enough to leave at least 600px for the readme column on a 1280px viewport.

**Why no polling for activity/health (mostly)?** The SSE event stream already pushes every subscription state change. Re-fetching on each event is wasteful — the in-memory cache used by the cross-plugin Subscriptions page (§7.11) and the per-plugin view (§7.5) is updated by the same handlers, and a lightweight re-render of the stats card reads straight from that cache. The one exception is the health endpoint, which has no event channel; the 30-second timer is a pragmatic compromise (fast enough to catch a flapping Redis connection, slow enough to not contribute meaningful load).

#### 7.4 Authentication & Session Flow

- **Login page (`GET /login`):** The unauthenticated entry point is served at `GET /login`, which returns the SPA shell. The SPA itself renders the centered login form (username + password fields, "Sign In" button in `#3D5AFE`). Submitting the form sends `POST /auth/login` with the credentials. The `/login` route is **not** auth-gated (it is the page that the server redirects to when no session is present).
- **Auth-gated root (`GET /`):** A request to `GET /` is auth-gated. If the caller has no valid session cookie and no valid `Authorization: Bearer <AUTOKB_API_KEY>` header, the server responds with `302 Found` to `/login`. Authenticated requests receive the SPA shell, which then loads the dashboard view.
- **Post-login redirect:** After a successful `POST /auth/login`, the SPA performs a full-page navigation to `GET /`. The server then serves the authenticated app shell (path is now `/` with the dashboard hash route, e.g. `/#/dashboard`).
- **Credentials:** The Web UI validates against `AUTOKB_ADMIN_USERNAME` and `AUTOKB_ADMIN_PASSWORD` environment variables.
- **Session:** On successful authentication, the server establishes a session cookie. All subsequent API calls include this cookie.
- **Logout:** A `POST /auth/logout` route clears the session cookie and redirects to the login page.
- **Proxied authentication:** When the Web UI proxies a request to the Manager, it adds `AUTOKB_BACKEND_API_KEY` as an internal header. The browser never sees this key.
- **MCP authentication:** The Web UI also validates `Authorization: Bearer <AUTOKB_API_KEY>` headers on the proxy path for MCP-originated requests (see §6). This token is validated against the configured `AUTOKB_API_KEY` before the request is forwarded to the Manager.

#### 7.5 Subscription Management View

Navigating to a plugin's subscription list shows all subscriptions for that plugin (excluding `DELETED`).

**Subscription Row Layout:**
Each row displays:
- **Name** (from the subscription's `name` column).
- **Plugin Description** (from `plugin.metadata["description"]` — describes the data source type).
- **Subscription Description** (from the subscription config's `description` field in JSONB — describes this specific instance).
- **Access Level** — rendered as a badge: `PUBLIC` (`#00C853` green tint) or `PRIVATE` (`#616161` gray tint).
- **Live Status badge:**
  - `ENABLED` — green tint (`#00C853`)
  - `ENQUEUED` — blue tint (`#3D5AFE`)
  - `IN_PROGRESS` — amber tint (`#FFD740`)
  - `ERROR` — red tint (`#FF5252`)
  - `DISABLED` — gray tint (`#616161`)
  - `DELETED` — not rendered in list
- **Last Updated** timestamp (relative: "2m ago", "1h ago", etc.).
- **High-Activity Monitor:** Number of executions in the past 24 hours (fetched via `GET /api/subscriptions/{sub_id}/activity`). Refreshed reactively on SSE status change events.
- **Real-time progress bar:** A horizontal bar (`background: #2A2A2A`, fill `#3D5AFE`) with a percentage label, e.g. `[|||||---------] 50%`. Visible only during `IN_PROGRESS` state.

**Row Action Buttons:**

| Button | Color | Behavior |
|---|---|---|
| Update | `#00AA55` | Calls `POST /api/subscriptions/{sub_id}/trigger`. Disabled if status is `ERROR`, `DISABLED`, or `DELETED`. |
| Edit | `#3D5AFE` | Opens a slide-out panel or modal with the dynamic form (§7.6). Disabled if `DELETED`. For `EVENT_BASED` subscriptions, editing restarts the monitor loop with new config. |
| State Toggle | Dynamic | Single button adapting to current status. `ERROR` or `DISABLED` → label "Enable" (`#3D5AFE`), sets `ENABLED`. `ENABLED` → label "Disable" (`#FF5252`), sets `DISABLED`. Hidden for `DELETED`. |
| Delete | `#FF5252` | Shows confirmation modal: "Are you sure you want to delete subscription '{name}'? This will permanently remove all output data." On confirm, calls `DELETE /api/subscriptions/{sub_id}` which sets status to `DELETED` and enqueues cleanup. Hidden for `DELETED`. |

**Global Actions (above the list):**
- **Create New Subscription** button (`#3D5AFE`): Opens the creation form (§7.6).
- **Edit Plugin** button (`#FFC107`, yellow): Loads the plugin's current source code into the Developer Lab in *Edit Plugin* mode (see §7.9). Clicking it navigates to `#/devlab?edit={plugin_id}`, which pre-populates the Plugin Name (read-only), Plugin Code, and (if previously uploaded) Icon fields, and surfaces a yellow "Editing existing plugin" banner at the top of the lab. The yellow color visually distinguishes the action from the blue Create button and signals the stricter semantics that apply (see §7.9 "Edit Plugin mode" for the schema-hash invariant).
- **Delete Plugin** button (`#FF5252`, red): Removes the plugin file from disk and returns to the Data Sources page. Disabled when the plugin has any subscriptions attached (button is greyed out with a tooltip: "Cannot delete a plugin with existing subscriptions. Delete them first."). On click, shows a confirmation modal: "Are you sure you want to delete the plugin '{name}'? The plugin file will be removed from disk. This action cannot be undone." On confirm, calls `DELETE /api/plugins/{plugin_id}` (§12). The button state is reactive — it updates after loading the subscription list and on SSE events (so it becomes enabled once all subscriptions are deleted).

#### 7.6 Dynamic Forms (Create & Edit)

Both the Create and Edit flows use forms dynamically rendered from the plugin's JSON Schema (fetched via `GET /api/plugins/{plugin_id}/schema`).

**Create Form Fields:**
- **Name** (text input) — required. Displayed with a note: "Allowed characters: letters, numbers, and periods. camelCase recommended."
- **Cron Expression** (text input) — optional. Defaults to `0 * * * *` for `SCHEDULED` plugins, `0 0 * * *` for `EVENT_BASED` plugins. The subscription type is inherited from the plugin's metadata. Invalid cron expressions are rejected with an inline error message. Note: these default values are randomized at creation time by the Manager (§4.1) — the stored cron will never be exactly `0 * * * *` or `0 0 * * *`.
- **Access Level** (radio toggle) — optional. Options: `PRIVATE` / `PUBLIC`. If not specified, defaults to the plugin's `DEFAULT_ACCESS_LEVEL` (fetched from `GET /api/plugins/{plugin_id}`). The form pre-selects the plugin's default value to make the behavior explicit.
- **Schema-generated fields** — dynamically rendered HTML inputs (text fields, combo boxes, radio buttons, checkboxes) matching the plugin's `get_schema()`. Schema fields with `"format": "password"` in their JSON Schema definition render as `<input type="password">` and are never returned in API or SSE responses (excluded from all GET payloads).
- **Extra Parameters** — three text inputs (`_extra_param_1`, `_extra_param_2`, `_extra_param_3`) with labels "Extra Param 1 / 2 / 3". These are reserved string fields auto-included in every plugin's schema (see §3.3, "Extra Parameters invariant"). The form does not require the user to provide values; the persistence layer injects any missing key with the empty string at write time. Existing values (assigned by a previous save or by a future plugin feature) are preserved.

**Edit Form:**
- Same schema-generated fields as Create, but pre-populated with the subscription's current `config` values.
- **Password-format fields are excluded from the Edit GET response.** The form renders blank password inputs. On Edit PUT, password fields follow these rules:
   - Field key **absent** from body → keep existing encrypted value unchanged. The `required` keyword is **not enforced** on Edit for password fields (it is enforced on Create).
   - Field key present, value is **non-empty string** → validate against all schema constraints, then encrypt and store the new value.
   - Field key present, value is **empty string or null** → keep existing encrypted value (no clearing mechanism).
- **No Name field** — the subscription name is immutable after creation and not accepted by the Edit endpoint.
- **Cron** (text input) — pre-populated with the current expression, optional to change.
- **Access Level** (radio toggle) — pre-populated with the current subscription's `access_level`. Optional to change.
- Saving calls `PUT /api/subscriptions/{sub_id}`. Config, extra params, and access level are re-validated.

**Form Submission Behavior:**
- Client-side validation for required fields, type correctness, and cron expression validity.
- Server-side validation errors (HTTP 400 with an error body) are displayed inline below the relevant field.
- Success closes the form/panel and the SSE event stream updates the subscription list reactively.

#### 7.7 Live Updates via SSE

The browser connects to `GET /api/events` (proxied to the Manager) using the standard `EventSource` API:

```
new EventSource('/api/events')
```

**Event Pipeline:**
1. Worker updates subscription status or progress in PostgreSQL.
2. `DatabaseManager` sends `SELECT pg_notify('subscription_updated', sub_id)` inside the same database transaction.
3. Manager's dedicated `asyncpg` connection pool receives the `LISTEN` notification.
4. Manager pushes the full updated subscription object to all connected SSE response streams.
5. The browser's `EventSource` handler receives the event and reactively updates the affected subscription row(s).
6. **On new SSE connection:** The Manager immediately sends a full state snapshot containing all current subscriptions (excluding DELETED) as an initial event. This ensures the client is fully synchronized even after a disconnection/reconnection cycle.

**SSE Payload Format — `subscription_update`:**

```json
{"type": "subscription_update", "data": {
  "id": "<uuid>", "plugin_id": "<str>", "name": "<str>",
  "status": "IN_PROGRESS", "progress": 50, "last_updated": "...",
  "last_error": null, "sub_type": "SCHEDULED", "cron": "0 * * * *",
  "access_level": "PRIVATE",
  "config": {...}
}}
```

**SSE Payload Format — `subscription_deleted`:**

```json
{"type": "subscription_deleted", "data": {"id": "<uuid>", "plugin_id": "<str>", "name": "<str>"}}
```

The `subscription_deleted` event is pushed directly by the Manager's DELETE endpoint (bypassing `pg_notify`, since the row will be removed later by the Worker). The UI responds by removing the row from the subscription list.

**Stale connection cleanup:** The Manager's SSE handler sends a keepalive comment (`:keepalive\n\n`) every 30 seconds. If the browser tab is closed, the `StreamResponse.write()` fails with `ConnectionResetError`. The handler catches this exception, removes the response stream from the active connections set, and cleans up the associated asyncio task. No manual timeout tracking is required — the write failure is the disconnection signal.

**Password masking in SSE payloads:** Password-format fields are **excluded** from the `config` field in the `subscription_update` SSE payload. The `config` object will not contain any keys matching password-format fields defined in the plugin schema.

**Reactivity:** On receiving any `subscription_update` event, the UI also re-fetches the 24h activity count for that subscription to keep the High-Activity Monitor column current. No polling is used for any other data; SSE is the sole mechanism for live updates.

#### 7.8 Recent Activity Page

A dedicated page in the sidebar (nav label: "Recent Activity") displays the execution history:

- **Data source:** `GET /api/logging` — returns up to 100,000 `EventLog` entries ordered by `executed_at DESC` (the API hard-caps at 100k entries to prevent unbounded response sizes). Each entry includes `id`, `subscription_id`, `subscription_name`, `plugin_id`, `executed_at`, `exit_code`, and `exit_string`. The `subscription_name` and `plugin_id` are joined from `Subscription` (a `plugin_id` of a subscription IS the plugin name; per §3.1 `plugin_id = sanitize_name(metadata["name"])`).
- **Table layout — column headers (clickable, sortable):** the row of column headers sits above the data rows. Clicking a sortable header toggles ascending/descending sort (or switches the sort key) and the active header is marked with a small arrow (`▲` for ascending, `▼` for descending, in `#3D5AFE`).
  - **View** — not sortable. Contains a "View" button (`#3D5AFE`). On the far left of the row.
  - **Plugin** (`plugin_id`) — sortable, string.
  - **Subscription** (`subscription_name`) — sortable, string.
  - **Timestamp** (`executed_at`) — sortable, datetime. Default sort key. Default direction: `desc` (newest first).
  - **Status** (`exit_code`) — sortable. Sort uses a logical severity order: Success (0) < Config Error (3) < Error (1) < Timeout (2), so grouping by status keeps the table meaningful.
  - **Column widths:** View is fixed 80px, Timestamp is fixed 200px, Status is fixed 140px. The remaining horizontal space is split **equally** between the Plugin and Subscription columns (`flex: 1` each) so they are the same width regardless of content.
- **Row columns (data rows):** rendered in the same order as the headers above.
  - **View** — `View` button (`#3D5AFE`).
  - **Plugin** — plugin name (e.g. `happyPathPlugin`).
  - **Subscription** — subscription name.
  - **Timestamp** — local time string, e.g. `6/6/2026, 3:35:32 AM`.
  - **Status** — colored badge with the textual label only (no numeric exit code in the table):
    - 0 — green (`#00C853`), "Success"
    - 1 — red (`#FF5252`), "Error"
    - 2 — red (`#FF5252`), "Timeout"
    - 3 — amber (`#FFD740`), "Config Error"
- **Event detail view (`#view-event-detail`):** clicking the `View` button in a row replaces the Recent Activity list with a full-screen detail view (the main area is cleared). The detail view shows:
  - **Plugin** — the plugin name.
  - **Subscription** — the subscription name.
  - **Timestamp** — full local time string.
  - **Status** — colored badge with label and the numeric exit code in parentheses, e.g. `Error (exit code 1)`.
  - **Error / Output** — the full `exit_string` rendered in a monospace `<pre>` block with no truncation. Multi-line / long error output is shown in full and scrollable.
  - A `← Back` button (`#3D5AFE` outline / secondary style) at the top returns to the Recent Activity list, preserving the current sort.
- **Footnote:** Entries displayed here are tied to active subscriptions. When a subscription is deleted, its event history is removed. For permanent audit history, retain `/logs/*.log` externally.
- **Clear All History:** A `#FF5252` button at the top of the page. On click, shows confirmation: "This will permanently delete all execution history. This action cannot be undone." Confirming calls `DELETE /api/logging`.

#### 7.9 Plugin Deployment (Developer Lab)

A sidebar-navigable page ("Developer Lab") for pasting and testing plugin code. The lab operates in two modes: **Create New** (default) and **Edit Plugin** (entered by clicking the yellow "Edit Plugin" button on a plugin's subscription list, §7.5). The two modes share the same UI and the same Save flow, but the Edit Plugin mode applies an additional schema-stability check at write time (see "Edit Plugin mode" below).

- **Interface:** A large monospace `<textarea>` for code input, a **Plugin Name** text input (e.g., `myPlugin`, `maxlength="32"` — the HTML input enforces the 32-character cap defined in §3.1, and the server-side validate/save endpoints also reject longer names with HTTP 400), an **Icon** file upload field, and two action buttons. The Plugin Name defines `metadata["name"]`, the filename (`{name}.py`), and the icon filename (`/assets/{sanitize_name(name)}.png`) simultaneously. There is a single input — no separate name and filename fields. In **Edit Plugin** mode, the Plugin Name field is read-only (it must match the existing plugin's name — the file name and the plugin's `metadata["name"]` are immutable when editing).
- **Edit Plugin banner (yellow):** When the lab is in Edit Plugin mode, a yellow banner is rendered at the top of the form: *"Editing existing plugin: {plugin_id} — changes to the config (schema) will be rejected. The plugin's source code, getData() implementation, and metadata can be updated, but the JSON schema returned by get_schema() must remain identical."* The banner's background is `#FFC107` (matching the Edit Plugin button) and its text is dark (`#212121`) for legibility.
- **Test Button** (`#3D5AFE`): Sends the code to `POST /api/dev_lab/validate`. The validation engine runs through all checks (syntax, inheritance, implementation completeness, `DEFAULT_ACCESS_LEVEL` validity, naming sanitization, schema hash comparison). Results are displayed inline: green checkmark on success, red error message with the specific failure reason on failure. No files are written to disk. The Test button works identically in both modes; in Edit Plugin mode, it serves as a dry-run of the schema check that the Save button will perform against the stored hash.
- **Save Button** (`#3D5AFE`): Runs the same validation as Test. If valid, the code is saved using an **atomic write pattern**: the code is first written to a temporary file (`/src/plugins/.{plugin_name}.py.tmp`), validated for syntax and importability via `importlib`, and if validation passes, the temp file is atomically renamed to `/src/plugins/{plugin_name}.py` via `os.rename()`. If validation of the temp file fails, an error is returned to the user and the temp file is deleted — the existing plugin file remains untouched. On success, a notice informs the user that the plugin is now available (background hot-swap: the file watcher detects the file change with a **2-second debounce** and loads the plugin into the running registry). In Edit Plugin mode, an additional schema-hash check is performed between the temp-file import and the atomic rename (see below).
- **Icon upload:** When saving a new plugin, an optional icon file can be uploaded. The icon must be a `.png` file, its filename must match the `plugin_id` (e.g., `biblePlugin.png`), and its resolution must not exceed 512×512 pixels. The icon is saved to `/assets/{plugin_id}.png`. If no icon is uploaded, `default_icon.png` is used as a fallback. The icon is re-uploaded as part of Edit Plugin mode if the user wants to update it; the existing icon is overwritten. **Icon metadata persistence:** The `dev_lab/save` endpoint also updates the `metadata["icon"]` value in the plugin's source code via AST-based text splice, so the file watcher picks up the new icon path on reload. Without this, the reloaded source would still reference the old icon filename and the API would continue returning `default_icon.png`.

**Edit Plugin mode — schema-stability invariant:**

The core invariant of Edit Plugin mode is: *the JSON schema returned by the plugin's `get_schema()` must remain byte-for-byte identical to the previously stored schema.* This protects every existing subscription's persisted `config` from becoming invalid against the new plugin, and it preserves the schema hash stored in `plugin_registry_state` (which the file watcher uses to detect breaking changes, §3.3 point 3).

The mechanism:

1. The user navigates to `#/devlab?edit={plugin_id}`. The Web UI calls `GET /api/dev_lab/load/{plugin_id}` to fetch the plugin's current source code and pre-populates the form.
2. The user edits the code (e.g., rewrites the body of `getData()`, fixes a bug, updates `metadata["description"]`).
3. The user clicks Save. The endpoint runs the same validation as Create New. If the static checks pass, the code is written to a temp file and imported via `importlib` to confirm it is importable and exposes a `BaseSubscription` subclass.
4. The endpoint instantiates the new class, calls `get_schema()`, and computes `schema_hash(new_schema)` — the same `sha256(json.dumps(augment_schema(schema), sort_keys=True))` hash that the registry uses (see `utils/misc_utils.py`).
5. The endpoint looks up the existing plugin record: `existing = registry.get(sanitized_plugin_id)`.
   - **If `existing is None`** (new plugin — Create New mode): the save proceeds and the new plugin is registered as a fresh entry.
   - **If `existing is not None`** and `existing.schema_hash_value == new_hash`: the save proceeds. The atomic rename replaces the file. The file watcher will reload the plugin on the next debounce, the new `schema_hash` will match the stored hash (no breaking change), and every existing subscription's `config` is still valid.
   - **If `existing is not None`** and `existing.schema_hash_value != new_hash`: the save is **rejected** with HTTP 400 and a detailed error message: *"Cannot edit plugin {plugin_id!r}: config (schema) has changed. Existing hash {hh[:12]}, new hash {hh[:12]}. Editing an existing plugin requires the config to remain identical. To change the config, create a new plugin."* The temp file is deleted, the existing plugin file on disk is **not touched**, the in-memory registry is unchanged, and every existing subscription continues to run with the previous plugin code. The user's edit is discarded in full — there is no partial apply.

The check is performed on the augmented schema (i.e. the schema including the 3 reserved `_extra_param_*` properties from §3.3). Property order is irrelevant (`sort_keys=True`), and the 3 reserved extras are always present in both the old and new hashes, so they cancel out of the comparison.

**Why a hash and not a field-by-field diff?** The hash is the same primitive the file watcher already uses to detect breaking changes (see §3.3 point 3 and `src/utils/registry.py`), so a single primitive covers both the dev_lab path and the file-watcher path. If a future change loosens the breaking-change policy (e.g. allowing non-breaking additions), the hash policy can be relaxed in one place.

**Worker behavior:** the Worker loads the plugin fresh on each job execution (`load_plugin_for_execution` re-imports the file via `importlib.util.spec_from_file_location`, see `src/utils/registry.py`). Once the new file has been atomically renamed and the file watcher's debounce has elapsed, the next job the Worker pulls from the queue will pick up the new code automatically. No Worker restart, no broadcast, no in-process state push is needed — the file is the source of truth.

**Why a separate mode rather than always allowing schema changes?** The schema-stability invariant protects every existing subscription. The old `dev_lab/save` endpoint allowed schema-breaking changes and relied on the file watcher to detect them after the fact — at which point affected subscriptions were DISABLED and the operator had to migrate manually. Edit Plugin mode makes the dev_lab itself refuse to author a breaking change in the first place; the file watcher's breaking-change detection remains in place as a safety net for operators who edit the plugin file directly (e.g. via SSH) without going through the dev_lab (this path is exercised by the breaking-change test, see `test_runner.py:418` and §15).

**API summary for Edit Plugin mode:**
- `GET /api/dev_lab/load/{plugin_id}` — returns `{"ok": True, "name": plugin_id, "code": "<file contents>"}` for the named plugin, or HTTP 404 if the plugin is not loaded.
- `POST /api/dev_lab/save` — accepts `{name, code, icon_base64?}`; in Create New mode creates a new plugin, in Edit Plugin mode (i.e. when the resulting `plugin_id` already exists in the registry) requires `schema_hash(new_schema) == existing.schema_hash_value`, otherwise returns HTTP 400 with a hash-mismatch error and leaves the existing plugin untouched. The response includes `{"ok": True, "path": ..., "mode": "create" | "edit"}` so the UI can show a context-appropriate success message.

#### 7.10 Data Sources Page

A sidebar-navigable page ("Data Sources", `#.data-sources`) that lists every available Data Source Implementation (i.e., every registered plugin). This used to be the "Dashboard" page; the layout described in §7.3's prior version now lives here.

- **Heading:** `Data Sources` (uppercase-styled `<h2>` matching the rest of the app).
- **Layout:** Responsive grid of plugin cards. Each card contains the plugin's icon (from `/assets/{plugin_id}.png`, falling back to `default_icon.png`), the plugin's display name, and a colored **`sub_type` badge** (`SCHEDULED`: blue — `rgba(61, 90, 254, 0.2)` background, `#3D5AFE` text; `EVENT_BASED`: yellow — `rgba(255, 215, 64, 0.2)` background, `#FFD740` text, displayed as "EVENT-BASED" with hyphen). The grid uses `grid-template-columns: repeat(auto-fill, minmax(330px, 1fr))` (cards are at least 330px wide so a 32-character plugin name fits on a single line). The card uses standard 16px padding. The plugin name is rendered with `text-overflow: ellipsis` and `white-space: nowrap` so any name beyond the available width is truncated cleanly with an ellipsis rather than wrapping. Clicking a card navigates to `#/subscriptions/{plugin_id}` (the per-plugin subscription list described in §7.5).
- **Data source:** `GET /api/plugins` — returns one entry per loaded plugin. The page is reloaded on each visit to `#.data-sources`; live updates of the plugin list (developer lab hot-swap) are not currently pushed to the Data Sources page (the user re-enters the tab to see new plugins).

#### 7.11 All Subscriptions Page

A sidebar-navigable page ("Subscriptions", `#.all-subscriptions`) that lists **every** subscription across **every** plugin in a single sortable table. This is a flat, cross-plugin view — different from the per-plugin list described in §7.5.

**Use case:** Operators with many subscriptions across many plugins want a single place to see everything at once and perform bulk-like triage (sort by last-updated to find stale, sort by 24h activity to find hot ones, sort by status to find broken ones, etc.).

**Data source:**
- `GET /api/subscriptions` (no `plugin_id` filter) — returns all non-`DELETED` subscriptions.
- `GET /api/subscriptions/activity?hours=24` — returns a `{subscription_id: count}` map of executions in the last 24h (one batched SQL `GROUP BY` query, avoiding N+1). Both calls fire in parallel on page entry.

**Table layout — column headers (clickable, sortable):** the row of column headers sits above the data rows. Clicking a sortable header toggles ascending/descending sort (or switches the sort key) and the active header is marked with a small arrow (`▲` for ascending, `▼` for descending, in `#3D5AFE`). Headers are uppercase, 11px, `#9E9E9E`, with 0.5px letter-spacing (matching the Recent Activity page style).
  - **Subscription** (`name`) — sortable, string. The subscription's display name. Column flex 1.
  - **Plugin** (`plugin_id`) — sortable, string. The owning plugin's name (a subscription's `plugin_id` IS the plugin's name per §3.1). Column flex 1. Both Subscription and Plugin are `flex: 1` so they split the remaining horizontal space **evenly** between them regardless of content.
  - **Status** (`status`) — sortable, string. Sort is alphabetical by the status string (`DISABLED` < `ENABLED` < `ENQUEUED` < `ERROR` < `IN_PROGRESS`). Column width 146px (wide enough to hold `IN_PROGRESS (100%)` without the row expanding). When status is `IN_PROGRESS` and `progress` (0–100) is set, the status badge displays the percentage inline, e.g. `IN_PROGRESS (45%)`, updated live via the SSE `subscription_update` stream (§7.7).
  - **Access Level** (`access_level`) — sortable, string. Column width 110px.
  - **Activity** (`activity_24h`) — sortable, integer. Count of executions in the last 24 hours. Column width 84px, **center-aligned**, `#9E9E9E`. (84px comes from giving 16px to the Status column above, so the total table width is preserved.)
  - **Updated** (`last_updated`) — sortable, datetime. Default sort key. Default direction: `desc` (most recently changed first). Column width 130px, formatted as relative time ("2m ago", "1h ago", "3d ago" — using the same `relativeTime()` helper as §7.5).
  - **Edit** — not sortable. Contains an "Edit" button (`#3D5AFE`).
  - **Enable / Disable** — not sortable. Single button adapting to current status (same logic as §7.5). Hidden / disabled for `DELETED`.
  - **Update** — not sortable. Contains an "Update" button (`#00AA55`, green). Calls `POST /api/subscriptions/{sub_id}/trigger`. Disabled if status is `ERROR`, `DISABLED`, or `DELETED` (i.e. when the subscription is not in a triggerable state per the §6 enqueue rules).

**Row action buttons:** identical look, feel, and behavior to the per-plugin subscription list (§7.5):
  - **Edit** — opens the same edit modal used elsewhere (re-uses `openEditForm(subId, pluginId)`). Disabled if status is `DELETED`.
  - **Enable / Disable** — calls `PUT /api/subscriptions/{sub_id}/status` with `{"status": "ENABLED"}` or `{"status": "DISABLED"}`. Label and color adapt to current status (same rules as §7.5: blue for Enable, red for Disable). Disabled if status is `DELETED`.
  - **Update** — calls `POST /api/subscriptions/{sub_id}/trigger` to manually enqueue the subscription for an immediate run. Green button (`#00AA55`). Disabled if status is `ERROR`, `DISABLED`, or `DELETED`. (The Delete action is intentionally omitted from this cross-plugin triage view — operators should navigate to the per-plugin list (§7.5) to delete a subscription.)

**Height and density:** the table uses a more compact row height than the per-plugin list (36px `min-height`, 6px vertical padding) so 20+ rows fit on a single screen without scrolling. The list scrolls inside the main area; there is no pagination — all matching rows are rendered. If the list grows large enough that scrolling becomes a problem, add client-side virtualization in a follow-up.

**Empty state:** when no subscriptions exist, the page shows a single line: "No subscriptions yet. Create one from a Data Source." with 16px padding.

**Live updates:** the all-subs table is kept in sync with the SSE `subscription_update` and `subscription_deleted` events (§7.7). When a subscription's status, last_updated, or other visible field changes anywhere in the system, the matching row in this table reflects the change immediately. The 24h activity count is also updated live: each time a `subscription_update` event signals a transition from a running state (`ENQUEUED`, `IN_PROGRESS`) to a terminal state (`ENABLED`, `ERROR`, `DISABLED`), the row's activity counter is incremented by 1 in place. A full re-fetch of the activity counts is still performed on page entry (or when the table is re-sorted) so the counter self-corrects over time.

**Why two list views?** §7.5 (per-plugin) is the place to *manage* a plugin's subscriptions: create new ones, edit config in context, see the plugin's description. §7.11 (all-subs) is the place to *triage* across the whole system: spot failures, find hot subscriptions, audit last-touched time. They are complementary.
- **Hot-Swap integration:** After saving, the user does not need to restart the stack. The plugin becomes available for creating subscriptions within seconds (see §7.5). If a schema-breaking change is detected for a modified plugin, the UI will show the subscription status updates via SSE as they are disabled.

### 8. The MCP Management Layer (AI Interface)

The MCP Server (`autokb-mcp`) acts as a secure, multi-tenant proxy between an AI Assistant (via the Model Context Protocol) and the AutoKB backend API. It exposes AutoKB's management capabilities as semantic MCP tools.

**Key design goals:**
- **Identity Passthrough:** The server extracts the end-user's authentication token from incoming HTTP requests and forwards it to the AutoKB API, ensuring all actions are subject to the user's permissions.
- **Granular Permissions:** Prevents the MCP server from acting as a "super-user" — every operation is scoped to the authenticated caller.
- **Token Optimization (TOON):** Bulk responses (lists) are compressed using the TOON protocol to reduce token consumption and cost.
- **Semantic Safety:** All destructive tool parameters include explicit warnings in their Pydantic `Field(description="...")` so the AI understands the consequences before calling.

#### 8.1 Container & Image Architecture

The MCP Server uses a **separate Docker image** from the unified Manager/Worker/Web UI base image. It has its own dependency set declared in `pyproject.toml` and is built from its own Dockerfile.

| Attribute | Value |
|---|---|
| Container name | `autokb-mcp` |
| Image | Separate (not the unified base image) |
| Startup command | `python -m main` |
| Internal port | `80` (configurable via `MCP_PORT`) |
| Python path | `PYTHONPATH=/mcp/src` |
| Base URL | `AUTOKB_BASE_URL` (default `http://autokb-web:80`) — routes through Web UI, not directly to Manager |
| Auth | Bearer token in `Authorization` header, passed through to Web UI for validation |

**Directory structure (within the separate image):**

```
/
├── Dockerfile        # MCP-specific image
├── pyproject.toml    # MCP-specific dependencies
├── logs/             # MCP log directory
└── src/
    ├── main.py       # FastMCP server lifecycle, ASGI middleware, identity extraction (contextvars), tool definitions
    └── client.py     # Authenticated API client, domain-driven API methods (NO TOON imports)
```

**Volume mounts:** The MCP container mounts `/logs` for its own log file (`/logs/mcp.log`) but does **not** mount `/src/plugins`, `/assets`, or `/output`.

#### 8.2 Transport & Identity Layer (`mcp/src/main.py`)

**Framework:** FastMCP with **STREAMABLE-HTTP ONLY** transport. The server runs as an ASGI application on `uvicorn` with the MCP endpoint mounted at `/mcp`.

**Authentication Middleware:** An ASGI middleware intercepts every incoming HTTP request:

1. **Clear context** — The `contextvars` token is unconditionally reset to `None` at the start of every request, regardless of whether an `Authorization` header is present. This prevents token leakage between concurrent requests in the asynchronous event loop.
2. **Extract or reject** — If the request includes an `Authorization: Bearer <token>` header, the token is stored in a module-level `ContextVar`. If no Bearer header is present, the middleware immediately returns HTTP 401 with body `{"error": "Missing authentication token"}` — no tool dispatch or forwarding occurs.
3. **Token retrieval** — The `get_user_token()` helper function retrieves the token from the `ContextVar`. Missing tokens are rejected at the middleware level, so a non-None token is guaranteed when tools execute.

```python
_current_user_token: ContextVar[Optional[str]] = ContextVar("current_user_token", default=None)
```

**Tool Definition Pattern (every tool follows this contract):**
1. **Schema Definition:** Pydantic models for all tool parameters. Every field includes `Field(description="...")` to provide rich semantic context to the LLM.
2. **Identity Integration:** Every tool retrieves the current user's token from the `contextvars` (via `get_user_token()`) and passes it into the client method.
3. **Delegation:** Tools contain no business logic — they parse parameters and delegate to the Client layer. The tool layer is a thin passthrough.

#### 8.3 Authenticated Client Layer (`mcp/src/client.py`)

**Pattern:** A dedicated asynchronous API client class (`AutoKBClient`) with the following design principles:

- **Configuration:** Base URL from `AUTOKB_BASE_URL` environment variable (default `http://autokb-web:80`).
- **Statelessness:** The client does not rely on a single static API key. Every method accepts an optional `api_key` parameter for per-request authentication.
- **Passthrough:** A private `_get_headers()` method injects the caller's `api_key` into the `Authorization` header of every outgoing request. If no `api_key` is provided, the header is omitted entirely — the MCP server passes through whatever Bearer token it received from the AI Assistant.
- **Transport:** `httpx.AsyncClient` for all asynchronous HTTP requests.
- **Error handling:** Every response calls `raise_for_status()` so non-200 status codes are intercepted and raised, allowing the tool layer to provide semantic error feedback to the LLM.
- **Domain-driven organization:** Methods are organized into logical modules (Subscription Management, Plugin Management, System Health) with exact one-to-one mapping to AutoKB API endpoints.

**TOON Protocol Integration:**

| Rule | Detail |
|---|---|
| Import location | `json_to_toon` is imported **only in `main.py`**, never in `client.py`. The client returns raw JSON. |
| Application scope | Apply TOON **only** for large/bulk datasets: `list_subscriptions`, `list_plugins`. |
| Exclusion scope | Do **NOT** apply TOON to single-record responses (`get_subscription_status`, `get_plugin_details`, `get_plugin_schema`, `get_system_health`). |
| Constraint | Do NOT apply TOON if the GET result is intended as input for a subsequent command (e.g., fetching an ID to use in a DELETE call). TOON is for terminal consumption/reading only. |

**Strict Schema Fidelity (CRITICAL):** When implementing domain-driven methods in `client.py`, the parameter names and JSON body keys **must match the AutoKB Manager API's schema exactly**:

- **No aliasing:** Do not rename fields to be more "Pythonic" if they differ from the AutoKB API.
- **Exact correspondence:** If the Manager API expects `plugin_id`, the client sends `plugin_id`, not `pluginId` or `plugin_uuid`.

#### 8.4 Required Tool Definitions

**I. Subscription Management**

| Tool Name | Description | Parameters | TOON |
|---|---|---|---|
| `create_subscription` | Create a new subscription for a specific plugin. WARNING: This will start a new background process. | `plugin_id: str`, `name: str`, `config: dict`, `cron: Optional[str]`, `access_level: Optional[Literal["PRIVATE", "PUBLIC"]]` | No |
| `edit_subscription` | Update configuration for an existing subscription. Note: name cannot be changed. | `sub_id: str`, `config: dict`, `cron: Optional[str]`, `access_level: Optional[Literal["PRIVATE", "PUBLIC"]]` | No |
| `delete_subscription` | Remove a subscription. WARNING: This is a destructive action that cannot be undone. | `sub_id: str` | No |
| `trigger_manual_update` | Manually trigger a single execution of a subscription. | `sub_id: str` | No |
| `set_subscription_status` | Set subscription status to ENABLED or DISABLED. WARNING: Disabling will prevent execution. | `sub_id: str`, `status: Literal["ENABLED", "DISABLED"]` | No |
| `list_subscriptions` | List all subscriptions (optionally filtered by plugin). | `plugin_id: Optional[str]` | **Yes** |
| `get_subscription_status` | Returns current status, last error, and progress. | `sub_id: str` | No |

**II. Plugin Discovery**

| Tool Name | Description | Parameters | TOON |
|---|---|---|---|
| `list_plugins` | List all available data source plugins and their metadata. | None | **Yes** |
| `get_plugin_details` | Get full metadata (description, icon) for a specific plugin. | `plugin_id: str` | No |
| `get_plugin_schema` | Returns the JSON Schema used to generate dynamic UI forms. | `plugin_id: str` | No |

**III. System Health**

| Tool Name | Description | Parameters | TOON |
|---|---|---|---|
| `get_system_health` | Checks connectivity to Redis, PostgreSQL, and the Manager API. | None | No |

#### 8.5 Authentication Flow

```
MCP Client (AI Assistant)
    │
    │  Sends: Authorization: Bearer <AUTOKB_API_KEY>
    ▼
autokb-mcp (MCP Server)
    │
    │  Extracts Bearer token via AuthMiddleware, stores in ContextVar
    │  Forwards to autokb-web:80 with same Bearer token
    ▼
autokb-web (Web UI)  ← external entry point (port 80)
    │
    │  Validates Bearer token against AUTOKB_API_KEY
    │  Adds AUTOKB_BACKEND_API_KEY header
    │  Proxies to autokb-manager:80 (internal)
    ▼
autokb-manager (Manager API)
    │
    │  Validates AUTOKB_BACKEND_API_KEY
    │  Processes request
    ▼
PostgreSQL / Redis
```

#### 8.6 API Mapping (MCP Tools → AutoKB Manager Endpoints)

| MCP Tool | AutoKB API Endpoint | HTTP Method |
|---|---|---|
| `create_subscription` | `/api/subscriptions/{plugin_id}` | POST |
| `edit_subscription` | `/api/subscriptions/{sub_id}` | PUT |
| `delete_subscription` | `/api/subscriptions/{sub_id}` | DELETE |
| `trigger_manual_update` | `/api/subscriptions/{sub_id}/trigger` | POST |
| `set_subscription_status` | `/api/subscriptions/{sub_id}/status` | PUT |
| `list_subscriptions` | `/api/subscriptions` | GET |
| `get_subscription_status` | `/api/subscriptions/{sub_id}` | GET |
| `list_plugins` | `/api/plugins` | GET |
| `get_plugin_details` | `/api/plugins/{plugin_id}` | GET |
| `get_plugin_schema` | `/api/plugins/{plugin_id}/schema` | GET |
| `get_system_health` | `/api/health` | GET |

#### 8.7 MCP-Specific Environment Variables

| Variable | Default | Description |
|---|---|---|
| `AUTOKB_BASE_URL` | `http://autokb-web:80` | Base URL for the AutoKB front-end (MCP routes through Web UI) |
| `AUTOKB_API_KEY` | (required by Web UI) | Bearer token for MCP → Web UI authentication (validated by Web UI; MCP passes through) |
| `MCP_PORT` | `80` | Port for the MCP uvicorn server |

### 9. Database & Persistence

#### 9.1 PostgreSQL Models

**`Subscription`**
| Column | Type | Notes |
|---|---|---|
| `id` | UUID | PK |
| `plugin_id` | String | FK → PluginRegistryState(plugin_id). Sanitized name (e.g., `biblePlugin`) |
| `name` | String | Sanitized, **immutable** after creation. Unique constraint on `(plugin_id, name)` |
| `config` | JSONB | Validated against `get_schema()` at write. `_extra_param_1`, `_extra_param_2`, `_extra_param_3` (system-reserved string keys, see §3.3 "Extra Parameters invariant") are auto-included in every plugin's JSON schema and auto-injected into every persisted `config` by `DatabaseManager.create_subscription` / `update_subscription` (with `""` defaults if absent). `DatabaseManager.backfill_extra_params()` runs once on manager startup to bring legacy rows into compliance. Values matching `"format": "password"` in the plugin schema are encrypted at rest using Fernet symmetric encryption with the key provided via the `ENCRYPTION_KEY` environment variable (see §9.2). |
| `status` | String | `ENABLED`, `ENQUEUED`, `IN_PROGRESS`, `ERROR`, `DISABLED`, `DELETED` |
| `last_updated` | DateTime | |
| `last_heartbeat` | DateTime | Set to `NOW()` when `IN_PROGRESS` is set (non-NULL initial value). Watchdog uses this to detect staleness. |
| `last_error` | Text | Nullable. Cleared when user re-enables from ERROR; otherwise persists until overwritten by a subsequent failure. |
| `access_level` | String(7) | `PRIVATE` or `PUBLIC`. Required, NOT NULL. Defaults to the plugin's `DEFAULT_ACCESS_LEVEL` at creation if not explicitly provided. |
| `progress` | Integer | 0–100 |
| `sub_type` | String | `SCHEDULED` or `EVENT_BASED` |
| `cron` | String | Nullable. Cron expression for scheduling |

Unique constraint: `(plugin_id, name)` enforced at the database level. The Manager handles any constraint violation errors gracefully and reports them back to the user.

**`EventLog`**
| Column | Type | Notes |
|---|---|---|
| `id` | UUIDv7 | PK. Generated at the application level using the `uuid7` Python package. |
| `subscription_id` | UUID | FK → Subscription with `ON DELETE CASCADE` |
| `executed_at` | DateTime | Default now() |
| `exit_code` | Integer | `0` = success, `1` = error, `2` = timeout, `3` = schema validation failure |
| `exit_string` | String(255) | Empty on success. Truncated error summary on failure. |

> **⚠️ NOTICE — The EventLog database table is a transient execution counter tied to active subscriptions only. Events are cascade-deleted when the subscription is deleted (`ON DELETE CASCADE`). This is by design — EventLog is NOT a permanent audit trail. For long-term execution history, operators must retain `/logs/*.log` files via external log aggregation.**

**`PluginRegistryState`**
| Column | Type | Notes |
|---|---|---|
| `plugin_id` | String | PK (sanitized name). Referenced by Subscription.plugin_id as FK. |
| `schema_hash` | String | `hashlib.sha256` hexdigest of augmented `get_schema()` output |

#### 9.2 DatabaseManager

- Thread-safe via `sqlalchemy.orm.scoped_session(sessionmaker(bind=engine))`.
- Watcher daemon threads create their own sessions (via `get_session()` context manager).
- `scoped_session` ensures thread-local session isolation — each OS thread gets its own session automatically. The watcher thread and main Level-1 thread are different OS threads and get isolated sessions. Child processes inherit via fork and get their own thread identity.
- Migrations managed via **Alembic**. Migration scripts in `manager/alembic/versions/`. Run at Manager startup via `alembic upgrade head`.
- Programmatic `NOTIFY`: All write methods (`update_status`, `update_heartbeat_and_progress`, etc.) execute `SELECT pg_notify('subscription_updated', :sub_id)` within the same transaction.

- **DELETED state guard:** Every method that modifies the `status` column (`update_status`, `update_status_and_heartbeat`, `set_subscription_status`) uses `WHERE id = :sid AND status != 'DELETED'` with a rowcount check. If the rowcount is zero, the update was blocked by the DELETED guard — `pg_notify` is skipped and the operation is silently ignored. This single DB-layer invariant prevents any caller (Worker post-execution paths, monitor cancellation, etc.) from accidentally transitioning out of DELETED. Only `try_enqueue`'s DELETED special case (which does not modify status) and `delete_subscription_row` (which removes the row entirely) bypass this guard.

  **Note:** `update_heartbeat_and_progress()` also uses the same DELETED guard (`WHERE status != 'DELETED'`) to prevent heartbeat updates on deleted subscriptions, but it modifies `last_heartbeat` and `progress`, not `status`. It is not grouped with the status-modifying methods above.

- **DISABLED/ERROR invariant guard:** System-generated status transitions enforce `WHERE status IN ('ENQUEUED', 'IN_PROGRESS')` (Worker success-to-ENABLED, watchdog, worker claiming work) alongside `WHERE status NOT IN ('DELETED', 'DISABLED')` (error-setting transitions), ensuring they never override a user-set DISABLED or system-set ERROR state. The `try_enqueue()` function naturally respects DISABLED and ERROR via `WHERE status IN ('ENABLED', 'ENQUEUED', 'IN_PROGRESS')`.

- **last_error preservation:** All methods that update the `last_error` column use `COALESCE(:error, last_error)` in their SQL. This ensures that when called without a new error value (success path), the existing error is preserved. Error messages persist until overwritten by a subsequent failure. When the user sets `ENABLED` from `ERROR`, `last_error` is explicitly set to `NULL` to clear the error.

- **Atomic enqueue:** `try_enqueue(sub_id)` first checks the current status. If the subscription is already `DELETED`, it returns `True` (enqueue for cleanup) without modifying the DB status — the `DELETED` state must never change. Otherwise, it uses `UPDATE subscriptions SET status = 'ENQUEUED' WHERE id = :sid AND status IN ('ENABLED', 'ENQUEUED', 'IN_PROGRESS')` with a rowcount check to prevent TOCTOU races. This design ensures cleanup of `DELETED` subscriptions can proceed after crashes or restarts while preventing any other state transition out of `DELETED`. ERROR and DISABLED subscriptions are naturally excluded.

- **record_execution guard:** `record_execution()` checks the subscription's current status before inserting an EventLog entry. If the subscription is None (row already deleted) or has status DELETED, the entry is skipped. This prevents stale execution events for subscriptions that were deleted during an overlapping execution.

- **Password encryption/decryption:** Values matching `"format": "password"` in the plugin schema are encrypted at rest using Fernet symmetric encryption. The `ENCRYPTION_KEY` environment variable provides the Fernet key. On write (create/update): password fields in the config JSONB are validated against all schema constraints on the plaintext value, then encrypted before being stored. On read (all API responses, SSE payloads, Edit GET): password-format fields are **excluded** from the response entirely — they are not present in the JSON payload. **Edit PUT password handling:**
   - Key absent → keep existing encrypted value. The `required` keyword is not enforced on Edit for password fields (it is enforced on Create).
   - Key present, non-empty string → validate against all schema constraints, then encrypt and store.
   - Key present, empty/null → keep existing encrypted value (no clearing).
   On read (Worker execution): password fields are decrypted before being passed to `getData(config, ...)`. The encryption/decryption logic is implemented as a utility in `DatabaseManager` or `utils/misc_utils`.

- **Access level persistence:** `access_level` is treated as a top-level subscription column alongside `status`, `cron`, and `sub_type`. The `DatabaseManager` persists it at creation time (defaulting to the plugin's `DEFAULT_ACCESS_LEVEL` if not explicitly provided in the request) and updates it via the Edit subscription path. It is included in all subscription read responses and SSE payloads. The `access_level` column is never part of the `config` JSONB — it is a separate first-class column.

#### 9.3 Watchdog Query Safety

The Manager's watchdog loop uses the following SQL condition to detect stale `IN_PROGRESS` subscriptions. The timeout threshold is computed as `WORKER_HEARTBEAT_TIMEOUT * 3` at startup (default: 900s / 15 minutes) and passed as a parameter (not hardcoded). Because `last_heartbeat` is initialized to `NOW()` when `IN_PROGRESS` is set, NULL is never a concern:

```sql
WHERE status IN ('ENQUEUED', 'IN_PROGRESS')
  AND last_heartbeat IS NOT NULL
  AND (NOW() - last_heartbeat) > make_interval(secs => :watchdog_timeout_s)
```

The `:watchdog_timeout_s` parameter is computed at startup as `HEARTBEAT_TIMEOUT * 3`. Changing `HEARTBEAT_TIMEOUT` from its default (300s) automatically adjusts the watchdog threshold — no hardcoded value is used. Both constants are defined in `src/utils/constants.py` and imported by both `scheduler.py` and `execution_engine.py`.

The watchdog's subsequent `update_status` call uses `WHERE status IN ('ENQUEUED', 'IN_PROGRESS')` to ensure it does not override a concurrent user action (e.g., setting DISABLED) while also handling subscriptions that were re-enqueued during execution.

#### 9.4 State Logic Summary

- **States:** `ENABLED`, `ENQUEUED`, `IN_PROGRESS`, `ERROR`, `DISABLED`, `DELETED`
- `ENABLED` is the baseline active state.
- `ENQUEUED` and `IN_PROGRESS` are transient active states.
- `DISABLED` is the user-paused state. `ERROR` is the system-failed state. Both are logically identical for execution gating.
- `DELETED` is the terminal/transient state. Set only by the Manager delete endpoint. No method or function may transition out of `DELETED`.
- Enqueuing allowed if status in `(ENABLED, ENQUEUED, IN_PROGRESS, DELETED)`.
- If status is `ERROR`, `DISABLED`, or `DELETED`, trigger requests are discarded.
- `PUT /api/subscriptions/{sub_id}/status` accepts `"ENABLED"` or `"DISABLED"`. Setting `ENABLED` while in `ERROR` transitions to `ENABLED` and clears `last_error`. When setting DISABLED, the running monitor loop is cancelled. When setting ENABLED (from any state), the cron expression is re-validated first; if invalid, the transition is rejected with HTTP 400 and body `{"error": "Invalid cron expression: {cron}"}`. On successful validation, the monitor loop is started for EVENT_BASED subscriptions. Returns 400 if subscription is DELETED (terminal state — cannot modify).

#### 9.5 asyncpg for LISTEN/NOTIFY

The Manager uses a dedicated `asyncpg` connection pool **solely** for subscribing to PostgreSQL `LISTEN`/`NOTIFY` and bridging notifications to async SSE clients. The existing sync SQLAlchemy engine remains unchanged for all other database operations. This is a lightweight, dedicated connection — not a replacement of the existing sync SQLAlchemy layer.

### 10. Observability

#### 10.1 SMTP Notifications

Sent via `SMTP_NOTIFY_EMAIL` env variable. SMTP connection uses `SMTP_USE_TLS` (STARTTLS, default `True`) or `SMTP_USE_SSL` (implicit TLS, default `False`). If both are set, `SMTP_USE_TLS` takes precedence. Triggers (with origin):
- Worker heartbeat timeout (Worker Level-1 parent)
- Non-timeout subscription exceptions (Worker Level-1 parent)
- Schema validation failure (pre-execution) (Worker Level-1 parent)
- System/worker crashes (Manager)
- Schema-breaking changes (Manager)
- Orphaned output directory cleanup failures (Worker Level-1 parent)
- Watchdog force-release of stale locks (Manager)

#### 10.2 Logging

All system logs written to per-container files in `/logs/` with timestamps and context (sub_id, plugin_id):
- `/logs/manager.log`
- `/logs/worker.log`
- `/logs/mcp.log`
- `/logs/web.log`

**Audit trail:** The `/logs/*.log` files are the primary forensic record for execution history, capturing full stack traces, exit codes, and error context. The EventLog database table (§9.1) is a **transient execution counter** used exclusively for the 24-hour activity monitor — it is not an audit trail. The `ON DELETE CASCADE` foreign key on `EventLog.subscription_id` is intentional: deletion of a subscription removes its transient event counters, while the full execution record persists in the log files. Operators requiring long-term audit history should retain `/logs/` via external log aggregation.

#### 10.3 Health Endpoint

`GET /api/health` checks connectivity to Redis (PING), PostgreSQL (SELECT 1), and confirms the PluginRegistry is loaded.

#### 10.4 Logging Standards

Two log levels are used across all components: **INFO** for normal operations and **DEBUG** for internal development/troubleshooting detail.

**INFO Events (complete list)**

| Component | Event | Log Action |
|---|---|---|
| Scheduler | Subscription enqueued | `source=cron sub_id=X name="Y"` |
| Scheduler | Subscription enqueued | `source=monitor sub_id=X name="Y"` |
| Scheduler | Subscription enqueued | `source=manual_trigger sub_id=X name="Y"` |
| Scheduler | Startup recovery scan | `recovered N subscriptions (IN_PROGRESS=M ENQUEUED=N DELETED=P)` |
| Scheduler | Monitor started/stopped | `monitor_{started/stopped} sub_id=X name="Y" plugin=Z` |
| Worker | Execution claimed | `worker-N claimed sub_id=X name="Y"` |
| Worker | Execution completed | `worker-N completed sub_id=X name="Y" exit_code=0 success` |
| Worker | Execution failed | `worker-N failed sub_id=X name="Y" exit_code=1 error="..."` |
| Worker | Execution timed out | `worker-N timeout sub_id=X name="Y" exit_code=2 HEARTBEAT_TIMEOUT exceeded` |
| Worker | Schema validation failure | `worker-N config_rejected sub_id=X name="Y" exit_code=3 error="..."` |
| Manager | Subscription created | `sub_id=X name="Y" plugin_id=Z created` |
| Manager | Subscription deleted | `sub_id=X name="Y" deleted` |
| Manager | Subscription edited | `sub_id=X name="Y" edited` |
| Manager | State transition | `sub_id=X name="Y" OLD_STATUS→NEW_STATUS by=user/admin` |
| Manager | Monitor cancellation | `sub_id=X name="Y" cancelled (reason: delete/disable/edit)` |
| Manager | Plugin loaded | `plugin_id=X loaded (N subscriptions)` |
| Manager | Plugin schema change | `plugin_id=X schema_hash changed — N subscriptions disabled` |
| Watchdog | Stale lock force-released | `worker-N timeout sub_id=X name="Y" watchdog force_released` |
| Watchdog | Subscription marked ERROR | `sub_id=X name="Y" watchdog marked ERROR` |
| SMTP (Worker) | Notification sent | `smtp sent type=heartbeat_timeout sub_id=X name="Y"` |
| SMTP (Manager) | Notification sent | `smtp sent type=watchdog_force_release sub_id=X name="Y"` |

**DEBUG Events (by component)**

| Component | Debug Logging |
|---|---|
| **Redis** | Lock acquire/release/refresh/force-release, TTL reset, queue push/pop/drain/collapse, queue depth, connection pool status |
| **Database (DB)** | SQL query execution, pg_notify sent, rowcount results, connection acquire/release, pool status, migration execution |
| **Worker** | Level-1 loop iteration count, Level-2 process spawn (PID), heartbeat_event set/clear/wait, watcher thread start/stop/timeout, `proc.join()` return, `proc.terminate()`/`proc.kill()`, exit code received |
| **SSE** | Client connect/disconnect, total connected clients, keepalive sent, event pushed (event type), `ConnectionResetError` caught, cleanup |
| **File Watcher** | File created/modified/deleted detected, debounce timer start/expiry, plugin reload triggered, load result (success/failure + reason) |
| **Monitor Loop** | Iteration start/end, cancel_token check, `monitor()` return value (True/False/exception), sleep duration, exception type + traceback (before MONITOR_ERROR_SLEEP retry) |
| **Scheduler** | Cron evaluation (next trigger time), cron expression parse result, trigger dispatch, startup recovery scan iteration per subscription |
| **Watchdog** | Iteration start, subscription(s) checked, last_heartbeat vs computed threshold, force-release result, update_status result |
| **Plugin Loader** | File found, import attempt, metadata validation, `DEFAULT_ACCESS_LEVEL` validation, schema hash comparison, hash match/mismatch value, rejection reason |

**Mandatory Log Entry Fields**

Every log entry, regardless of level, MUST include these fields in a consistent structured format:

| Field | Description | Example |
|---|---|---|
| `timestamp` | ISO 8601 with timezone | `2026-06-02T14:30:00.123Z` |
| `level` | Log level | `INFO` / `DEBUG` |
| `component` | Originating component | `worker-3` / `scheduler` / `watchdog` / `sse` / `file_watcher` |
| `subscription_id` | UUID of the subscription (or `-` if none) | `abc123` / `-` |
| `subscription_name` | Name of the subscription (or `-` if none) | `MySubscription` / `-` |
| `action` | What happened | `enqueued` / `completed` / `lock_acquired` |
| `result` | Outcome or detail | `exit_code=0` / `timeout=300` / `success` |

Components are identified as: `manager`, `scheduler`, `worker-N` (N = 0-indexed process number), `watcher-N` (N = 0-indexed), `watchdog`, `file_watcher`, `sse`, `db`, `redis`. SMTP notifications use the originating component (`worker-N`, `watchdog`, `file_watcher`, etc.) as the `component` field.

### 11. Output Hierarchy & Immutability

```
/output/{sanitized_plugin_name}/{sanitized_subscription_name}/{data_files}
```

- A subscription's name **cannot be changed** after creation.
- Deleting a subscription sets its status to `DELETED` and pushes it to the Primary Queue. The Manager also pushes a `subscription_deleted` event to SSE streams and calls `cancel_monitor(sub_id)` for EVENT_BASED subscriptions. The Worker handles the actual cleanup: upon acquiring the lock and detecting `DELETED` status, it removes the output directory via `shutil.rmtree` and deletes the DB row. Returns 409 if the subscription is already DELETED.
- The Worker silently skips any subscription_id that does not exist in the DB (race-safe).
- Creating a subscription checks whether the target output directory already exists. If it does, the creation is rejected with HTTP 409 and body `{"error": "Output directory already exists for subscription name '{name}'"}`. A database-level unique constraint on `(plugin_id, name)` provides a secondary guard against duplicate names.

### 12. Plugin Deletion Rules

- A plugin **cannot be deleted** if it has any subscriptions attached.
- `DELETE /api/plugins/{plugin_id}` → returns HTTP 409 Conflict with body `{"error": "Cannot delete plugin with N active subscriptions"}` if any subscriptions reference the plugin.
- This constraint applies across all interfaces: direct API calls, MCP tools, and Web UI. All subscriptions referencing the plugin must be manually deleted first.
- This does not affect the Developer Lab — code validation and saving in `/src/plugins/` are separate operations.
- When a plugin is successfully deleted (all subscriptions removed first), the output directory `/output/{plugin_id}` is also removed via `shutil.rmtree`, and the plugin is immediately removed from the in-memory `PluginRegistry` so it no longer appears in API responses.
- **Race condition protection:** The plugin deletion endpoint and the subscription creation endpoint both use `SELECT ... FOR UPDATE` on the `PluginRegistryState` row to serialize access. The subscription creation endpoint acquires the `FOR UPDATE` lock on `PluginRegistryState` as its **first** step (before any in-memory checks), then re-verifies the plugin exists in the in-memory `PluginRegistry` under the safety of the lock. The plugin deletion endpoint performs the `DELETE FROM PluginRegistryState` **inside the same transaction** as the `FOR UPDATE` lock (before `COMMIT`), eliminating the window where a concurrent creation could succeed between the subscription count check and row deletion. The FK constraint on `Subscription.plugin_id` provides the ultimate safety guarantee.

  > **Note:** The in-memory `PluginRegistry` check within the create subscription endpoint is a best-effort optimization. The FK constraint (`Subscription.plugin_id → PluginRegistryState.plugin_id`) is the real safety net for referential integrity at the database level. The `FOR UPDATE` lock serializes access to the `PluginRegistryState` row, and the FK constraint guarantees no orphaned subscriptions can exist regardless of any timing race.

- **⚠️ Operational Note:** The subscription count check includes subscriptions in all states, including `DELETED`. Since `DELETED` subscriptions are cleaned up asynchronously by the Worker (output directory removal → DB row deletion), a plugin may remain undeletable if its subscriptions are in the `DELETED` state but the Worker has not yet processed the cleanup. This is safe by design (preventing referential integrity violations), but operators should be aware that:
  - The Worker container must be running to process pending DELETED cleanup tasks.
  - If the Worker never processes a DELETED subscription, the plugin referencing it can never be removed.
  - In emergencies, manual database cleanup is required (not recommended).

### 13. Environment Variables Summary

| Variable | Default | Used By |
|---|---|---|
| `MANAGER_PORT` | `80` | Manager |
| `WEBUI_PORT` | `80` | Web UI |
| `MCP_PORT` | `80` | MCP Server |
| `AUTOKB_BASE_URL` | `http://autokb-web:80` | MCP (routes through Web UI) |
| `AUTOKB_MANAGER_URL` | `http://autokb-manager:80` | Web UI proxy (routes to Manager) |
| `AUTOKB_ADMIN_USERNAME` | `admin` | Web UI |
| `AUTOKB_ADMIN_PASSWORD` | (required) | Web UI |
| `AUTOKB_API_KEY` | (required for MCP) | MCP, Web UI (validation) |
| `AUTOKB_BACKEND_API_KEY` | (auto-generated recommended) | Web UI, Manager |
| `WORKER_COUNT` | `4` | Worker |
| `REDIS_URL` | `redis://autokb-redis:6379/0` | All |
| `DATABASE_URL` | `postgresql://autokb:autokb@autokb-db:5432/autokb` | All |
| `SMTP_HOST`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASS`, `SMTP_NOTIFY_EMAIL` | — | Manager, Worker (SMTP) |
| `SMTP_USE_TLS` | `True` | Enable STARTTLS for SMTP. Takes precedence over SMTP_USE_SSL |
| `SMTP_USE_SSL` | `False` | Enable implicit SSL/TLS for SMTP |
| `ENCRYPTION_KEY` | (required) | Fernet key for encrypting password-format fields at rest |
| `MAX_STARTUP_RETRIES` | `100` | Maximum connection retry attempts for Redis and PostgreSQL on startup (Manager, Worker) |
| `STARTUP_RETRY_SLEEP` | `1` | Seconds between connection retry attempts on startup |

### 14. Overall Data Flow Summary (K.I.S.S. Strategy)

The frontend elements (Web UI, API, MCP server) generate changes that need to be performed. Those changes are immediately committed to the database, and a subsequent job (with just the `subscription_id`) is placed onto the Redis queue. The workers perform the backend work by looking at the database for the actual work details and performing the work. The primary and secondary queues allow the worker to keep working on a subscription while they have a lock on that subscription, ensuring one worker per subscription. The worker commits changes back to the DB, and the manager is listening for those DB changes (via `pg_notify`/`LISTEN`) and passing them along to other listeners (SSE to Web UI, etc.).

| Step | Component | Action |
|---|---|---|
| 1 | Web UI / MCP / Cron / Monitor | Triggers a subscription change |
| 2 | Manager | Commits to DB, pushes `subscription_id` to Redis P-Queue |
| 3 | Worker | Pops from P-Queue, acquires lock, drains queues, reads details from DB |
| 4 | Worker | Executes `getData()` via Level-2 process with heartbeat monitoring |
| 5 | Worker | Writes results to DB (status, progress, event log) |
| 6 | DB | Sends `pg_notify('subscription_updated', sub_id)` in same transaction |
| 7 | Manager | Receives `LISTEN` notification via asyncpg, bridges to SSE clients |
| 8 | Web UI | Reactively updates UI from SSE events |

---

### 15. Test Plugins

The following test plugins exercise every major behavior path in the AutoKB system. Each plugin is designed to exploit a specific scenario — happy path, edge case, or failure mode — using constants from `src/utils/constants.py` (`HEARTBEAT_TIMEOUT`, `MONITOR_ERROR_SLEEP`, etc.) so that changing the constants scales all test behavior accordingly.

#### 15.1 Test Plugin Table

| # | Plugin Name | Type | What It Tests | Design |
|---|---|---|---|---|
| 1 | `happyPathPlugin.py` | SCHEDULED | Normal success path | Calls `progress_callback` at 25, 50, 75, 100. Writes one small file to `/tmp/` with `time.sleep(0.05)` during the write to simulate I/O. Calls `move_to_destination()`. Exits cleanly with code 0. |
| 2 | `eventHappyPlugin.py` | EVENT_BASED | EVENT_BASED success | `monitor()` uses a call counter; returns `True` on the 3rd invocation and `False` otherwise. A `_fired` flag prevents re-triggering after the first successful enqueue — the monitor fires exactly once per monitor lifetime. `getData()` succeeds immediately. Tests that the monitor triggers exactly one enqueue and the subscription executes once per trigger. |
| 3 | `noHeartbeatPlugin.py` | SCHEDULED | Heartbeat timeout → ERROR | `getData()` sleeps for `HEARTBEAT_TIMEOUT * 2` without ever calling `progress_callback`. The watcher terminates the child at `HEARTBEAT_TIMEOUT`. Expected: EventLog exit_code=2, status=ERROR, SMTP notification sent. |
| 4 | `longRunningSuccessPlugin.py` | SCHEDULED | Long execution with regular heartbeats → success | Runs for `HEARTBEAT_TIMEOUT * 2`, calling `progress_callback` every `HEARTBEAT_TIMEOUT / 10`. Writes files with `time.sleep(0.05)` between writes. Tests that the Redis lock TTL is refreshed by `progress_callback`. Should complete with status ENABLED. |
| 5 | `longRunningFailurePlugin.py` | SCHEDULED | Long execution with regular heartbeats → runtime error | Runs for `HEARTBEAT_TIMEOUT`, calling `progress_callback` every `HEARTBEAT_TIMEOUT / 10`, then raises `RuntimeError`. Parent records EventLog exit_code=1, sends SMTP. Tests that errors after successful heartbeats are handled correctly. |
| 6 | `crashPlugin.py` | SCHEDULED | Immediate exception → ERROR | `getData()` raises `Exception("Something went wrong")` immediately. The wrapper serializes the exception to the `exception_queue`. EventLog exit_code=1 with `exit_string="Exception: Something went wrong"`, SMTP sent, full traceback logged, status=ERROR. Tests the full exception capture path (child serializes, parent reads queue and records EventLog/SMTP/status). |
| 7 | `cancellationPlugin.py` | SCHEDULED | Graceful cancellation mid-execution | Loops N iterations, calling `progress_callback` every `HEARTBEAT_TIMEOUT / 30`. The `progress_callback` checks DB status and raises `SubscriptionCancelledError` if status is DISABLED or DELETED. Child exits with code 0. Parent sees exit code 0 and status DISABLED/DELETED → skips EventLog entry. |
| 8 | `schemaBreakingPlugin.py` | SCHEDULED | Breaking schema change (in-place modification) | Single file modified in-place by the test runner. V1 schema: fields `["title", "author"]`. V2 schema: fields `["title", "writer"]` (field renamed). Hash differs. The test runner saves V1, creates subscriptions, then overwrites with V2 and triggers a file watcher reload. Manager detects hash mismatch → all V1 subscriptions DISABLED, SMTP sent, plugin refused loading. Tests the breaking-change detection. |
| 9 | `passwordPlugin.py` | SCHEDULED | Password field encryption | Schema has one field: `{"name": "apiKey", "type": "string", "format": "password"}`. Test: value encrypted at rest in JSONB, excluded from GET/SSE/Edit GET, decrypted for Worker execution, blank on Edit PUT keeps existing value. |
| 10 | `emptyOutputPlugin.py` | SCHEDULED | No output files | `getData()` succeeds but never calls `move_to_destination()`. No files appear in `/output/`. Status set to ENABLED. Tests that the system tolerates plugins that produce no output. |
| 11 | `largeOutputPlugin.py` | SCHEDULED | Large file generation and cleanup | Writes 100 files of 10MB each to `/tmp/`, each write followed by `time.sleep(0.05)`, then calls `move_to_destination()` once. On delete, `shutil.rmtree` must clean up the entire directory. Tests disk usage and rmtree performance. |
| 12 | `delayedInitPlugin.py` | SCHEDULED | Long initialization before first heartbeat | `getData()` sleeps for `HEARTBEAT_TIMEOUT * 0.8` before making its first `progress_callback` call. Tests that the auto `progress_callback(0)` call at the start provides enough buffer for initialization phases. |
| 13 | `customRoutePlugin.py` | SCHEDULED | Custom API routes | Implements `get_custom_routes()` returning `[PluginRoute(path="/status", method="GET", handler=lambda: {"status": "ok"})]`. Tests that `dynamic_mount.py` mounts the route and it is accessible. |
| 14 | `invalidNamePlugin.py` | SCHEDULED | sanitize_name rejection | `metadata["name"]` contains consecutive periods (`"bad..name"`). Plugin load validation rejects with `ValueError`. Tests that the naming constraint is enforced. |
| 15 | `monitorNeverTriggerPlugin.py` | EVENT_BASED | Silent event failure → fallback cron | `monitor()` always returns `False`. The subscription relies on its fallback cron expression (default: daily). Tests that the system still triggers `getData()` via the cron path even when the event mechanism produces no triggers. |
| 16 | `monitorErrorPlugin.py` | EVENT_BASED | Monitor exceptions → retry loop | `monitor()` raises `ConnectionError` on every invocation. The Manager logs the exception, sleeps `MONITOR_ERROR_SLEEP` seconds, and retries the loop. Tests that the monitor loop never crashes and continues retrying indefinitely. |
| 17 | `configValidationPlugin.py` | SCHEDULED | All schema field types | Schema includes one field of each type: text, enum combo box (options `["A","B","C"]`), radio button (options `["X","Y"]`), checkbox (boolean), password, plus `_extra_param_1/2/3`. Tests form rendering, submission, and validation end-to-end. |
| 18 | `nonZeroExitPlugin.py` | SCHEDULED | `sys.exit(1)` without Python exception | `getData()` calls `sys.exit(1)` directly — no Python exception is raised, so no exception detail is placed on the `exception_queue`. The parent finds the queue empty and uses the generic fallback `exit_string="Subscription failed with exit code 1"`. EventLog exit_code=1, SMTP sent, status=ERROR. Tests the empty-queue fallback path. |
| 19 | `zombiePlugin.py` | SCHEDULED | Ignores cancellation, force-killed by watcher | `progress_callback` never checks DB status. `getData()` sleeps for `HEARTBEAT_TIMEOUT * 2`. User sets status to DISABLED mid-execution. The child process ignores the cancellation and keeps running until `HEARTBEAT_TIMEOUT` elapses → watcher calls `proc.terminate()` (then `proc.kill()` as fallback). Tests the force-termination path. |
| 20 | `moveToDestErrorPlugin.py` | SCHEDULED | Invalid output path → ValueError | `getData()` calls `self.move_to_destination(".")`. `sanitize_name(".")` raises `ValueError` (period-only input). The exception is serialized to the `exception_queue` with full traceback. Parent reads the queue, records EventLog with `exit_string="ValueError: ..."`, logs the traceback, sends SMTP, status=ERROR. Tests the `sanitize_name` error path with full exception capture. |
| 21 | `longNamePlugin32CharNameForUITes.py` | SCHEDULED | 32-char plugin name UI layout | Plugin name is exactly 32 characters (`longNamePlugin32CharNameForUITes`). Tests that the Data Sources grid and subscription list correctly truncate long names with `text-overflow: ellipsis` and that the 32-character server-side limit is enforced. |
| 22 | `editMatchPlugin.py` (V1) | SCHEDULED | Edit Plugin schema match | Schema: `{"label": {"type": "string", "minLength": 1}}`. `getData()` writes `VERSION_1` to output. The test creates a subscription, saves V1 via Edit Plugin mode (schema hash matches → save succeeds), triggers execution, and verifies output contains `VERSION_1`. |
| 23 | `editMatchPlugin.py` (V2) | SCHEDULED | Edit Plugin schema mismatch | Same plugin name but schema changed to `{"label": {"type": "string", "minLength": 1}, "extra": {"type": "string"}}` (field added). Hash differs. The test attempts to save V2 via Edit Plugin mode → save rejected with HTTP 400, existing plugin untouched. Tests the schema-stability invariant. |
| 24 | `eventOftenPlugin.py` | EVENT_BASED | EVENT_BASED fires on enable + every 42s | `monitor()` fires immediately on first call (after enable), then every 42 seconds. `getData()` writes output and calls `move_to_destination()`. Tests that EVENT_BASED subscriptions trigger repeatedly at the expected interval, not just once. |
| 25 | `deleteAllPlugin.py` | SCHEDULED | Subscription-and-plugin deletion cascade | Minimal SCHEDULED plugin. The test creates a subscription, then deletes both the subscription and the plugin in sequence. Verifies that: subscription status transitions to DELETED, output directory is cleaned up, plugin is removed from the registry, and no orphaned DB rows remain. Tests the full deletion lifecycle. |
| 26 | `cronRandomizePlugin.py` | SCHEDULED | Cron randomization at creation | Minimal SCHEDULED plugin that writes no output. The test creates a subscription with `cron="0 * * * *"` and verifies the stored cron has a randomized minute. Also tests with `cron="0 0 * * *"` for EVENT_BASED randomization. Verifies default cron strings are never stored. |

#### 15.2 Usage Notes

- Test plugins are placed in `/src/plugins/` like any other plugin. No special test harness is required — the system treats them identically to production plugins.
- **Self-contained test directory:** The canonical source for test plugins lives in `src/testing/plugins/` (25 files). At test startup, `_sync_test_plugins()` copies them to `/src/plugins/` so the test runner always exercises a known-good baseline. This directory also contains `test_runner.py`, the automated test orchestrator.
- **Test runner invocation:** `python /src/testing/test_runner.py confirm [--reset]`. The `confirm` argument is a safety guard to prevent accidental execution. `--reset` wipes all subscriptions, the event log, test-created plugin files, and `/output/` subdirectories before running.
- **Access Level Defaults:** All test plugins define `DEFAULT_ACCESS_LEVEL` (defaulting to `"PRIVATE"` from the base class). Most test plugins leave the default as `"PRIVATE"` since test data is generally synthetic. Plugins that represent inherently public test data sources (e.g., `happyPathPlugin.py`) may set `DEFAULT_ACCESS_LEVEL = "PUBLIC"` to exercise both access level code paths.
- To run a test, create a subscription for the test plugin via the Web UI or API, trigger it, and observe the expected behavior (status transitions, EventLog entries, SMTP notifications, log output).
- The default constant values (`HEARTBEAT_TIMEOUT=300`, `MONITOR_ERROR_SLEEP=10`) produce the time ranges described above. For faster iteration during testing, set `HEARTBEAT_TIMEOUT=10` (or lower) as an environment variable — all test plugin behaviors scale proportionally.
- Some test plugins require manual intervention during execution (e.g., setting a subscription to DISABLED while IN_PROGRESS for `cancellationPlugin.py` and `zombiePlugin.py`). These are intentional — they test system-invariant enforcement.
- `schemaBreakingPlugin.py` is a single file that the test runner modifies in-place to simulate breaking changes. V1 uses schema fields `["title", "author"]`; V2 renames to `["title", "writer"]`. The test runner overwrites the file and triggers a file watcher reload to exercise the breaking-change detection path.

---
