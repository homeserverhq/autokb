<picture>
  <source media="(prefers-color-scheme: dark)" srcset="assets/akb_full_logo.png">
  <img alt="AutoKB Logo" src="assets/akb_full_logo.png" width="438" height="80">
</picture>

**Distributed, Event-Driven ETL Orchestration Engine**

AutoKB connects to diverse data sources through a hot-swappable plugin architecture, pulls data on a schedule or in response to events, and writes structured output files for consumption by downstream Knowledge Base importers. It is designed as the ingestion layer for Retrieval-Augmented Generation (RAG) pipelines, semantic search indexes, and any system that needs a steady feed of chunked, pre-processed content.

---

## Key Features

- **Pluggable Architecture** — Data sources are single Python files. Drop a new plugin into `/src/plugins/` and it is hot-swapped into the running system within ~2 seconds. No restart, no config change, no deployment pipeline.
- **Data Destinations (Sinks)** — Hot-swappable destination connectors in `/src/sinks/` sync plugin output to remote Knowledge Bases / datasets (Data Targets) — Open WebUI, RAGFlow, and LightRAG are built in. A reconciliation engine diffs `/output/` against remote state after every run.
- **Built-In Seeding** — On startup, the built-in plugins/sinks baked into the image (`/src/builtin_plugins/`, `/src/builtin_sinks/`) are copied into the live (host-mounted) `/src/plugins/` and `/src/sinks/` directories **only if they don't already exist**. Same-name user files are never overwritten, so upgrades can ship new built-ins without clobbering customizations. Existing built-ins are not auto-updated; see [Built-in Seeding and Upgrade Recovery](#built-in-seeding-and-upgrade-recovery).
- **Dual Interface** — Full-featured dark-mode Web UI (SPA with SSE live updates) for human operators, plus an MCP (Model Context Protocol) server that lets AI assistants act as remote administrators.
- **Two-Tier Queue with Aggressive Collapsing** — Redis-backed P-Queue / S-Queue design ensures the **One Worker per Subscription** invariant while handling high-frequency event bursts. Duplicate triggers are collapsed before execution.
- **Process-Level Isolation** — Each `getData()` call runs in a dedicated child subprocess with heartbeat monitoring, cancellation support, and timeout enforcement. A crash in one plugin never affects another.
- **Schema-Immutable Contracts** — Plugin `get_schema()` output is SHA-256 hashed and treated as an immutable contract. Breaking schema changes are detected, blocked, and reported via SMTP — existing subscriptions are never silently invalidated.
- **Live SSE Updates** — PostgreSQL `LISTEN`/`NOTIFY` bridges every state change to browser and MCP clients in real time. No polling required.
- **SMTP Notifications** — Configurable email alerts for heartbeat timeouts, runtime errors, schema validation failures, schema-breaking changes, watchdog force-releases, and aggregated duplicate-content blocks (one summary per target/KB per run).
- **Forward-Compatible Schema Evolution** — Three reserved `_extra_param_*` fields in every plugin schema allow future credential additions without requiring operators to recreate subscriptions.

---

## Architecture

AutoKB runs as six Docker containers orchestrated via `docker-compose.yml`. The Manager, Worker, and Web UI share a single unified base image for environment parity; the MCP Server has its own lighter image.
| Container | Role | Analogy |
|---|---|---|
| **Subscription Manager** (`autokb-manager`) | FastAPI app — plugin & sink registries, scheduling, REST API, SSE bridge, file watcher, watchdog | The Brain |
| **Worker** (`autokb-worker`) | Multiprocessing pool — executes plugin `getData()` in isolated child processes, runs sink reconciliation | The Muscle |
| **Web UI** (`autokb-web`) | aiohttp SPA with dark-mode dashboard, subscription management, Data Destinations / Targets, and Developer Lab | The Dashboard |
| **MCP Server** (`autokb-mcp`) | FastMCP ASGI app — exposes management tools to AI assistants via Model Context Protocol | The AI Proxy |
| **Redis** (`autokb-redis`) | Valkey-backed two-tier queue and distributed safety locks | The Message Bus |
| **PostgreSQL** (`autokb-db`) | Persistent storage for subscriptions, configs, event logs, and plugin registry state | The Memory |

### Data Flow

```
Trigger (cron / monitor / manual / MCP)
       │
       ▼
Manager ──► PostgreSQL (commit config + status)
       │
       └──► Redis P-Queue (push {subscription_id, operation: FULL|SINK_ONLY})
                │
                ▼
Worker pops from P-Queue ──► acquires Redis lock ──► drains both queues
       │
       ├── [DELETED]  ──► shutil.rmtree output dir ──► sink cleanup ──► delete DB row
       ├── [DISABLED/ERROR] ──► release lock, skip
       └── [ENABLED]  ──► set IN_PROGRESS ──► spawn child process
                │
                ▼
         Plugin.getData(config, progress_callback)
                │
                ├── progress_callback(pct) ──► update heartbeat + DB
                ├── SubscriptionCancelledError ──► exit(0) silently
                ├── Exception ──► serialize to exception file ──► exit(1)
                └── Success ──► move_to_destination() ──► exit(0)
                         │
                         ▼
                  /output/{plugin}/{subscription}/
                         │
                         ▼
            Sink reconciliation (after run, or SINK_ONLY op)
            compares /output/ vs DB ──► add/update/remove datafiles
                         │
                         ▼
         Data Target (remote KB / dataset, e.g. Open WebUI, RAGFlow, LightRAG)

PostgreSQL pg_notify ──► Manager asyncpg LISTEN ──► SSE clients (Web UI / MCP)
```

The Manager's watchdog monitors `last_heartbeat` timestamps at `AUTOKB_HEARTBEAT_TIMEOUT × 3` intervals, force-releasing stale locks and marking subscriptions as `ERROR` with full SMTP notification.

---

## Data Destinations (Sinks)

AutoKB can push plugin output beyond the local `/output/` directory into remote Knowledge Bases / datasets. The pieces:

- **Source plugin** (Data Source) — a plugin in `/src/plugins/` that fetches data and writes files via `move_to_destination()`.
- **Destination connector** (Sink) — a single Python file in `/src/sinks/` that knows how to talk to one remote service type (e.g. Open WebUI, RAGFlow, LightRAG). Sinks are hot-swapped into the running system like plugins.
- **Data Target** — a configured remote instance of a sink service (a URL, an API key, and a set of linked subscriptions). Targets are DB rows you manage from the Web UI.
- **Reconciliation** — after every full subscription run, and on demand via a `SINK_ONLY` queue operation, the Worker compares `/output/{plugin}/{subscription}/` against the database and the remote, calling the sink's add/update/remove methods to bring the remote in sync.

### The Sink Contract

A sink subclasses `BaseSink` (in `src/utils/sink_base.py`) and implements six remote-operation methods:

| Method | Trigger | Returns |
|---|---|---|
| `add_datafile(path)` | new file on disk | remote datafile ID |
| `update_datafile(remote_id, path, previous_hash=None, new_hash=None, datafile_id=None)` | content changed (hash diff) | new remote datafile ID |
| `remove_datafile(remote_id)` | file deleted on disk | — |
| `add_target()` | first recon for a target | remote target ID |
| `remove_target()` | Target explicitly deleted | — |
| `clear_target()` | contract-only (not yet invoked) | — |

The optional `previous_hash` / `new_hash` / `datafile_id` arguments let content-addressed sinks avoid deleting a new version's remote data and let shared-file sinks refcount remote ids. Sinks should accept these keyword arguments even if they ignore them.

Registration mirrors plugins: files matching `*Sink.py` in `/src/sinks/` are scanned by `SinkRegistry`, and the file stem must equal the sanitized `metadata["name"]`. Full authoring guide: [`assets/sink-development.md`](assets/sink-development.md).

### Built-in Sinks

| Sink | Service | Notes |
|---|---|---|
| **Open WebUI Knowledge Base** (`openWebUISink`) | Per-target KB `AutoKB_{target}` via the Open WebUI REST API | Uploads into Open WebUI's global file repo, links into the KB, reuses an existing owner on duplicate content, and refcounts shared remote files. |
| **RAGFlow Dataset** (`ragflowSink`) | Per-target dataset `AutoKB_{target}` via the RAGFlow REST API | Uses the General chunking method with the Docling parser; documents are re-created on content change because RAGFlow cannot replace them in place. |
| **LightRAG Knowledge Base** (`lightRagSink`) | Shared LightRAG instance via its REST API | Deterministic `autokb_{target}_` document names, page-bounded upload batches, background indexing, and target-scoped deletion from a shared KB. |

API keys for targets are encrypted at rest and decrypted only at reconciliation time; sink-specific defaults can be provided via the sink's `default_api_url` / `api_key_env_var` (see Environment Variables).

#### Duplicate-content handling

A sink may reject a document because its content already exists remotely. AutoKB treats that as a **per-document outcome**, not a target-wide failure:

- the link stays `ENABLED`;
- the document is recorded with state `DUPLICATE_BLOCKED` (`target_datafile.state` / `last_error` / `last_attempt`);
- an existing linked owner is reused when one can be found, avoiding delete/re-embed churn;
- unchanged blocked documents are skipped on later passes;
- pressing **Update** on the target clears blocked outcomes and retries them;
- one aggregated notification email is sent per target/KB per run when blocked documents are attempted.

Actual orphan-vector cleanup remains a remote-service operation (for example, resetting an Open WebUI knowledge base), followed by an AutoKB **Update**.

---

## Built-in Plugins

| Plugin | Type | Description |
|---|---|---|
| **Crawl4AI Web Scraper** (`crawl4AIWebScraperPlugin`) | SCHEDULED | Crawls websites with configurable depth/pages, converts to markdown, uses 3-way SHA-256 hash reconciliation |
| **FastCRW Web Scraper** (`fastCRWWebScraperPlugin`) | SCHEDULED | Crawls websites through the FastCRW API with configurable depth/pages, stores markdown one file per page, uses 3-way SHA-256 hash reconciliation |
| **Bible Scriptures** (`eBiblePlugin`) | SCHEDULED | Downloads Bible books/chapters in multiple versions with dynamic dropdowns populated via custom API routes |
| **Paperless Docling Parser** (`ePaperlessDoclingPlugin`) | SCHEDULED | Queries Paperless-ngx documents matching a filter, sends new and changed documents to Docling for OCR and parsing, and writes chunked markdown output (~490 tokens per chunk) |
| **IMAP Folder Watch** (`imapFolderWatchPlugin`) | EVENT_BASED | Watches IMAP folders via IDLE push notifications, downloads new emails, chunks long messages |
| **YouTube Transcriptions** (`youTubeTranscriptionPlugin`) | SCHEDULED | Downloads channel transcripts via YouTube API, chunks by token budget (~490 tokens), supports multiple languages |
| **HSHQ Help** (`hshqPlugin`) | SCHEDULED | Scans HSHQ Script-Server functions and extracts function names and descriptions to markdown |
| **Nextcloud / Twenty Contact Sync** (`nextcloudTwentyContactSyncPlugin`) | SCHEDULED | Reconciles contacts between Twenty CRM People and a Nextcloud Global address book, using AutoKB files as the source of truth and writing corrections back to both systems |

### Built-in Seeding and Upgrade Recovery

On startup, the built-in plugins and sinks baked into the image are copied from `/src/builtin_plugins/` and `/src/builtin_sinks/` into the live, host-mounted `/src/plugins/` and `/src/sinks/` directories **only when a same-name file does not already exist**.

That protects user customisations, but it has an important upgrade consequence:

> If a newer image fixes a built-in plugin or sink, the old live copy keeps being loaded until you remove it. The image never overwrites an existing file.

If a built-in is not behaving correctly after an image upgrade, and you have **not modified it**, the safe recovery is:

1. Inspect the live file against the image copy while the stack is running.
2. Stop the stack.
3. Back up the live plugin/sink directories.
4. Delete only the affected built-in file(s) from the host directory mounted at `/src/plugins/` or `/src/sinks/`.
5. Start the stack again. The new image re-seeds the missing built-in file from `/src/builtin_plugins/` or `/src/builtin_sinks/`.

```bash
# Inspect whether the live file differs from the image copy (while running).
docker compose exec autokb-manager \
  diff /src/sinks/ragflowSink.py /src/builtin_sinks/ragflowSink.py

# Stop writers.
docker compose stop autokb-manager autokb-worker

# Back up, then remove only the affected unmodified built-in.
cp -a <host-sinks-dir>/ragflowSink.py <host-sinks-dir>/ragflowSink.py.bak
rm <host-sinks-dir>/ragflowSink.py

docker compose up -d
```

Notes:

- These directories may also contain **custom** plugins/sinks. Do not delete the whole directory unless you have backed it up and confirmed every file is a stock built-in.
- If you **did** modify the built-in, do not delete it: back it up, compare it against the new image copy, and merge your changes manually.
- Removing a code file does not delete subscriptions, targets, target links, or other database state. The newly seeded built-in is picked up under the same name.
- A schema change in a re-seeded plugin still follows the normal schema-validation rules and may require creating a new subscription.

---

## Plugin Development

Plugins are single Python files subclassing `BaseSubscription`. The system handles process isolation, progress tracking, heartbeat monitoring, cancellation, output directory management, password encryption, config validation, and scheduling — plugin authors focus only on data fetching and transformation.

```python
from utils.plugin_base import BaseSubscription

class myPlugin(BaseSubscription):
    metadata = {
        "name": "myPlugin",
        "display_name": "My Plugin",
        "icon": "default_icon.png",
        "description": "Fetches data from an API.",
        "sub_type": "SCHEDULED",
    }
    DEFAULT_ACCESS_LEVEL = "PUBLIC"

    def get_schema(self):
        return {
            "type": "object",
            "properties": {
                "api_url": {"type": "string", "minLength": 1},
            },
            "required": ["api_url"],
        }

    def getData(self, config, progress_callback):
        progress_callback(0, message="Starting...")
        # fetch data, write to /tmp/, call self.move_to_destination()
        progress_callback(100, message="Done")
```

`display_name` (max 64 characters) is required and shown in the UI; `name` must sanitize to the file stem. Destination connectors follow the same pattern but subclass `BaseSink` and implement six remote-operation methods (see [Data Destinations (Sinks)](#data-destinations-sinks)).

Full documentation: [`assets/plugin-development.md`](assets/plugin-development.md) and [`assets/sink-development.md`](assets/sink-development.md)

---

## MCP Server

The MCP server (`autokb-mcp`) exposes AutoKB's management functionality as semantic tools for AI assistants:

- **Subscription Management** — Create, edit, delete, trigger, enable/disable, list subscriptions
- **Plugin Discovery** — List plugins, get details and schemas
- **System Health** — Check connectivity to Redis, PostgreSQL, and the Manager API
- **Plugin-Specific Shortcuts** — Specialized tools for creating Bible, YouTube, IMAP, and Crawl4AI subscriptions

The server uses Bearer token passthrough for identity, ensuring all AI actions respect user permissions. Bulk responses are compressed via TOON protocol.

Full documentation: [`mcp/README.md`](mcp/README.md)

---

## Getting Started

### Prerequisites

- Docker & Docker Compose

### Quick Start

1. Clone the repository:
   ```bash
   git clone <repo-url> autokb
   cd autokb
   ```

2. Configure environment variables in `stack.env` (or copy to `.env`):
   ```bash
   # Required credentials
   AUTOKB_ADMIN_USERNAME=admin
   AUTOKB_ADMIN_PASSWORD=<secure-password>
   AUTOKB_API_KEY=<secure-api-key>
   AUTOKB_BACKEND_API_KEY=<secure-backend-key>
   ENCRYPTION_KEY=<fernet-encryption-key>

   # Database (defaults work for local dev)
   POSTGRES_PASSWORD=<db-password>
   DATABASE_URL=postgresql://autokb:<password>@autokb-db:5432/autokb
   ```

3. Start the stack:
   ```bash
   docker compose up -d
   ```

4. Access the Web UI at `http://<host>:<web-port>`. The reference `docker-compose.yml` exposes services only on the internal Docker network, so either place it behind a reverse proxy or add a `ports:` mapping for `autokb-web` (container port `80`).

5. Log in with `AUTOKB_ADMIN_USERNAME` / `AUTOKB_ADMIN_PASSWORD`.

6. Navigate to **Data Sources** to see available plugins, then create a subscription.

### Isolated Development Stack

Never run the production `docker-compose.yml` from this repository when a
production `autokb` project already exists: Compose's default project name
would match it. Use the dedicated dev stack instead:

```bash
./scripts/dev-up.sh          # project autokb-dev, compose file docker-compose.dev.yml
./scripts/dev-down.sh        # keeps the dev DB volume
./scripts/dev-reset.sh       # DESTRUCTIVE: removes the dev DB volume too
```

The dev stack uses its own project (`autokb-dev`), containers
(`autokb-dev-*`), network (`autokb-dev-net`), DB volume (`autokb-dev-db`), and
localhost-only ports (`18080` web, `18083` Mailpit). It never starts services
on the production `autokb` network.

Bind-mount sources are absolute **host** paths rooted at
`AUTOKB_DEV_HOST_ROOT` (default
`/home/bolt/hshq/data/stacks/opencode/repos/autokb`, the host view of
`/repos/autokb` inside the opencode container). Override it in the shell or in
`stack.dev.env` if the repository moves.

Because the agent runs inside a container, host port mappings are only for a
human browser. Agent-side verification should run inside the dev containers,
or attach the agent container to the private dev network:

```bash
./scripts/dev-agent-connect.sh   # one-time per agent container restart

docker exec -e MANAGER_URL=http://autokb-dev-manager:80 \
  autokb-dev-manager python /src/testing/test_runner.py confirm --reset
docker exec autokb-dev-manager python /src/testing/unit_tests/run_all.py
```

`stack.dev.env` sets the test-runner timeouts to short values
(`AUTOKB_HEARTBEAT_TIMEOUT=5`, `AUTOKB_LOCK_TTL=60`, etc.) so the suite does not
wait for production-length timeouts. Dev SMTP points at Mailpit, so test
notifications never reach real recipients.

### MCP Client Connection

Connect to `http://<host>:<mcp-port>/mcp` with streamable HTTP transport, sending `Authorization: Bearer <AUTOKB_API_KEY>`. As with the Web UI, the reference compose does not publish the MCP port; add a `ports:` mapping or reach it through your reverse proxy.

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `MANAGER_PORT` | `80` | Manager FastAPI port |
| `WEBUI_PORT` | `80` | Web UI (aiohttp) port |
| `MCP_PORT` | `80` | MCP Server (uvicorn) port |
| `AUTOKB_ADMIN_USERNAME` | `admin` | Web UI login username |
| `AUTOKB_ADMIN_PASSWORD` | _(required)_ | Web UI login password |
| `AUTOKB_API_KEY` | _(required)_ | Bearer token for MCP → Web UI auth |
| `AUTOKB_BACKEND_API_KEY` | _(required)_ | Internal Web UI → Manager auth key |
| `AUTOKB_WEBHOOK_API_KEY` | — | Webhook/API authentication key used by the web layer |
| `AUTOKB_MANAGER_URL` | `http://autokb-manager:80` | Manager URL used by the Web UI proxy |
| `AUTOKB_BASE_URL` | — | Web URL used by the MCP container (compose sets `http://autokb-web:80`) |
| `AUTOKB_USE_HTTPS` | `0` | Set to `1` to mark session cookies `Secure` behind HTTPS |
| `AUTOKB_ASSETS_DIR` | `/assets` | Asset directory served by the Web UI |
| `DATABASE_URL` | `postgresql://autokb:autokb@autokb-db:5432/autokb` | PostgreSQL connection |
| `REDIS_URL` | `redis://autokb-redis:6379/0` | Redis connection |
| `POSTGRES_USER` / `POSTGRES_PASSWORD` / `POSTGRES_DB` | `autokb` / `autokb` / `autokb` | PostgreSQL container credentials/database |
| `ENCRYPTION_KEY` | _(required)_ | Fernet key **or passphrase** for password-field encryption; passphrases are derived with PBKDF2 |
| `WORKER_COUNT` | `4` | Number of parallel worker processes |
| `WORKER_STARTUP_DELAY_S` | `5` | Delay before worker initializes sink components (lets Manager run migrations) |
| `AUTOKB_HEARTBEAT_TIMEOUT` | `300` | Seconds before the watcher terminates an unresponsive plugin |
| `AUTOKB_MONITOR_ERROR_SLEEP` | `10` | Delay after a monitor exception before retrying |
| `AUTOKB_LOCK_TTL` | `3600` | Redis safety-lock TTL in seconds |
| `AUTOKB_DEBOUNCE_SECONDS` | `2` | Plugin/sink hot-swap debounce delay |
| `AUTOKB_DEBOUNCE_PHASE_SECONDS` | `5` | Sleep after a successful execution before re-evaluation |
| `AUTOKB_WATCHDOG_INTERVAL` | `5` | Manager watchdog poll interval |
| `AUTOKB_SSE_KEEPALIVE` | `30` | SSE keepalive comment interval |
| `AUTOKB_ORPHAN_SWEEP_SECONDS` | `30` | Worker orphan-process sweep interval |
| `SMTP_HOST` / `SMTP_PORT` / `SMTP_USER` / `SMTP_PASS` | — | SMTP server configuration |
| `SMTP_FROM` | `autokb@localhost` | Notification sender address |
| `SMTP_NOTIFY_EMAIL` | — | Notification recipient |
| `SMTP_USE_TLS` / `SMTP_USE_SSL` | `True` / `False` | SMTP transport security |
| `CRAWL4AI_API_URL` | `http://crawl4ai-app:11235` | Crawl4AI API base URL used by `crawl4AIWebScraperPlugin` |
| `FASTCRW_API_URL` | `http://fastcrw-app:3000` | FastCRW API base URL used by `fastCRWWebScraperPlugin` |
| `FASTCRW_API_KEY` | — | Default FastCRW bearer token; per-subscription `api_key` overrides it |
| `PAPERLESS_URL` | `http://paperless-app:8000` | Default Paperless-ngx URL for `ePaperlessDoclingPlugin` |
| `PAPERLESS_TOKEN` | — | Default Paperless-ngx API token |
| `DOCLING_URL` | `http://docling-app:5001` | Docling service URL used by ingestion plugins |
| `DOCLING_API_KEY` | — | Docling API key |
| `OPENWEBUI_API_KEY` | — | Default API key fallback for the Open WebUI sink |
| `RAGFLOW_API_KEY` | — | Default API key fallback for the RAGFlow sink |
| `LIGHTRAG_API_KEY` | — | Default API key fallback for the LightRAG sink |

Plugin and sink defaults are used to pre-fill subscription/target forms and as fallbacks when a per-subscription or per-target value is blank.

---

## Repository Structure

```
├── DesignSpecification.md       # Full system specification
├── docker-compose.yml           # Production/reference 6-container orchestration
├── docker-compose.dev.yml       # Isolated autokb-dev development stack
├── docker-compose.test.yml      # Isolated autokb-test CI/test stack
├── Dockerfile                   # Unified base image (Manager, Worker, Web UI)
├── requirements.txt             # Python dependencies
├── stack.env                    # Production/reference environment configuration
├── stack.dev.env                # Dev-only environment configuration
├── .dockerignore                # Keeps dev/runtime data out of image builds
├── assets/                      # Plugin/sink icons, brand assets, development guides
├── scripts/                     # dev-up/dev-down/dev-reset/dev-exec helpers
├── output/                      # Plugin output data (runtime mount; gitignored)
├── logs/                        # Per-container log files (runtime mount; gitignored)
├── mcp/                         # MCP server source and its own Docker image
└── src/
    ├── manager/                 # FastAPI app, scheduler, routes, registry, alembic migrations
    ├── worker/                  # Multiprocessing pool, execution engine, sink reconciliation
    ├── builtin_plugins/         # Built-in data sources (seeded into plugins/ at startup)
    ├── builtin_sinks/           # Built-in destinations (seeded into sinks/ at startup)
    ├── plugins/                 # Live data sources (host-mounted at runtime; seeded, never overwritten)
    ├── sinks/                   # Live destinations (host-mounted at runtime; seeded, never overwritten)
    ├── web/                     # aiohttp SPA with dark-mode UI
    ├── utils/                   # Shared components (DB, queue, crypto, schema, sink base/registry)
    └── testing/                 # Test runner, sinks/plugins fixtures, and unit_tests/
```

---

## Subscription and Target States

### Subscription states

| State | Description |
|---|---|
| `ENABLED` | Baseline active state — eligible for triggering |
| `ENQUEUED` | In a Redis queue awaiting worker pickup |
| `IN_PROGRESS` | Currently being executed by a worker |
| `ERROR` | Failed execution — requires manual re-enable |
| `DISABLED` | User-paused — not eligible for triggering |
| `DELETED` | Terminal state — pending output directory cleanup and row deletion |

### Target-link states (`target_subscriptions`)

| State | Description |
|---|---|
| `ENABLED` | Link is active and reconciled |
| `ENQUEUED` | A downstream sync is queued |
| `IN_PROGRESS` | Recon is currently running for this link |
| `ERROR` | Persistent sink failure; **Update** clears it and retries |
| `DISABLED` | Operator paused this link |
| `DELETED` | Link is being removed and cleaned up |

### Per-document sink states (`target_datafile`)

| State | Description |
|---|---|
| `SYNCED` | Remote copy exists and matches the tracked content |
| `DUPLICATE_BLOCKED` | Remote rejected the content as duplicate; the link stays `ENABLED`, repeat passes skip unchanged content, and **Update** retries |

---

## Testing

The project includes 26 test plugins and a test sink covering every major behavior path (happy path, heartbeat timeout, cancellation, schema-breaking changes, password encryption, custom routes, cron randomization, sink reconciliation, duplicate-content handling, etc.). The current suite runs 78 checks: 33 integration tests (26 plugin behavior + 7 sink/target flows), 44 sink unit tests (CRUD, queue encoding, registry, service wrappers, reconciliation), plus a leak check.

Run the suite from inside the Manager container (it talks to the Manager over HTTP):

```bash
docker exec -e MANAGER_URL=http://autokb-manager:80 \
  autokb-manager python /src/testing/test_runner.py confirm --reset
```

The `--reset` flag wipes all subscriptions, event logs, and output directories before running. Test subscriptions use an `akbtest-` / `e2e-` prefix so cleanup never touches real subscriptions. Test timing constants such as `AUTOKB_HEARTBEAT_TIMEOUT`, `AUTOKB_LOCK_TTL`, and `AUTOKB_DEBOUNCE_PHASE_SECONDS` can be overridden via environment variables for faster iteration.

For a quick standalone sink-pipeline smoke test (2 e2e + 4 unit checks):

```bash
python /src/testing/mini_dkb_test.py
```

---

## License

This project is provided for internal use. See license file for details.
