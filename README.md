<picture>
  <source media="(prefers-color-scheme: dark)" srcset="assets/autokb.png">
  <img alt="AutoKB Logo" src="assets/autokb.png" width="64" height="64">
</picture>

# AutoKB

**Distributed, Event-Driven ETL Orchestration Engine**

AutoKB connects to diverse data sources through a hot-swappable plugin architecture, pulls data on a schedule or in response to events, and writes structured output files for consumption by downstream Knowledge Base importers. It is designed as the ingestion layer for Retrieval-Augmented Generation (RAG) pipelines, semantic search indexes, and any system that needs a steady feed of chunked, pre-processed content.

---

## Key Features

- **Pluggable Architecture** — Data sources are single Python files. Drop a new plugin into `/src/plugins/` and it is hot-swapped into the running system within ~2 seconds. No restart, no config change, no deployment pipeline.
- **Dual Interface** — Full-featured dark-mode Web UI (SPA with SSE live updates) for human operators, plus an MCP (Model Context Protocol) server that lets AI assistants act as remote administrators.
- **Two-Tier Queue with Aggressive Collapsing** — Redis-backed P-Queue / S-Queue design ensures the **One Worker per Subscription** invariant while handling high-frequency event bursts. Duplicate triggers are collapsed before execution.
- **Process-Level Isolation** — Each `getData()` call runs in a dedicated child subprocess with heartbeat monitoring, cancellation support, and timeout enforcement. A crash in one plugin never affects another.
- **Schema-Immutable Contracts** — Plugin `get_schema()` output is SHA-256 hashed and treated as an immutable contract. Breaking schema changes are detected, blocked, and reported via SMTP — existing subscriptions are never silently invalidated.
- **Live SSE Updates** — PostgreSQL `LISTEN`/`NOTIFY` bridges every state change to browser and MCP clients in real time. No polling required.
- **SMTP Notifications** — Configurable email alerts for heartbeat timeouts, runtime errors, schema validation failures, schema-breaking changes, and watchdog force-releases.
- **Forward-Compatible Schema Evolution** — Three reserved `_extra_param_*` fields in every plugin schema allow future credential additions without requiring operators to recreate subscriptions.

---

## Architecture

AutoKB runs as six Docker containers orchestrated via `docker-compose.yml`. The Manager, Worker, and Web UI share a single unified base image for environment parity; the MCP Server has its own lighter image.

| Container | Role | Analogy |
|---|---|---|
| **Subscription Manager** (`autokb-manager`) | FastAPI app — plugin registry, scheduling, REST API, SSE bridge, file watcher, watchdog | The Brain |
| **Worker** (`autokb-worker`) | Multiprocessing pool — executes plugin `getData()` in isolated child processes | The Muscle |
| **Web UI** (`autokb-web`) | aiohttp SPA with dark-mode dashboard, subscription management, and Developer Lab | The Dashboard |
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
       └──► Redis P-Queue (push subscription_id)
                │
                ▼
Worker pops from P-Queue ──► acquires Redis lock ──► drains both queues
       │
       ├── [DELETED]  ──► shutil.rmtree output dir ──► delete DB row
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
    (consumed by downstream Knowledge Base importers)

PostgreSQL pg_notify ──► Manager asyncpg LISTEN ──► SSE clients (Web UI / MCP)
```

The Manager's watchdog monitors `last_heartbeat` timestamps at `HEARTBEAT_TIMEOUT × 3` intervals, force-releasing stale locks and marking subscriptions as `ERROR` with full SMTP notification.

---

## Built-in Plugins

| Plugin | Type | Description |
|---|---|---|
| **Crawl4AI Web Scraper** (`crawl4AIWebScraperPlugin`) | SCHEDULED | Crawls websites with configurable depth/pages, converts to markdown, uses 3-way SHA-256 hash reconciliation |
| **eBible** (`eBiblePlugin`) | SCHEDULED | Downloads Bible books/chapters in multiple versions with dynamic dropdowns populated via custom API routes |
| **ePaperlessDoclingPlugin** (`ePaperlessDoclingPlugin`) | SCHEDULED | Watches a Paperless-ngx storage path, sends new and changed documents to Docling for OCR and parsing, and writes chunked markdown output (~490 tokens per chunk). |
| **IMAP Folder Watch** (`imapFolderWatchPlugin`) | EVENT_BASED | Watches IMAP folders via IDLE push notifications, downloads new emails, chunks long messages |
| **YouTube Transcription** (`youTubeTranscriptionPlugin`) | SCHEDULED | Downloads channel transcripts via YouTube API, chunks by token budget (~490 tokens), supports multiple languages |

---

## Plugin Development

Plugins are single Python files subclassing `BaseSubscription`. The system handles process isolation, progress tracking, heartbeat monitoring, cancellation, output directory management, password encryption, config validation, and scheduling — plugin authors focus only on data fetching and transformation.

```python
from utils.plugin_base import BaseSubscription

class myPlugin(BaseSubscription):
    metadata = {
        "name": "myPlugin",
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

Full documentation: [`assets/plugin-development.md`](assets/plugin-development.md)

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

4. Access the Web UI at `http://<host>:<web-port>` (default: port 80).

5. Log in with `AUTOKB_ADMIN_USERNAME` / `AUTOKB_ADMIN_PASSWORD`.

6. Navigate to **Data Sources** to see available plugins, then create a subscription.

### MCP Client Connection

Connect to `http://<host>:<mcp-port>/mcp` with streamable HTTP transport, sending `Authorization: Bearer <AUTOKB_API_KEY>`.

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
| `AUTOKB_BACKEND_API_KEY` | _(auto-generated)_ | Internal Web UI → Manager auth |
| `DATABASE_URL` | `postgresql://autokb:autokb@autokb-db:5432/autokb` | PostgreSQL connection |
| `REDIS_URL` | `redis://autokb-redis:6379/0` | Redis connection |
| `ENCRYPTION_KEY` | _(required)_ | Fernet key for password field encryption |
| `WORKER_COUNT` | `4` | Number of parallel worker processes |
| `SMTP_HOST` / `SMTP_PORT` / `SMTP_USER` / `SMTP_PASS` / `SMTP_NOTIFY_EMAIL` | — | SMTP notification configuration |
| `HEARTBEAT_TIMEOUT` | `300` | Seconds before watcher terminates unresponsive plugin |

---

## Repository Structure

```
├── DesignSpecification.md       # Full system specification
├── docker-compose.yml           # 6-container orchestration
├── Dockerfile                   # Unified base image (Manager, Worker, Web UI)
├── requirements.txt             # Python dependencies
├── stack.env                    # Environment configuration
├── assets/                      # Plugin icons and brand assets
├── output/                      # Plugin output data (mounted volume)
├── logs/                        # Per-container log files (mounted volume)
├── src/
│   ├── manager/                 # FastAPI app, scheduler, routes, registry
│   ├── worker/                  # Multiprocessing pool, execution engine
│   ├── plugins/                 # Built-in and custom data source plugins
│   ├── web/                     # aiohttp SPA with dark-mode UI
│   ├── utils/                   # Shared components (DB, queue, crypto, schema)
│   └── testing/                 # Automated test suite + 25 test plugins
└── mcp/
    ├── Dockerfile               # MCP-specific image
    ├── pyproject.toml           # MCP dependencies
    └── src/
        ├── main.py              # FastMCP server, middleware, tool definitions
        └── client.py            # Authenticated API client
```

---

## Subscription States

| State | Description |
|---|---|
| `ENABLED` | Baseline active state — eligible for triggering |
| `ENQUEUED` | In a Redis queue awaiting worker pickup |
| `IN_PROGRESS` | Currently being executed by a worker |
| `ERROR` | Failed execution — requires manual re-enable |
| `DISABLED` | User-paused — not eligible for triggering |
| `DELETED` | Terminal state — pending output directory cleanup and row deletion |

---

## Testing

The project includes 25 test plugins covering every major behavior path (happy path, heartbeat timeout, cancellation, schema-breaking changes, password encryption, custom routes, cron randomization, etc.).

```bash
cd /repos/autokb
python /src/testing/test_runner.py confirm [--reset]
```

The `--reset` flag wipes all subscriptions, event logs, and output directories before running. Test constants like `HEARTBEAT_TIMEOUT` can be overridden via environment variables for faster iteration.

---

## License

This project is provided for internal use. See license file for details.
