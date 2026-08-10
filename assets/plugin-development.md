# AutoKB Plugin Development Guide

## 1. Overview

An AutoKB plugin is **a single Python file** subclassing `BaseSubscription` with a handful of implemented methods. It can be uploaded through the **Web UI Developer Lab** (`#/devlab`) or placed directly in `/src/plugins/`. Either way it is hot-swapped into the running system within ~2 seconds. No server restart, no config file, no registration step.

### Architecture at a Glance

```
Plugin file → file watcher (2s debounce) → PluginRegistry (validates + loads)
                                                        ↓
Worker picks up subscription → child process → plugin.getData(config, progress_callback)
```

- **Manager** runs the file watcher and API.
- **Worker** executes `getData()` in an isolated child process.
- **Web UI** provides a **Developer Lab** for uploading/testing/saving plugins with optional icon, and a form-driven subscription editor driven by your plugin's schema.
- **Output** lands in `/output/{sanitized_plugin_name}/{sanitized_subscription_name}/`.

### The Single-File Rule

**Everything** your plugin needs must live in one `.py` file. There are no sidecar files, no companion modules, no package directories. The file stem (minus `.py`) must be a valid **camelCase** identifier ≤ 32 characters.

If your plugin needs logic split across multiple concerns, write helper functions inside the same file. If it needs data files (icons, reference data), those go in `/assets/`. The icon filename is derived from the plugin name: `{sanitize_name(metadata["name"])}.png`. Upload or place a matching `.png` in `/assets/`; `default_icon.png` is used when absent.

### Zero Access Required

You do **not** need to read any AutoKB source code to create a plugin. Everything you need is in this document and the `BaseSubscription` class documented below. The system handles: process isolation, progress tracking, heartbeat monitoring, cancellation, output directory management, password encryption, config validation, and scheduling.

---

## 2. Plugin Skeleton

Every plugin starts with this minimal structure:

```python
from utils.plugin_base import BaseSubscription

class myFirstPlugin(BaseSubscription):
    metadata = {
        "name": "myFirstPlugin",
        "description": "Downloads data from an example source and writes it as markdown files.",
        "sub_type": "SCHEDULED",
    }
    DEFAULT_ACCESS_LEVEL = "PUBLIC"

    def get_schema(self):
        return {
            "type": "object",
            "properties": {
                "api_url": {
                    "type": "string",
                    "minLength": 1,
                    "description": "URL of the data source API",
                },
            },
            "required": ["api_url"],
        }

    def getData(self, config, progress_callback):
        progress_callback(0, message="Starting...")

        # 1. Extract config
        url = config["api_url"]

        # 2. Fetch data (example: write a file)
        tmp = "/tmp/myFirstPlugin_output.md"
        with open(tmp, "w") as f:
            f.write(f"Data from {url}\n")
            f.write("Hello from AutoKB!\n")

        # 3. Move to canonical location
        self.move_to_destination(tmp)

        progress_callback(100, message="Done")
```

That is a complete, working plugin. Save it as `myFirstPlugin.py` in `/src/plugins/` and it will appear in the Data Sources view within seconds.

---

## 3. Metadata Fields

The `metadata` dict is a **class-level** attribute. It is validated at load time.

| Field          | Type   | Required | Description |
|----------------|--------|----------|-------------|
| `name`         | str    | Yes      | camelCase identifier, ≤ 32 chars, must match the `.py` filename stem exactly (after `sanitize_name()`). Example: `"youTubeTranscriptionPlugin"` |
| `description`  | str    | No       | Shown in the Data Sources grid. Should explain what data source this connects to and any scheduling advice. |
| `sub_type`     | str    | Yes      | `"SCHEDULED"` or `"EVENT_BASED"` — see below. |

### Icon

The icon filename is derived from the plugin name: `{sanitize_name(metadata["name"])}.png`. Place a matching `.png` in `/assets/` or upload via the Developer Lab. Falls back to `default_icon.png` when missing.

### `sub_type`: SCHEDULED vs EVENT_BASED

#### SCHEDULED

The subscription runs on a **cron schedule**. The user provides a 5-field cron expression when creating the subscription (the system assigns a default, then randomizes minute/hour to avoid thundering herd).

Use SCHEDULED when:
- You want to poll an API periodically (e.g. YouTube channel, RSS feed, website scraper)
- Data changes on a predictable cadence
- The plugin has no concept of "events" or "push notifications"

**What to implement:** `get_schema()`, `getData()`.  
**No `monitor()` method needed.**

#### EVENT_BASED

The subscription runs when an **external event occurs**, detected by an async `monitor()` coroutine that runs continuously in the Manager's event loop. When `monitor()` returns `True`, a `getData()` run is enqueued.

Use EVENT_BASED when:
- You want to react to push notifications (e.g. IMAP IDLE, websocket, file system watcher)
- Polling would be wasteful or miss real-time changes
- The source provides some kind of event stream

**What to implement:** `get_schema()`, `getData()`, `monitor()`.  
**Crucial requirement:** `monitor()` is async and runs in the Manager's event loop. Use **asyncio-native libraries only** (e.g. `aioimaplib`, `aiofiles`, `asyncio` subprocess). Blocking calls will stall the entire Manager. If you cannot use an async library, put a SCHEDULED wrapper that polls instead.

### `DEFAULT_ACCESS_LEVEL`

A class-level string, `"PRIVATE"` or `"PUBLIC"`.

| Value     | When to use |
|-----------|-------------|
| `"PUBLIC"` | Data sources where the subscription config contains no secrets (e.g. Bible plugin, web scraper). The config is visible through the API. |
| `"PRIVATE"` | Data sources where the config contains credentials (e.g. IMAP plugin with password). The config is masked in API responses; password-format fields are encrypted at rest. |

---

## 4. `get_schema()` — The Configuration Contract

`get_schema()` returns a [JSON Schema](https://json-schema.org/) dict describing the subscription's configuration parameters. This schema drives the web UI form, validates user input, and is treated as an **immutable contract** — changes after subscriptions exist will be rejected as a "breaking change."

```python
def get_schema(self):
    return {
        "type": "object",
        "properties": {
            "username": {
                "type": "string",
                "minLength": 1,
                "description": "Your account username",
            },
            "password": {
                "type": "string",
                "minLength": 8,
                "format": "password",
                "description": "Your account password",
            },
            "server": {
                "type": "string",
                "default": "https://api.example.com",
                "description": "API base URL",
            },
            "max_items": {
                "type": "integer",
                "minimum": 1,
                "maximum": 1000,
                "default": 50,
                "description": "Maximum items to fetch per run",
            },
            "include_metadata": {
                "type": "boolean",
                "default": True,
                "description": "Include metadata in output",
            },
        },
        "required": ["username", "password"],
    }
```

### Supported Field Constraints

| Constraint     | Applies to   | Behavior |
|----------------|--------------|----------|
| `type`         | all          | `"string"`, `"integer"`, `"boolean"` |
| `minLength`    | string       | Minimum character length |
| `maxLength`    | string       | Maximum character length |
| `pattern`      | string       | Regex pattern the value must match |
| `format`       | string       | Only `"password"` is special — see below |
| `minimum`      | integer      | Minimum value (inclusive) |
| `maximum`      | integer      | Maximum value (inclusive) |
| `enum`         | string       | Fixed set of allowed values (rendered as dropdown) |
| `default`      | any          | Default value if user leaves the field empty |
| `x-enum-source`| string       | See "Dynamic Dropdowns" below |

### Password Fields (`format: "password"`)

Properties with `"format": "password"` are:
- **Encrypted at rest** using Fernet (AES) with the system's `ENCRYPTION_KEY`.
- **Masked in API responses** — the API returns `"********"` instead of the real value.
- **Preserved on edit** — if the user submits an empty password on edit, the existing encrypted value is kept.
- **Passed decrypted to `getData()`** — your plugin receives the plaintext.

Use this for API keys, passwords, tokens, or any sensitive credential.

### Dynamic Dropdowns (`x-enum-source`)

Instead of a static `enum`, you can populate a dropdown from a custom API route:

```python
"version": {
    "type": "string",
    "default": "eng_kjv",
    "x-enum-source": "/api/plugins/eBiblePlugin/versions",
}
```

The web UI fetches this endpoint and uses the response to build an `<option>` list. The endpoint is served by your plugin's `get_custom_routes()` (see Section 9).

### Auto-Injected Fields

The system automatically appends three extra string fields to every schema:

- `_extra_param_1`
- `_extra_param_2`
- `_extra_param_3`

These are reserved for forward-compatible schema migration. Your plugin can ignore them; they will always be present in the config dict with a default value of `""`.

### Schema Immutability

Once a subscription exists, **the schema hash must not change**. The system stores a SHA-256 hash of the augmented schema. On reload (file change), if the hash differs from the stored value, the plugin is **refused** and all existing subscriptions are **disabled** with an error message about a breaking schema change.

---

## 5. `getData()` — The Core Logic

```python
def getData(self, config: dict, progress_callback: Callable[[int, Optional[str]], None]) -> None:
```

This is the heart of your plugin. It runs in a **child subprocess** (isolated from the Manager and Worker). It receives:

### `config: dict`

A dictionary of subscription configuration values. The keys match the properties defined in `get_schema()`. Password-format fields are **already decrypted** by the time they reach your plugin.

Example for the schema in Section 4:
```python
config = {
    "username": "alice",
    "password": "hunter2",            # already decrypted
    "server": "https://api.example.com",
    "max_items": 50,
    "include_metadata": True,
    "_extra_param_1": "",              # auto-injected
    "_extra_param_2": "",
    "_extra_param_3": "",
}
```

### `progress_callback(pct: int, message: str = None) -> None`

You **must** call this periodically. Requirements:

- Call at minimum every 300 seconds (the heartbeat timeout) — but ideally every 5-30 seconds.
- Pass an integer from 0 to 100 indicating completion percentage.
- Optionally pass a `message` string that will be stored in the subscription's
  `last_message` field. This is the **primary way to surface plugin-specific
  results to the user** — use it to summarize what happened (e.g.
  "12 articles added, 3 removed"). The message is visible in the Web UI
  and should be a human-readable summary of the run. Omit `message` on
  high-frequency heartbeat calls (every 5-30s) to avoid unnecessary churn;
  reserve it for meaningful milestones.
- **Do NOT catch `SubscriptionCancelledError`** — this exception is raised by the callback when the subscription is disabled or deleted mid-execution. Let it propagate; the execution engine will handle it cleanly (exit code 0, no error notification).

Call progression pattern (from the crawl4AI plugin):
```python
progress_callback(0, message="Starting crawl...")
# ... some work ...
progress_callback(10, message="API returned 42 pages")
# ... more work ...
progress_callback(100, message="Done: 42 pages processed, 3 skipped, 5 removed")
```

### Writing Output

Your plugin produces **files**. Write them to a temp location (typically `/tmp/`), then call `self.move_to_destination()`:

**⚠️ Output Directory Restriction**

The output directory is consumed directly by downstream Knowledge Base import pipelines. **Do not place metadata files, tracking files, checksum manifests, state files, or any other non-data files** in `/output/`. Every file in the output directory is treated as importable content. If you need to track state across runs, encode it in filenames (e.g. embedding a checksum hash) or compute it from the source during reconciliation.

```python
tmp = "/tmp/my_output.txt"
with open(tmp, "w") as f:
    f.write("content here")

self.move_to_destination(tmp)  # moves to /output/{plugin}/{sub}/{filename}
```

#### `self.move_to_destination(temp_file_path: str) -> str`

- Moves a single file (or directory) to the canonical output location.
- Target path: `/output/{sanitized_plugin_name}/{sanitized_subscription_name}/{sanitized_filename}`.
- If the source is a directory, it is copied recursively then the source is removed.
- Returns the target path.
- All path components are sanitized — only `[a-zA-Z0-9.-]` characters survive.

#### `self.get_destination_path() -> str`

- Returns the output directory path: `/output/{sanitized_plugin_name}/{sanitized_subscription_name}/`.
- Does **not** create the directory or write anything.
- Used primarily for **reconciliation** — checking which files already exist on disk.

### Complete Lifecycle Contract

1. The execution engine calls `progress_callback(0)` before entering your method.
2. Your `getData()` runs.
3. The execution engine calls `progress_callback(100)` after your method returns.
4. If `SubscriptionCancelledError` is raised at any point (including step 1 or 3), the execution exits cleanly with code 0.
5. If any other exception propagates out of `getData()`, the execution is recorded as an error (exit code 1), the subscription is set to ERROR state, and an EventLog entry is created.
6. On success (exit code 0), the subscription is set back to ENABLED state.

---

## 6. Reconciliation — File Management Strategy

Reconciliation ensures that the output directory contains exactly the right files: nothing missing, nothing stale, nothing outdated. This is **your responsibility** — there is no base class method for it. Each plugin implements its own strategy.

Three patterns exist in the codebase. Choose the one that fits your data source.

**⚠️ No Auxiliary Files in Output Directory**

The output directory is read directly by downstream Knowledge Base importers — every file present is treated as content to ingest. You **must not** place:

- Checksum or hash manifest files
- Timestamp or cursor tracking files
- JSON/CSV state files
- Any file that is not actual content for downstream consumption

Persist state across runs using the subscription's `config` dict (via schema fields) or encode tracking data in filenames (e.g. `{id}.{checksum_prefix}.md`).

### Pattern A: Existence Check (Simplest)

**Used by:** `youTubeTranscriptionPlugin`, `eBiblePlugin`

Skip items that already have a file on disk. No content comparison, no deletion of stale files.

```python
output_dir = self.get_destination_path()
completed = set()

if os.path.isdir(output_dir):
    for fname in os.listdir(output_dir):
        if not fname.endswith(".txt"):
            continue
        # Extract a stable identifier from the filename
        item_id = _parse_id_from_filename(fname)
        if item_id:
            completed.add(item_id)

for item in fetched_items:
    if item["id"] in completed:
        continue   # already processed
    # ... process item, write file, move_to_destination() ...
```

**Use when:** Items are never removed from the source (append-only), you don't need to detect content changes, and stale files are acceptable until the output directory is cleaned.

### Pattern B: 3-Way Hash Diff (Most Correct)

**Used by:** `crawl4AIWebScraperPlugin`

Compare SHA-256 hashes of file content between what the API returned and what exists on disk. Four sets emerge:

```python
import hashlib

def _file_hash(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        h.update(f.read())
    return h.hexdigest()

output_dir = self.get_destination_path()

# 1. Build index of what the API returned
api_index = {}   # stable_id -> {"tmp_path": ..., "content_hash": ...}
for result in api_results:
    stable_id = _stable_id(result)
    tmp = f"/tmp/{stable_id}.md"
    with open(tmp, "w") as f:
        f.write(_format_output(result))
    content_hash = _file_hash(tmp)
    api_index[stable_id] = {"tmp_path": tmp, "content_hash": content_hash}

# 2. Build index of what's on disk
disk_index = {}  # stem -> content_hash
if os.path.isdir(output_dir):
    for fname in os.listdir(output_dir):
        stem = fname.rsplit(".", 1)[0]
        fpath = os.path.join(output_dir, fname)
        try:
            disk_index[stem] = _file_hash(fpath)
        except OSError:
            pass

# 3. Three-way set comparison
api_keys = set(api_index.keys())
disk_keys = set(disk_index.keys())

to_add = api_keys - disk_keys               # new — needs writing
to_update = {k for k in api_keys & disk_keys
             if api_index[k]["content_hash"] != disk_index[k]}  # changed — needs rewriting
to_delete = disk_keys - api_keys            # removed from source — needs removal
to_skip = {k for k in api_keys & disk_keys
           if api_index[k]["content_hash"] == disk_index[k]}    # unchanged — skip

# 4. Process: add + update
for stable_id in to_add | to_update:
    tmp = api_index[stable_id]["tmp_path"]
    self.move_to_destination(tmp)
    # Done: os.remove(tmp) is called by move_to_destination

# 5. Process: delete stale files
for stable_id in to_delete:
    for fname in os.listdir(output_dir):
        if fname.startswith(stable_id):
            os.remove(os.path.join(output_dir, fname))

# 6. Clean up temp files for skipped items (they weren't moved)
for stable_id in to_skip:
    os.remove(api_index[stable_id]["tmp_path"])

# 7. Stray file removal — delete anything in output dir not in api_index
for fname in os.listdir(output_dir):
    stem = fname.rsplit(".", 1)[0]
    if stem not in api_index:
        os.remove(os.path.join(output_dir, fname))
```

**Use when:** Content can change at the source, items can be removed, and you need the output directory to be an exact reflection of the source. This is the most robust pattern.

### Pattern C: Set Comparison with External Source

**Used by:** `imapFolderWatchPlugin`

Compare identifiers (UIDs, IDs, primary keys) from the external source against filenames on disk:

```python
output_dir = self.get_destination_path()

# 1. Get all identifiers from the source
source_ids = set(fetch_all_ids_from_source())   # e.g. IMAP UIDs

# 2. Get identifiers from disk
disk_ids = set()
if os.path.isdir(output_dir):
    for fname in os.listdir(output_dir):
        match = re.match(r"^(?P<id>\d+)\.\d+\.txt$", fname)
        if match:
            disk_ids.add(int(match.group("id")))

# 3. Diff
to_add = source_ids - disk_ids
to_remove = disk_ids - source_ids

# 4. Process adds
for item_id in to_add:
    content = fetch_item(item_id)
    for i, chunk in enumerate(chunk_content(content)):
        tmp = f"/tmp/{item_id}.{i}.txt"
        with open(tmp, "w") as f:
            f.write(chunk)
        self.move_to_destination(tmp)

# 5. Process removals
for item_id in to_remove:
    for fname in os.listdir(output_dir):
        if fname.startswith(f"{item_id}."):
            os.remove(os.path.join(output_dir, fname))
```

**Use when:** The source provides a stable, unique identifier for each item, items can be added and removed, and content doesn't change after creation (e.g. emails, immutable database records).

### Stray File Cleanup

Regardless of which pattern you choose, consider a final pass that removes any file in the output directory that doesn't correspond to any known source item. This prevents orphaned files from accumulating:

```python
if os.path.isdir(output_dir):
    for fname in os.listdir(output_dir):
        stem = fname.rsplit(".", 1)[0]
        if stem not in api_index:
            os.remove(os.path.join(output_dir, fname))
```

---

## 7. Chunking Strategies

When a single source item (e.g. a YouTube video transcript, a Bible chapter, a long email) is too large to fit in one output file, you **chunk** it into multiple files. The system has no opinion on chunk size, but all existing plugins target **~490 tokens per chunk** (roughly 350 words).

### Make Chunking Optional

**If your plugin chunks, you MUST expose a `chunking_enabled` boolean field in your schema, defaulting to `True`, so each user can turn chunking off.**

Chunking is a convenience for downstream import pipelines, but not every consumer wants it. Some prefer a single whole document per source item — for example, a downstream RAG or embedding pipeline that does its own splitting, or a user who wants the raw email body or full transcript kept intact. Forcing chunking with no way to disable it makes the plugin unusable for those cases.

Add the field to `get_schema()`:

```python
"chunking_enabled": {
    "type": "boolean",
    "default": True,
    "description": "Chunk by token budget (~490 tokens per file). Disable to write the full item as a single document.",
},
```

Then honor it in `getData()` by gating the chunking branch:

```python
chunking_enabled = config.get("chunking_enabled", True)

if chunking_enabled and total_tokens > MAX_CHUNK_TOKENS:
    # ... split into multiple chunk files ...
else:
    # ... write the full item as a single file ...
```

When chunking is disabled, write **one file per source item** containing the entire content, with the same stable, deterministic filename the chunked output would use — so reconciliation keeps working regardless of the setting.

All three strategies below produce files named with a chunk index, such as:
- `{video_id}-000.txt`, `{video_id}-001.txt`, ...
- `{book}-{chapter}-{v1}-{v2}.txt`
- `{uid}.1.txt`, `{uid}.2.txt`, ...

### Strategy 1: No Chunking

One source item = one file. Simplest approach, used by `crawl4AIWebScraperPlugin`.

```python
tmp = f"/tmp/{stable_id}.md"
with open(tmp, "w") as f:
    f.write(content)
self.move_to_destination(tmp)
```

**When to use:** Each source item naturally fits in one file (e.g. a web page's markdown, a short API response).

### Strategy 2: RecursiveCharacterTextSplitter (LangChain)

**Used by:** `youTubeTranscriptionPlugin`, `imapFolderWatchPlugin`

Splits text recursively on paragraph breaks, line breaks, sentence boundaries, and finally individual characters until each chunk fits the target size.

```python
import tiktoken
from langchain_text_splitters import RecursiveCharacterTextSplitter

enc = tiktoken.get_encoding("cl100k_base")

text_splitter = RecursiveCharacterTextSplitter(
    chunk_size=440,          # target tokens per chunk
    chunk_overlap=0,
    length_function=lambda t: len(enc.encode(t)),
    separators=["\n\n", "\n", ". ", " ", ""],
)

text = "..."  # the full text to chunk
chunks = text_splitter.split_text(text)

for i, chunk_text in enumerate(chunks):
    # Optionally prepend metadata
    content = f"Header: metadata\n\n{chunk_text}"
    tmp = f"/tmp/{stable_id}-{i:03d}.txt"
    with open(tmp, "w") as f:
        f.write(content)
    self.move_to_destination(tmp)
```

**When to use:** Natural language text (transcripts, articles, email bodies). The recursive strategy produces semantically coherent chunks.

### Strategy 3: Custom Mathematical Distribution

**Used by:** `eBiblePlugin`

When items are discrete units (verses, rows, records) and you want to distribute them evenly across chunks:

```python
import math
import tiktoken

MAX_CHUNK_TOKENS = 490
EFFECTIVE_TARGET = 440      # MAX_CHUNK_TOKENS - metadata overhead buffer

enc = tiktoken.get_encoding("cl100k_base")

def chunk_items(items, total_tokens):
    """Distribute items evenly into chunks respecting MAX_CHUNK_TOKENS."""
    chunk_count = max(1, math.floor(total_tokens / EFFECTIVE_TARGET))

    while True:
        threshold = total_tokens / chunk_count
        chunks = distribute_evenly(items, threshold)
        # Invariant: each chunk + metadata overhead <= MAX_CHUNK_TOKENS
        metadata_overhead = compute_metadata_overhead()
        ok = all(
            sum(len(enc.encode(item)) for item in chunk) + metadata_overhead <= MAX_CHUNK_TOKENS
            for chunk in chunks
        )
        if ok:
            return chunks
        chunk_count += 1

def distribute_evenly(items, threshold):
    """Group items into chunks where each chunk stays near the threshold."""
    chunks = []
    current_chunk = []
    current_tokens = 0
    for item in items:
        item_tokens = len(enc.encode(item))
        if current_tokens + item_tokens > threshold and current_chunk:
            chunks.append(current_chunk)
            current_chunk = []
            current_tokens = 0
        current_chunk.append(item)
        current_tokens += item_tokens
    if current_chunk:
        chunks.append(current_chunk)
    return chunks
```

**When to use:** Structured records (verses, rows, items) where you want semantically complete chunks rather than mid-sentence splits.

### Token Budgeting

| Constant           | Value | Used by |
|--------------------|-------|---------|
| `MAX_CHUNK_TOKENS` | 490   | YouTube, eBible |
| `TOKEN_BUDGET`     | 480   | IMAP |
| `EFFECTIVE_TARGET` | 440   | YouTube, eBible |
| `SAFETY_FLOOR`     | 50    | IMAP |

The `cl100k_base` encoding is pre-cached in the Docker image and always available. Use `tiktoken.get_encoding("cl100k_base")` — there is no download delay.

---

## 8. `monitor()` for EVENT_BASED Plugins

If your plugin has `sub_type = "EVENT_BASED"`, you must implement `monitor()`:

```python
async def monitor(self, config: dict, cancel_token: Any) -> bool:
    """Return True to enqueue a getData() run, False to keep waiting."""
```

### Contract

- Runs as an asyncio task in the Manager's event loop.
- Called repeatedly in a loop. Between iterations the scheduler inserts
  a brief cancellable pause (2s) as a safety throttle — this prevents
  tight-looping when `monitor()` returns quickly and ensures cancellation
  requests are promptly honored. For long-blocking implementations
  (e.g. IMAP IDLE) this delay is negligible; the function blocks until
  an event occurs.
- `config` is the subscription's configuration dict (decrypted).
- `cancel_token` is an awaitable that completes when the subscription is being cancelled.
- Return `True` to signal: "something happened, enqueue a run."
- Return `False` to signal: "nothing happened, keep waiting."

### Async Libraries Only

Because `monitor()` runs in the Manager's event loop, you **must** use asyncio-native libraries. Blocking calls (e.g. `time.sleep()`, `requests.get()`, `imaplib`) will stall the entire Manager.

| ✅ Use                     | ❌ Avoid          |
|----------------------------|-------------------|
| `asyncio.sleep()`          | `time.sleep()`    |
| `httpx.AsyncClient`        | `requests`        |
| `aioimaplib`               | `imaplib`         |
| `aiofiles`                 | built-in `open()` in tight loops |
| `asyncio.open_connection`  | `socket` blocking |

### Patterns

#### Pattern A: Long-lived push connection (IMAP IDLE)

```python
async def monitor(self, config, cancel_token):
    self._apply_config(config)
    try:
        mail = await self._connect()
        while True:
            result = await self._idle_once(mail, cancel_token)
            if result:  # push notification received
                return True
    except ConnectionError:
        self.log.warning("imap_connection_error")
        await asyncio.sleep(10)
        return False
```

#### Pattern B: Polling with cancellation

```python
async def monitor(self, config, cancel_token):
    for i in range(3):
        if await self._check_for_updates():
            return True
        await asyncio.sleep(5)
    return False
```

### Cron Fallback

Even for EVENT_BASED plugins, the system also checks the subscription's cron expression as a fallback. If no event fires but the cron is due, the subscription will still be enqueued. This provides a safety net for missed events.

---

## 9. Custom API Routes

Your plugin can expose custom REST endpoints under `/api/plugins/{plugin_id}/...` by implementing `get_custom_routes()`:

```python
from utils.plugin_base import BaseSubscription, PluginRoute

def get_custom_routes(self):
    return [
        PluginRoute(
            path="/versions",
            method="GET",
            handler=self._get_versions,
        ),
    ]

async def _get_versions(self, request):
    """Handler receives a FastAPI Request, returns JSON-serializable data."""
    return {"versions": [...]}
```

### `PluginRoute` Fields

| Field     | Type                    | Description |
|-----------|-------------------------|-------------|
| `path`    | str                     | URL path under `/api/plugins/{plugin_id}` (e.g. `/versions`) |
| `method`  | str                     | HTTP method (e.g. `"GET"`, `"POST"`) |
| `handler` | `Callable[..., Any]`    | Async or sync function. Receives a FastAPI `Request` object. Must return JSON-serializable data. |

### Use Cases

- **Dynamic dropdown data** (eBible: `/versions` returns available Bible versions)
- **Status checks** (test: `/status` returns `{"ok": True}`)
- **Proxy endpoints** that translate or aggregate data from the external source

---

## 10. File Naming & Output Structure

### Recommended Naming Convention

All existing plugins follow this general pattern:

```
{stable_identifier}{separator}{sequence_number}.{extension}
```

| Plugin | Pattern | Example |
|--------|---------|---------|
| Crawl4AI | `{url_hash_16}.md` | `a1b2c3d4e5f6g7h8.md` |
| YouTube | `{video_id}-{chunk:03d}.txt` | `abc123xyz-000.txt` |
| eBible | `{book}-{chapter}-{v_start}-{v_end}.txt` | `genesis-1-1-31.txt` |
| IMAP | `{uid}.{chunk}.txt` | `42.1.txt` |

The extension communicates file type to downstream consumers:

| Extension | When to use |
|-----------|-------------|
| `.md`     | Rich text / markdown content (web pages, rendered documents) |
| `.txt`    | Plain text (transcripts, email bodies, scripture) |
| `.json`   | Structured data |
| `.csv`    | Tabular data |

### Output Directory Structure

```
/output/
  {sanitized_plugin_name}/
    {sanitized_subscription_name}/
      file1.md
      file2.md
      ...
```

`sanitize_name()` strips all characters except `[a-zA-Z0-9.-]`, removes leading/trailing periods, and collapses consecutive periods.

Only data files intended for downstream import. No metadata, manifest, checksum, or tracking files of any kind.

### Choosing a Stable Identifier

The filename should be **deterministic** — given the same source item, the same filename is always produced. This is what makes reconciliation possible.

- **URL hash** for web pages (deterministic: same URL → same hash)
- **Video ID** for YouTube (immutable)
- **Book + chapter** for Bible (stable over time)
- **IMAP UID** for email (assigned by server, never changes for that email)

Avoid using database auto-increment IDs or timestamps as the sole identifier — they are not stable across runs.

---

## 11. Error Handling

### Do NOT Catch SubscriptionCancelledError

`SubscriptionCancelledError` is raised by `progress_callback()` when the subscription is disabled or deleted mid-execution. Let it propagate untouched:

```python
def getData(self, config, progress_callback):
    try:
        for item in items:
            progress_callback(pct)  # may raise SubscriptionCancelledError
            process(item)
    except SubscriptionCancelledError:
        raise   # let it propagate
    except Exception as e:
        self.log.error("item_failed", error=str(e))
        # continue processing remaining items
```

The execution engine catches it at the top level and exits cleanly (exit code 0, no error notification).

### Per-Item Error Resilience

When processing multiple items, catch per-item exceptions and continue:

```python
processed = 0
skipped = 0
for item in items:
    try:
        result = fetch_and_process(item)
        processed += 1
    except SpecificError as e:
        self.log.warning("item_skipped", item_id=item["id"], error=str(e))
        skipped += 1
    except SubscriptionCancelledError:
        raise  # never swallow this
```

### Structured Logging

A logger is available at `self.log` with name `plugin.{ClassName}`. Use structured keyword arguments:

```python
self.log.info("fetch_started", url=url, max_pages=max_pages)
self.log.info("fetch_completed", pages=len(results))
self.log.error("fetch_failed", error=str(exc))
```

Logs are written to `/logs/web.log` with ISO timestamps and structured fields.

---

## 12. Dependencies

### What is Already Installed

The unified Docker image has all of these pre-installed (from `requirements.txt`):

| Category | Packages | Purpose |
|----------|----------|---------|
| **Web / API** | `fastapi`, `uvicorn`, `gunicorn`, `aiohttp`, `httpx`, `requests`, `websockets`, `aiodns` | HTTP servers, clients, async networking |
| **Database** | `sqlalchemy`, `asyncpg`, `psycopg2-binary`, `alembic`, `redis` | ORM, drivers, queue |
| **Validation / Config** | `pydantic`, `pydantic-settings`, `PyYAML`, `orjson`, `msgpack`, `tomli`, `python-multipart` | Schema validation, serialization |
| **Encryption** | `cryptography` | Fernet password encryption |
| **Parsing / Feeds** | `beautifulsoup4`, `feedparser`, `dateparser`, `charset-normalizer`, `markdown` | HTML, RSS, dates, encoding |
| **Documents** | `pypdf`, `pdfplumber`, `python-docx`, `openpyxl`, `odfpy` | PDF, Word, Excel, ODF |
| **Data Analysis** | `pandas`, `numpy`, `scipy` | ETL, statistics |
| **Media** | `Pillow`, `mutagen`, `pydub` | Images, audio metadata, audio processing |
| **Tokenization** | `tiktoken==0.13.0`, `langchain`, `langchain-text-splitters` | Token counting, text splitting |
| **Communications** | `imapclient`, `aioimaplib>=1.0.1`, `paramiko`, `asyncssh` | Email, SSH/SFTP |
| **File System** | `watchdog`, `aiofiles`, `rarfile`, `py7zr` | FS events, async I/O, archives |
| **Logging / CLI** | `structlog`, `rich`, `psutil`, `click` | Logging, terminal output, system info |
| **IDs** | `uuid7` | UUIDv7 generation |
| **YouTube** | `youtube-transcript-api`, `yt-dlp` | Transcripts, metadata extraction |

### Adding New Dependencies

Most common plugin needs are already covered by the pre-installed packages above. If your plugin requires a library not in the list:

1. **Note the exact library and version** you need in your plugin's docstring or comments.
2. **Convey this to the person or team** maintaining the Docker image.
3. The dependency is added to `/repos/autokb/requirements.txt`.
4. The unified Docker image is rebuilt.

There is no per-plugin dependency mechanism. All plugins share the same runtime image.

---

## 13. Validation at Load Time

When the file watcher detects a new or changed plugin file, the system validates:

| Check | Failure behavior |
|-------|------------------|
| File is valid Python | Plugin is not loaded; error logged |
| Exactly one `BaseSubscription` subclass exists | Plugin is not loaded |
| `metadata["name"]` exists and matches filename stem (after sanitization) | Plugin is not loaded |
| `sub_type` is `"SCHEDULED"` or `"EVENT_BASED"` | Plugin is not loaded |
| `DEFAULT_ACCESS_LEVEL` is `"PRIVATE"` or `"PUBLIC"` | Plugin is not loaded |
| `get_schema()` returns a dict | Plugin is not loaded |
| Schema hash has not changed since last load (if subscriptions exist) | Plugin refused, existing subscriptions disabled with "breaking change" error |

---

## 14. Full Walkthrough: RSS Feed Plugin

Let's create a complete plugin from scratch that fetches an RSS feed and writes each article as a markdown file, with proper reconciliation and chunking.

```python
import hashlib
import os
import re
import time

import feedparser
import requests
import tiktoken
from langchain_text_splitters import RecursiveCharacterTextSplitter

from utils.plugin_base import BaseSubscription


class rssFeedPlugin(BaseSubscription):
    metadata = {
        "name": "rssFeedPlugin",
        "description": (
            "Fetches articles from an RSS/Atom feed and writes each as "
            "markdown. Long articles are chunked by token budget."
        ),
        "sub_type": "SCHEDULED",
    }
    DEFAULT_ACCESS_LEVEL = "PUBLIC"
    MAX_CHUNK_TOKENS = 490

    def get_schema(self):
        return {
            "type": "object",
            "properties": {
                "feed_url": {
                    "type": "string",
                    "minLength": 1,
                    "description": "URL of the RSS/Atom feed (e.g. https://example.com/feed.xml)",
                },
                "max_articles": {
                    "type": "integer",
                    "default": 0,
                    "minimum": 0,
                    "description": "Max articles to process per run (0 = all)",
                },
                "chunking_enabled": {
                    "type": "boolean",
                    "default": True,
                    "description": "Chunk by token budget (~490 tokens per file). Disable to write the full article as a single document.",
                },
            },
            "required": ["feed_url"],
        }

    # ------------------------------------------------------------------
    # getData
    # ------------------------------------------------------------------
    def getData(self, config, progress_callback):
        progress_callback(0, message="Starting...")

        feed_url = config["feed_url"]
        max_articles = config.get("max_articles", 0)
        chunking_enabled = config.get("chunking_enabled", True)

        # 1. Fetch and parse feed
        progress_callback(5, message="Fetching feed...")
        feed = feedparser.parse(feed_url)
        entries = feed.entries
        if max_articles > 0:
            entries = entries[:max_articles]

        if not entries:
            progress_callback(100, message="No articles found")
            return

        # 2. Build API index from feed entries
        api_index = {}
        enc = tiktoken.get_encoding("cl100k_base")
        splitter = RecursiveCharacterTextSplitter(
            chunk_size=440,
            chunk_overlap=0,
            length_function=lambda t: len(enc.encode(t)),
            separators=["\n\n", "\n", ". ", " ", ""],
        )

        for article in entries:
            article_id = self._stable_id(article)
            title = getattr(article, "title", "Untitled")
            link = getattr(article, "link", "")
            published = getattr(article, "published", "unknown date")
            content = self._get_content(article)

            # Build markdown
            markdown = f"# {title}\n\n"
            markdown += f"**Source:** [{link}]({link})\n\n"
            markdown += f"**Published:** {published}\n\n"
            markdown += "---\n\n"
            markdown += content

            # Chunk if needed (respects chunking_enabled so users can
            # opt for a single whole document per article)
            total_tokens = len(enc.encode(markdown))
            if chunking_enabled and total_tokens > self.MAX_CHUNK_TOKENS:
                chunks = splitter.split_text(markdown)
                chunk_paths = []
                for i, chunk_text in enumerate(chunks):
                    tmp = f"/tmp/{article_id}-{i:03d}.md"
                    with open(tmp, "w") as f:
                        f.write(chunk_text)
                    chunk_paths.append(tmp)
                # Use first chunk for hash comparison (all-or-nothing)
                api_index[article_id] = {
                    "tmp_path": chunk_paths[0],
                    "content_hash": self._file_hash(chunk_paths[0]),
                    "chunks": chunk_paths,
                }
            else:
                # Chunking disabled (or article already fits): single file
                tmp = f"/tmp/{article_id}.md"
                with open(tmp, "w") as f:
                    f.write(markdown)
                api_index[article_id] = {
                    "tmp_path": tmp,
                    "content_hash": self._file_hash(tmp),
                    "chunks": [tmp],
                }

        # 3. Reconciliation: 3-way hash diff
        output_dir = self.get_destination_path()
        disk_index = {}
        if os.path.isdir(output_dir):
            for fname in os.listdir(output_dir):
                stem = fname.rsplit(".", 1)[0]
                # Map any chunk back to the article_id (stem before -NNN)
                fpath = os.path.join(output_dir, fname)
                try:
                    disk_index.setdefault(
                        self._chunk_to_article_id(stem), set()
                    ).add(fpath)
                except OSError:
                    pass

        api_keys = set(api_index.keys())
        disk_keys = set(disk_index.keys())
        to_add = api_keys - disk_keys
        to_update = {
            k for k in api_keys & disk_keys
            if api_index[k]["content_hash"] != self._file_hash(
                # Use any disk file for that article; take first
                next(iter(disk_index[k]))
            )
        }
        to_delete = disk_keys - api_keys
        to_skip = api_keys & disk_keys - to_update

        total_work = len(to_add) + len(to_update) + len(to_delete)
        done_work = 0

        progress_callback(
            10,
            message=f"{len(api_keys)} articles, "
                    f"{len(to_add)} new, {len(to_update)} updated, "
                    f"{len(to_delete)} stale, {len(to_skip)} unchanged",
        )

        # 4. Process adds + updates
        for stable_id in to_add | to_update:
            for tmp in api_index[stable_id]["chunks"]:
                self.move_to_destination(tmp)
            done_work += 1
            pct = 10 + int(85 * done_work / total_work) if total_work else 95
            progress_callback(pct)

        # 5. Process deletions
        for stable_id in to_delete:
            for fpath in disk_index[stable_id]:
                try:
                    os.remove(fpath)
                except FileNotFoundError:
                    pass
            done_work += 1
            pct = 10 + int(85 * done_work / total_work) if total_work else 95
            progress_callback(pct)

        # 6. Stray file cleanup
        all_article_chunks = set()
        for v in api_index.values():
            for tmp in v["chunks"]:
                all_article_chunks.add(os.path.basename(tmp))
        if os.path.isdir(output_dir):
            for fname in os.listdir(output_dir):
                if fname not in all_article_chunks:
                    os.remove(os.path.join(output_dir, fname))

        # 7. Clean up temp files for skipped items
        for stable_id in to_skip:
            for tmp in api_index[stable_id]["chunks"]:
                try:
                    os.remove(tmp)
                except FileNotFoundError:
                    pass

        progress_callback(
            100,
            message=f"Done: {len(to_add)} added, {len(to_update)} updated, "
                    f"{len(to_delete)} removed, {len(to_skip)} unchanged",
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _stable_id(entry):
        """Deterministic identifier from a feed entry."""
        id_str = entry.get("id") or entry.get("link") or entry.get("title", "")
        return hashlib.sha256(id_str.encode()).hexdigest()[:16]

    @staticmethod
    def _file_hash(path):
        h = hashlib.sha256()
        with open(path, "rb") as f:
            h.update(f.read())
        return h.hexdigest()

    @staticmethod
    def _chunk_to_article_id(stem):
        """Map 'abc123-002' -> 'abc123' for chunked articles."""
        return stem.rsplit("-", 1)[0] if re.search(r"-\d{3}$", stem) else stem

    @staticmethod
    def _get_content(entry):
        """Extract best text content from a feed entry."""
        if hasattr(entry, "content") and entry.content:
            return entry.content[0].get("value", "")
        if hasattr(entry, "summary"):
            return entry.summary
        return ""
```

This plugin demonstrates every major concept: schema with configurable fields (including optional `chunking_enabled`), data fetching via HTTP, file output in markdown, 3-way hash reconciliation with stray file cleanup, optional chunking with `RecursiveCharacterTextSplitter`, structured progress tracking, and per-item error resilience.

---

## 15. File Placement

| Item | Location / Method |
|------|------------------|
| Plugin `.py` file | **Option A:** Upload via Web UI **Developer Lab** (`#/devlab`) — paste code, click **Save**. **Option B:** Place directly in `/src/plugins/{yourPlugin}.py`. |
| Plugin icon `.png` | **Option A:** Upload through the Developer Lab's icon picker alongside your plugin code. **Option B:** Place manually in `/assets/{yourPlugin}.png` (≤ 512×512). The registry derives the filename from the plugin name — no `metadata["icon"]` key needed. |

Both delivery methods end up in the same place — the Developer Lab writes the file to `/src/plugins/`, triggering the same hot-swap watcher. There is no registration step, no API call, and no restart.

---

## 16. Checklist

Before considering your plugin complete, verify:

- [ ] File is a single `.py` file in `/src/plugins/`
- [ ] Class subclasses `BaseSubscription`
- [ ] `metadata["name"]` matches the filename stem (camelCase, ≤ 32 chars)
- [ ] `metadata["sub_type"]` is `"SCHEDULED"` or `"EVENT_BASED"` (and `monitor()` is implemented if EVENT_BASED)
- [ ] `DEFAULT_ACCESS_LEVEL` is `"PRIVATE"` or `"PUBLIC"`
- [ ] `get_schema()` returns a valid JSON Schema dict
- [ ] `getData()` calls `progress_callback()` periodically
- [ ] `getData()` does NOT catch `SubscriptionCancelledError`
- [ ] `getData()` calls `progress_callback(100)` before returning
- [ ] Milestone `progress_callback()` calls include a `message` summarizing what the plugin did
- [ ] Output files are written to `/tmp/` then moved via `self.move_to_destination()`
- [ ] Output files use a stable, deterministic naming scheme
- [ ] Output directory contains only content files — no metadata, tracking, or state files
- [ ] Reconciliation handles: new files, changed files, deleted files, stray files
- [ ] If the plugin chunks, `get_schema()` exposes a `chunking_enabled` boolean (default `True`) and `getData()` honors it
- [ ] Token counting uses `tiktoken.get_encoding("cl100k_base")` (pre-cached)
- [ ] Any new dependencies are documented and conveyed to the Docker image maintainer
- [ ] Icon file (if not using `default_icon.png`) is placed in `/assets/{yourPlugin}.png` or uploaded via Developer Lab
- [ ] All blocking I/O in `getData()` is sync; all I/O in `monitor()` is async
- [ ] Structured logging uses `self.log.info/error` with keyword arguments
