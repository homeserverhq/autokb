# AutoKB Sink Development Guide

## 1. Overview

An AutoKB **sink** (also called a *Destination connector*) is **a single Python file** subclassing `BaseSink` that implements six small remote-operation methods. Where a **plugin** pulls data out of an external source and writes files to `/output/`, a **sink** pushes those files into an external *destination* — a knowledge base, a dataset, a document store, or any remote service that should receive AutoKB's output.

Each sink describes one **Destination service type** (e.g. Open WebUI, Cognee). A user creates a **Data Target** under that Destination — one concrete remote instance (one knowledge base, one dataset) bound to a URL, an API key, and the subscriptions whose output it should receive. Every time a subscription runs, the recon engine compares what is on disk in `/output/` against what the target already has, and calls your six methods to add, update, and remove the difference.

A sink can be uploaded through the **Web UI Developer Lab → Destination Developer** (`#/devlab/destination`) or placed directly in `/src/sinks/`. Either way it is hot-swapped into the running system within ~2 seconds. No server restart, no config file, no registration step.

### Architecture at a Glance

```
Sink file → file watcher (2s debounce) → SinkRegistry (validates + loads)
                                                     ↓
Worker recon engine → compare /output vs remote → your six methods
                                                     ↓
                                  base_add_datafile / base_update_datafile /
                                  base_remove_datafile / base_add_target
                                    (DB bookkeeping + hashing handled for you)
```

- **Manager** runs the file watcher, the API, and registers every loaded sink as a Destination row.
- **Worker** runs the recon engine after every subscription run (and on `SINK_ONLY` triggers) and instantiates your sink against each linked Target.
- **Web UI** provides a **Developer Lab** for uploading/testing/saving sinks with an optional icon, and a Destination/Target management view.
- **Input** comes from `/output/{sanitized_plugin_name}/{sanitized_subscription_name}/` — the files your plugins produce.

### The Single-File Rule

**Everything** your sink needs must live in one `.py` file. There are no sidecar files, no companion modules, no package directories. The file stem (minus `.py`) must:

- **end with `Sink`** — the registry only scans files matching `*Sink.py` in `/src/sinks/`;
- be a valid **camelCase** identifier ≤ 32 characters;
- equal `sanitize_name(metadata["name"])` exactly.

If your sink needs data files (icons, reference data), those go in `/assets/` and are referenced by the `metadata["icon"]` field. When uploading through the **Developer Lab**, you can attach the `.png` icon alongside the sink code.

### Zero Access Required

You do **not** need to read any AutoKB source code to create a sink. Everything you need is in this document and the `BaseSink` class documented below. The system handles: target bookkeeping, file hashing, change detection, reconciliation scheduling, API-key encryption at rest (and decryption before your code runs), orphan-target cleanup, and error notification.

---

## 2. Sink Skeleton

Every sink starts with this minimal structure. It subclasses `BaseSink` and implements the six abstract remote-operation methods:

```python
import os

import requests

from utils.sink_base import BaseSink


class myFirstSink(BaseSink):
    metadata = {
        "name": "myFirstSink",
        "display_name": "My First Connector",
        "description": "Uploads AutoKB output files to an example REST endpoint.",
        "icon": "myFirstSink.png",
    }
    default_api_url = "https://example.com"
    api_key_env_var = "MY_FIRST_SINK_API_KEY"

    def add_datafile(self, path: str) -> str:
        """Upload *path* to the target. Return the remote file id."""
        resp = requests.post(
            f"{self.api_url}/files",
            headers=self._headers(),
            files={"file": (os.path.basename(path), open(path, "rb"))},
            timeout=60,
        )
        self._check(resp, "add datafile")
        return resp.json()["id"]

    def update_datafile(self, remote_datafile_id: str, path: str) -> str:
        """Re-upload *path* over an existing remote file. Return the new id."""
        resp = requests.put(
            f"{self.api_url}/files/{remote_datafile_id}",
            headers=self._headers(),
            files={"file": (os.path.basename(path), open(path, "rb"))},
            timeout=60,
        )
        self._check(resp, "update datafile")
        return resp.json()["id"]

    def remove_datafile(self, remote_datafile_id: str) -> None:
        """Delete a remote file. Must be idempotent."""
        resp = requests.delete(
            f"{self.api_url}/files/{remote_datafile_id}",
            headers=self._headers(),
            timeout=60,
        )
        if resp.status_code == 404:
            return  # already gone → idempotent
        self._check(resp, "remove datafile")

    def add_target(self) -> str:
        """Create the remote container (knowledge base / dataset). Return its id."""
        resp = requests.post(
            f"{self.api_url}/targets",
            headers=self._headers(),
            json={"name": f"AutoKB_{self.name}"},
            timeout=60,
        )
        self._check(resp, "add target")
        return resp.json()["id"]

    def remove_target(self) -> None:
        """Destroy the remote container and all its files."""
        resp = requests.delete(
            f"{self.api_url}/targets/{self.remote_target_id}",
            headers=self._headers(),
            timeout=60,
        )
        if resp.status_code == 404:
            return
        self._check(resp, "remove target")

    def clear_target(self) -> None:
        """Remove every file from the remote container but keep it."""
        resp = requests.delete(
            f"{self.api_url}/targets/{self.remote_target_id}/files",
            headers=self._headers(),
            timeout=60,
        )
        self._check(resp, "clear target")

    # ---- helpers ----
    def _headers(self):
        return {"Authorization": f"Bearer {self.api_key}", "Accept": "application/json"}

    def _check(self, resp: requests.Response, action: str) -> None:
        if resp.status_code >= 400:
            raise RuntimeError(
                f"{self.metadata['name']} {action} failed: HTTP {resp.status_code}: {resp.text[:500]}"
            )
```

That is a complete, working sink. Save it as `myFirstSink.py` in `/src/sinks/` and it will appear in the **Data Destinations** view within seconds.

---

## 3. Metadata Fields

The `metadata` dict is a **class-level** attribute. It is validated at load time.

| Field          | Type   | Required | Description |
|----------------|--------|----------|-------------|
| `name`         | str    | Yes      | camelCase identifier, ≤ 32 chars, must **end with `Sink`** and equal the `.py` filename stem exactly (after `sanitize_name()`). Example: `"openWebUISink"` |
| `display_name` | str    | No       | Human-friendly label shown in the Data Destinations grid and Target forms (e.g. `"Open WebUI Knowledge Base"`). Falls back to `name` when absent. |
| `icon`         | str    | No       | Filename in `/assets/`. Default: `"default_icon.png"`. Place a matching `.png` in `/repos/autokb/assets/` (≤ 512×512). |
| `description`  | str    | No       | Shown in the Data Destinations grid. Should explain what remote service this connects to and what a Target maps to on the remote side. |

### Naming Rules

- The file stem **must end with `Sink`** (`openWebUISink.py`, `cogneeSink.py`). Files that do not match `*Sink.py` are ignored by the registry.
- `metadata["name"]` must be a valid **camelCase** identifier ≤ 32 characters that **already ends with `Sink`**.
- `sanitize_name(metadata["name"])` must equal the file stem exactly — the same rule plugins follow for their names. `sanitize_name()` keeps only `[a-zA-Z0-9.-]`, so keep the metadata name clean.

---

## 4. Class-Level Defaults

The Web UI needs to know sensible defaults for a Target *before* the user fills in the create-target form. You provide them with class attributes plus an optional classmethod:

```python
default_api_url: str = ""
api_key_env_var: Optional[str] = None
```

| Attribute | Purpose |
|-----------|---------|
| `default_api_url` | Fallback base URL used when the target row has no explicit `api_url` (e.g. `"http://openwebui-app:8080"`). Surfaced as the pre-filled value in the create-target form. |
| `api_key_env_var` | Env var name that supplies the API-key default (e.g. `"OPENWEBUI_API_KEY"`). The secret itself never leaves the backend. |

### `get_defaults()` classmethod

`BaseSink` provides a default implementation that returns:

```python
{
    "api_url": default_api_url or "",
    "has_api_key_default": bool(api_key_env_var and os.environ.get(api_key_env_var)),
}
```

- `api_url` — the pre-filled URL in the create-target form.
- `has_api_key_default` — a **boolean**, not the key itself. When `True`, the UI can show "an API key is available by default" and the user can leave the field blank; the real key resolves at recon time in `__init__`.

You rarely need to override `get_defaults()`; setting the two class attributes is enough.

---

## 5. The Instance Contract

Your sink is **not** instantiated once per service. It is instantiated **once per Target, per recon pass** by the recon engine:

```python
def __init__(self, target_row: Any, db: Any):
```

`target_row` is the ORM row for one Data Target and exposes these attributes:

| Attribute | Description |
|-----------|-------------|
| `id`          | Local Target row id (AutoKB's internal id). |
| `service_id`  | id of the Destination (`sink`) row this Target belongs to. |
| `name`        | The user-chosen Target name (e.g. `"My Dev KB"`). |
| `api_url`     | Base URL of the remote service. **May be empty** — use your `default_api_url` as a fallback (the `or` pattern shown in the Section 2 skeleton). |
| `api_key`     | The API key, **already decrypted** by the caller. Never send it back to AutoKB; it is masked in API responses and encrypted at rest in the database. |
| `remote_target_id` | The id assigned by the **remote** service once the container (knowledge base / dataset) has been created. `None` until the first successful `add_target()`. |
| `target_extra_params` | A free-form dict of extra configuration the user supplied when creating the Target (see Section 8). |

You also receive `db`, the `DatabaseManager`, for any custom bookkeeping — but for a garden-variety sink you will never touch it: the base wrapper methods (Section 7) handle all persistence.

A typical `__init__`:

```python
def __init__(self, target_row, db):
    super().__init__(target_row, db)
    self.api_url = (self.api_url or self.default_api_url).rstrip("/")
    self.api_key = self.api_key or (os.environ.get(self.api_key_env_var, "") if self.api_key_env_var else "")
```

This normalizes the URL (strips trailing `/` so your path joins are clean) and falls back to the env-var default when the user left the key blank.

---

## 6. The Six Abstract Methods

These six methods are the **only** thing you implement. They perform remote operations only — the base class does all local bookkeeping (hashing, `akb_datafile` rows, `target_datafile` join rows, remote-id persistence). Your methods must **not** write to the database.

### 6.1 `add_datafile(path) -> str`

Called when a new file appears in `/output/{plugin}/{sub}/` that the target does not yet have.

- Upload the file at `path` to the remote service.
- **Return the `remote_datafile_id`** assigned by the remote instance. The engine persists it, so it can `update_datafile`/`remove_datafile` later.
- If the upload fails partway, raise an exception — the engine will log it, mark the target subscription ERROR (once), and continue with the other files.

### 6.2 `update_datafile(remote_datafile_id, path) -> str`

Called when a tracked file's content changes (the engine detects the hash differs from what the target last received).

- Re-upload `path` over the existing remote file identified by `remote_datafile_id`.
- **Return the NEW `remote_datafile_id`** assigned after the re-upload. It may equal the old id if the remote instance dedupes by content — callers persist whatever you return, so returning the old id when nothing changed is correct and harmless.

### 6.3 `remove_datafile(remote_datafile_id) -> None`

Called when a file is deleted from `/output/` but is still tracked on the target.

- Delete the remote file.
- **Must be idempotent**: if the remote file is already gone, treat a `404` as success and return normally (do not raise). The engine can call this multiple times for the same id across retries.

### 6.4 `add_target() -> str`

Called during recon when the target row has no `remote_target_id` yet — i.e. the first time a subscription's output must be delivered to this Target.

- Create the remote container — a knowledge base, dataset, collection, bucket, etc.
- **Return the `remote_target_id`** assigned by the remote instance. The engine persists it on the target row, so later passes use it for `update`/`remove`.
- Naming the container deterministically (e.g. `AutoKB_{target_name}`) lets you find it again on the remote side.

### 6.5 `remove_target() -> None`

Called when the **last** subscription is unlinked from the Target (or the Target is deleted with no remaining subscriptions). The remote container is now orphaned.

- Destroy the remote container **and all its datafiles**.
- **Must be idempotent** (404 == success).
- Do not touch the local database — the engine cleans up the join rows and the target row itself after you return.

### 6.6 `clear_target() -> None`

- Remove all datafiles from the remote container **but keep the container itself**.
- Reserved for a full re-import workflow. **It is not currently invoked** — implement it to satisfy the abstract contract, but know that today the engine never calls it. A clean implementation (delete all files in the container) is all that's required.

### Summary Table

| Method | Trigger | Returns | Idempotent required |
|--------|---------|---------|---------------------|
| `add_datafile`    | new file on disk         | `remote_datafile_id` | no  |
| `update_datafile` | file content changed     | new `remote_datafile_id` | no |
| `remove_datafile` | file deleted on disk     | — | **yes** (404 == success) |
| `add_target`      | first recon, `remote_target_id` is null | `remote_target_id` | no |
| `remove_target`   | last subscription removed | — | **yes** (404 == success) |
| `clear_target`    | (contract only, unused)  | — | yes |

---

## 7. Base Wrapper Methods — What AutoKB Does For You

You implement the six abstract methods above. `BaseSink` then wraps them so the recon engine never has to touch the database directly:

| Wrapper | What it does |
|---------|--------------|
| `base_add_datafile(sub_id, path)` | Sizes/hashes the file (`compute_file_hash`), gets-or-creates the `akb_datafile` row, checks the `target_datafile` join, and only then calls your `add_datafile(path)` and persists the returned remote id. Returns early if the file is already tracked for this target. |
| `base_update_datafile(datafile_id, new_hash)` | Looks up the tracked join row, calls your `update_datafile(remote_id, path)`, persists the new remote id (if returned) and the new content hash. |
| `base_remove_datafile(datafile_id)` | Looks up the join row, calls your `remove_datafile(remote_id)`, then deletes the join row. |
| `base_add_target()` | Calls your `add_target()`, persists the returned remote id on the target row, and updates `self.remote_target_id`. |

An additional concrete helper is available for building deterministic remote filenames:

| Helper | What it does |
|--------|--------------|
| `remote_file_name(path)` | Returns a deterministic remote filename: `autokb_{target}_{basename}`, or when `include_path_in_filename` is enabled, `autokb_{target}_{rel_path_with_underscores}` so the full directory under `/output/` is embedded. See Section 9 for the flag. |

You should **call these wrappers yourself only in tests**. In production the recon engine calls them. The point of the design: your six methods are pure remote I/O; everything local is handled for you.

### `compute_file_hash(path) -> str`

A module-level helper in `utils.sink_base` that returns the SHA-256 hex digest of a file, chunked (1 MiB) so large files don't exhaust memory. You do not need it for a garden-variety sink — the wrappers use it internally — but it's available if you want to compute hashes for custom logic (e.g. deduping).

---

## 8. Recon Lifecycle — When and How Your Sink Runs

The recon engine (`worker/sink_recon.py`) drives every sink. It is triggered from two paths:

1. At the **end of a FULL subscription run** (after the upstream plugin finishes, debounce, and a re-eval).
2. **Directly** via a `SINK_ONLY` queue operation — e.g. when a Target is created and linked to existing subscriptions, or when the user manually triggers a target update.

For each subscription, the engine:

1. **Gathers linked Targets** (`target_subscription` rows). Subscriptions with no Targets are skipped entirely.
2. **Collects files** on disk under `/output/{plugin}/{sub}/` (recursive).
3. For each Target (skipping `DISABLED`/`ERROR` links):
   - **Ensure the remote container exists** — if `remote_target_id` is null, calls `base_add_target()`. If it raises, the target link transitions to `ERROR` and recon continues to the next target.
   - **Pass I.1 — adds**: every file on disk with no tracked join row → `base_add_datafile()`.
   - **Pass I.1 — updates**: every tracked file whose content hash on disk differs from the last synced hash → `base_update_datafile()`.
   - **Pass I.2 — removals**: every tracked join row with no matching file on disk → `base_remove_datafile()`.
   - Sets the link status back to `ENABLED` with a summary message (`Reconciled: +N added, ~N updated, -N removed`).
4. **Pass II** syncs `akb_datafile` stats (size/mtime/hash) from the filesystem and prunes rows for deleted files.
5. **Orphan cleanup**: when the last subscription is unlinked from a Target, the engine calls `remove_target()` and deletes the target row.

### Per-File Resilience

The engine wraps **each** add/update/remove call in its own `try/except`. A failure on one file logs a warning (`sink_add_failed`, `sink_update_failed`, `sink_remove_failed`) and does **not** abort the rest of the recon. Only `add_target()` failures mark the whole link `ERROR` — because without a remote container nothing else can proceed.

### Error Transitions

When a target-subscription transitions to `ERROR`, the engine sends **one** notification email (subject `[AutoKB] SINK target error: {target_name}`) and only on the transition (not on every recon). On the next successful recon the link returns to `ENABLED`.

---

## 9. Configuration via `target_extra_params`

Plugins define a JSON Schema (`get_schema()`) and the UI builds a form from it. **Sinks have no schema.** Their configuration channel is the `target_extra_params` field on the Target, a free-form JSON dict the user supplies when creating the Target, which arrives in `__init__` as `self.target_extra_params` (already parsed, defaulting to `{}`).

Read and validate it in `__init__`, raising a clear error if a required value is missing:

```python
def __init__(self, target_row, db):
    super().__init__(target_row, db)
    self.api_url = (self.api_url or self.default_api_url).rstrip("/")
    self.api_key = self.api_key or (os.environ.get(self.api_key_env_var, "") if self.api_key_env_var else "")
    self.embed_model = self.target_extra_params.get("embed_model", "default-embedder")
```

### First-Class Target Flags

Beyond `target_extra_params`, every Target row carries a **first-class boolean** that the base class reads automatically:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `include_path_in_filename` | `bool` | `false` | When enabled, the **remote filename** includes the full directory structure under `/output/`. Instead of `autokb_{target}_{basename}`, the scheme becomes `autokb_{target}_{rel_dir_with_underscores}_{basename}`. See `BaseSink.remote_file_name()` below. |

This field is toggled through the **checkbox** in the create/edit Target form (not via the JSON textarea). Every sink benefits from it immediately: `BaseSink.remote_file_name(path)` checks the flag before formatting the name.

### Create-Target API Fields

The POST `/api/sinks/{service_id}/targets` endpoint accepts:

| Field | Type | Description |
|-------|------|-------------|
| `name` | str | Target name (required). |
| `api_url` | str | Base URL of the remote service (required). |
| `api_key` | str | API key — encrypted at rest, decrypted before your `__init__`. |
| `target_extra_params` | str/obj | Free-form JSON; becomes `self.target_extra_params`. |
| `subscription_ids` | list | Subscriptions to link; each linked sub gets a `SINK_ONLY` enqueue so recon runs immediately. |

---

## 10. Error Handling

### Raise, Don't Swallow

When a remote operation fails, raise an exception (a `RuntimeError` with a short human-readable message is the convention). The engine:

- logs the failure,
- continues with the other files (per-file resilience), or marks the link `ERROR` for `add_target()` failures,
- sends one notification email on transition to `ERROR`.

Do not return `None`/`""` from `add_datafile()` or `add_target()` as a way to signal failure — an empty remote id will corrupt tracking. Raise instead.

### Idempotency for Deletes

`remove_datafile()` and `remove_target()` may be called multiple times for the same remote object. Treat "not found" as success:

```python
resp = requests.delete(...)
if resp.status_code == 404:
    return  # already gone → idempotent
self._check(resp, "remove datafile")
```

### HTTP Helper

A small `_check(resp, action)` helper keeps every method to a clean call + guard:

```python
def _check(self, resp: requests.Response, action: str) -> None:
    if resp.status_code >= 400:
        body = resp.text[:500]
        raise RuntimeError(f"{self.metadata['name']} {action} failed: HTTP {resp.status_code}: {body}")
```

### Structured Logging

A logger is available at `self.log` (name `sink.{ClassName}`) for anything you want to record beyond the exceptions:

```python
self.log.warning("remote_timeout", action="add_datafile", target=self.name)
```

Logs are written to `/logs/worker.log`.

---

## 11. Dependencies

### What is Already Installed

The unified Docker image ships with everything the existing sinks use. The most relevant for sinks:

| Package | Why |
|---------|-----|
| `requests` | The de-facto choice for synchronous HTTP calls. |
| `httpx` | Async HTTP client, if you ever need it. |
| `aiohttp` | Another async HTTP client. |
| `cryptography` | Used by AutoKB for API-key encryption at rest — do not roll your own. |
| `sanitize_name` | From `utils.misc_utils` — use it for filenames that go to the remote side (`autokb_{sanitized_name}_{basename}`). |

Sink methods run in the **worker process** (not an event loop), so plain synchronous `requests` calls are fine. You do not need asyncio.

### Adding New Dependencies

Most needs are covered by the pre-installed packages. If your sink requires a library not present:

1. **Note the exact library and version** in your sink's docstring or comments.
2. **Convey this to the person or team** maintaining the Docker image.
3. The dependency is added to `/repos/autokb/requirements.txt`.
4. The unified Docker image is rebuilt.

There is no per-sink dependency mechanism. All sinks share the same runtime image.

---

## 12. Validation at Load Time

When the file watcher detects a new or changed sink file — and again when you click **Save** in the Developer Lab — the system validates:

| Check | Failure behavior |
|-------|------------------|
| File is valid Python | Sink is not loaded; error logged |
| Exactly one `BaseSink` subclass exists (defined in the file) | Sink is not loaded |
| `metadata["name"]` exists and sanitizes to the filename stem | Sink is not loaded |
| Filename stem ends with `Sink` | Sink is not loaded |
| All six abstract methods are implemented (`add_datafile`, `update_datafile`, `remove_datafile`, `add_target`, `remove_target`, `clear_target`) | Sink is not loaded |
| (Save path only) The module imports and the class is not abstract | Save is rejected with a validation error |

The Developer Lab `Test` button runs the static checks without touching disk; `Save` additionally imports your code from a temp file to confirm it actually loads, then atomically writes it to `/src/sinks/{name}.py` and (optionally) saves your uploaded icon to `/assets/{name}.png`.

---

## 13. Full Walkthrough: `restUploadSink`

Let's build a complete, garden-variety sink that delivers AutoKB output files to a simple REST service. It assumes the remote API follows a plain, uncomplicated convention:

```
POST   {api_url}/targets                      → create a container        → {"id": "..."}
DELETE {api_url}/targets/{target_id}          → destroy a container        → 204
POST   {api_url}/targets/{target_id}/files    → upload a file (multipart)  → {"id": "..."}
PUT    {api_url}/targets/{target_id}/files/{remote_id}  → re-upload        → {"id": "..."}
DELETE {api_url}/targets/{target_id}/files/{remote_id}  → delete a file    → 204 (404 ok)
DELETE {api_url}/targets/{target_id}/files    → clear a container          → 204
```

The convention is deliberately minimal — one remote container per Data Target, one HTTP call per method. Files are added when they appear, replaced when they change, and deleted when they disappear; the container is created on the first recon and destroyed when the last subscription is removed. That keeps every method to a single request plus an error guard, which is the correct shape for most targets.

```python
"""restUploadSink — delivers AutoKB output to a simple REST file service.

Each Data Target maps to one remote container (a knowledge base / dataset /
collection) created on the remote service. Files are uploaded, replaced on
change, and deleted when they disappear from /output/.
"""

import os

import requests

from utils.sink_base import BaseSink


class RestUploadSink(BaseSink):
    metadata = {
        "name": "restUploadSink",
        "display_name": "Generic REST Upload",
        "description": (
            "Uploads AutoKB output files to a generic REST endpoint. Each "
            "Data Target creates one container on the remote service; files "
            "are added, replaced on change, and removed when deleted."
        ),
        "icon": "restUploadSink.png",
    }
    default_api_url = "http://rest-upload-app:8000"
    api_key_env_var = "REST_UPLOAD_API_KEY"
    _TIMEOUT = 60

    def __init__(self, target_row, db):
        super().__init__(target_row, db)
        self.api_url = (self.api_url or self.default_api_url).rstrip("/")
        self.api_key = self.api_key or (
            os.environ.get(self.api_key_env_var, "") if self.api_key_env_var else ""
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _headers(self):
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/json",
        }

    def _url(self, endpoint: str) -> str:
        return f"{self.api_url}/{endpoint.lstrip('/')}"

    def _check(self, resp: requests.Response, action: str) -> None:
        if resp.status_code >= 400:
            body = resp.text[:500]
            raise RuntimeError(
                f"{self.metadata['name']} {action} failed: "
                f"HTTP {resp.status_code}: {body}"
            )

    def _remote_file_name(self, path: str) -> str:
        """Deterministic remote filename: autokb_{target}_{basename}."""
        from utils.misc_utils import sanitize_name
        return f"autokb_{sanitize_name(self.name)}_{os.path.basename(path)}"

    # ------------------------------------------------------------------
    # The six abstract methods
    # ------------------------------------------------------------------
    def add_datafile(self, path: str) -> str:
        with open(path, "rb") as f:
            files = {"file": (self._remote_file_name(path), f, "application/octet-stream")}
            resp = requests.post(
                self._url(f"targets/{self.remote_target_id}/files"),
                headers=self._headers(),
                files=files,
                timeout=self._TIMEOUT,
            )
        self._check(resp, "add datafile")
        return resp.json()["id"]

    def update_datafile(self, remote_datafile_id: str, path: str) -> str:
        with open(path, "rb") as f:
            files = {"file": (self._remote_file_name(path), f, "application/octet-stream")}
            resp = requests.put(
                self._url(f"targets/{self.remote_target_id}/files/{remote_datafile_id}"),
                headers=self._headers(),
                files=files,
                timeout=self._TIMEOUT,
            )
        self._check(resp, "update datafile")
        return resp.json()["id"]

    def remove_datafile(self, remote_datafile_id: str) -> None:
        resp = requests.delete(
            self._url(f"targets/{self.remote_target_id}/files/{remote_datafile_id}"),
            headers=self._headers(),
            timeout=self._TIMEOUT,
        )
        if resp.status_code == 404:
            return  # already gone → idempotent
        self._check(resp, "remove datafile")

    def add_target(self) -> str:
        resp = requests.post(
            self._url("targets"),
            headers=self._headers(),
            json={"name": f"AutoKB_{self.name}"},
            timeout=self._TIMEOUT,
        )
        self._check(resp, "add target")
        return resp.json()["id"]

    def remove_target(self) -> None:
        target_id = self.remote_target_id
        if not target_id:
            return
        resp = requests.delete(
            self._url(f"targets/{target_id}"),
            headers=self._headers(),
            timeout=self._TIMEOUT,
        )
        if resp.status_code == 404:
            return  # already gone → idempotent
        self._check(resp, "remove target")

    def clear_target(self) -> None:
        target_id = self.remote_target_id
        if not target_id:
            return
        resp = requests.delete(
            self._url(f"targets/{target_id}/files"),
            headers=self._headers(),
            timeout=self._TIMEOUT,
        )
        self._check(resp, "clear target")


__all__ = ["RestUploadSink"]
```

This sink demonstrates every concept in this guide:

- **Class-level defaults** — `default_api_url` and `api_key_env_var` drive the create-target form.
- **Constructor normalization** — trailing-`/` strip and env-var key fallback.
- **All six abstract methods**, each a single HTTP call + `_check` guard.
- **Idempotent deletes** — `404` treated as success in `remove_datafile` and `remove_target`.
- **Deterministic remote filenames** — `autokb_{sanitized_target}_{basename}` so the same local file always maps to the same remote name.
- **Pure remote I/O** — no database access; the recon engine and base wrappers handle all bookkeeping.

To adapt it to a real service (Open WebUI, Cognee, a WebDAV server, etc.), change the HTTP calls inside the six methods to match that service's API. The surrounding contract — what each method is for, what it returns, and when the engine calls it — stays identical.

---

## 14. File Placement

| Item | Location / Method |
|------|------------------|
| Sink `.py` file | **Option A:** Upload via Web UI **Developer Lab → Destination Developer** (`#/devlab/destination`) — paste code, click **Save**. **Option B:** Place directly in `/src/sinks/{yourSink}.py`. |
| Sink icon `.png` | **Option A:** Upload through the Developer Lab's icon picker alongside your sink code. **Option B:** Place manually in `/assets/{yourSink}.png` (≤ 512×512, referenced by `metadata["icon"]`). |

Both delivery methods end up in the same place — the Developer Lab writes the file to `/src/sinks/`, triggering the same hot-swap watcher. There is no registration step, no API call, and no restart. The Manager's watcher reloads the `SinkRegistry` and upserts the `sink` row within ~2 seconds; the Worker lazy-loads the file from disk on demand, so the very next recon uses your new code.

---

## 15. Checklist

Before considering your sink complete, verify:

- [ ] File is a single `.py` file in `/src/sinks/` whose name **ends with `Sink`**
- [ ] Class subclasses `BaseSink`
- [ ] `metadata["name"]` matches the filename stem, is camelCase, ends with `Sink`, and is ≤ 32 chars
- [ ] `metadata["display_name"]` is a human-friendly label (falls back to `name` if absent)
- [ ] All six abstract methods are implemented: `add_datafile`, `update_datafile`, `remove_datafile`, `add_target`, `remove_target`, `clear_target`
- [ ] `add_datafile` and `update_datafile` return the remote id assigned by the remote service
- [ ] `add_target` returns the remote container id assigned by the remote service
- [ ] `remove_datafile` and `remove_target` are idempotent (treat "not found" as success)
- [ ] `__init__` falls back to `default_api_url` when `api_url` is empty and to `api_key_env_var` when `api_key` is empty
- [ ] Remote filenames use a deterministic scheme (e.g. `autokb_{sanitized_target}_{basename}`)
- [ ] Your methods perform **remote I/O only** — no direct database access
- [ ] Failures are reported by raising, never by returning empty/`None` remote ids
- [ ] Remote calls use a timeout and a `_check`-style HTTP guard that surfaces a readable error
- [ ] Any new dependencies are documented and conveyed to the Docker image maintainer
- [ ] Icon file (if not using `default_icon.png`) is placed in `/assets/` or uploaded via Developer Lab
