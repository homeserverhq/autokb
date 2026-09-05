"""The Subscription Manager — FastAPI app, routes, plugin loader, and scheduler.

Wires together:
  * SQLAlchemy engine + alembic migrations
  * Redis-backed queue
  * In-memory plugin registry with file-watcher hot-swap
  * The trigger coordinator (cron + monitor)
  * SSE bridge (asyncpg LISTEN/NOTIFY)
  * All REST endpoints under /api/*
"""

import asyncio
import hmac
import json
import os
import sys
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, List, Optional, Set

# Ensure /src is on sys.path so sibling package imports work when this
# module is invoked as a script (`python /src/manager/manager.py`).
# The trick: when running a script, Python adds the script's directory
# (here /src/manager) to sys.path[0]. We must remove that to prevent
# `manager.py` from shadowing the `manager` package, and add /src instead.
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_PARENT = os.path.dirname(_THIS_DIR)
# Remove the script's directory from sys.path
sys.path = [p for p in sys.path if os.path.realpath(p) != os.path.realpath(_THIS_DIR)]
if _PARENT not in sys.path:
    sys.path.insert(0, _PARENT)
# Set __package__ so sibling imports work even when this file is the entrypoint.
if __name__ == "__main__" and __package__ in (None, ""):
    __package__ = "manager"

from fastapi import Body, FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from utils.constants import (
    ACCESS_PRIVATE,
    ACCESS_PUBLIC,
    DEBOUNCE_SECONDS,
    NOTIFY_CHANNEL,
    P_QUEUE_KEY,
    STATE_DELETED,
    STATE_DISABLED,
    STATE_ENABLED,
    STATE_ERROR,
    STATE_IN_PROGRESS,
    SUB_TYPE_EVENT_BASED,
    SUB_TYPE_SCHEDULED,
    TRIGGERABLE_STATES,
)

from manager.devlab import (  # noqa: E402
    MAX_DISPLAY_NAME_LEN,
    MAX_PLUGIN_NAME_LEN,
    _find_plugin_class_in_module,
    _find_sink_class_in_module,
    _require_display_name,
    _set_metadata_display_name_in_source,
    _validate_plugin_code,
    _validate_sink_code,
)
from manager.services import run_notify_bridge, run_watchdog, sse_generator  # noqa: E402
from manager.validation import (  # noqa: E402
    _ensure_target_remote,
    _validate_access_level,
    _validate_pages_per_batch,
    _validate_schedule_times,
    _validate_subscription_name,
    _validate_target_name,
)

from utils.database import DatabaseManager
from utils.misc_utils import (
    collect_password_field_names,
    encrypt_password_fields,
    get_logger,
    is_valid_cron,
    schema_hash,
    validate_config_against_schema,
)
from utils.queue_utils import QueueManager, wait_for_redis
from utils.registry import PluginRegistry
from utils.seed_builtins import seed_builtins
from utils.sink_registry import SinkRegistry

from .registry import ManagerPluginRegistry
from worker.sink_recon import _remove_orphan_target, _remove_remote_target_strict


# ---------------------------------------------------------------------------
# Globals
# ---------------------------------------------------------------------------
LOG_FILE = "/logs/manager.log"
LOG = get_logger("manager", LOG_FILE)

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://autokb:autokb@autokb-db:5432/autokb")
REDIS_URL = os.environ.get("REDIS_URL", "redis://autokb-redis:6379/0")
BACKEND_API_KEY = os.environ.get("AUTOKB_BACKEND_API_KEY", "")

if not BACKEND_API_KEY:
    print(
        "[manager] WARNING: AUTOKB_BACKEND_API_KEY is not set; "
        "all /api/* routes will be refused (HTTP 503) until it is configured.",
        flush=True,
    )

SMTP_CONFIG = {
    "smtp_host": os.environ.get("SMTP_HOST", ""),
    "smtp_port": int(os.environ.get("SMTP_PORT", "25")),
    "smtp_user": os.environ.get("SMTP_USER", ""),
    "smtp_pass": os.environ.get("SMTP_PASS", ""),
    "from_addr": os.environ.get("SMTP_FROM", "autokb@localhost"),
    "to_addr": os.environ.get("SMTP_NOTIFY_EMAIL", ""),
    "use_tls": os.environ.get("SMTP_USE_TLS", "True").lower() == "true",
    "use_ssl": os.environ.get("SMTP_USE_SSL", "False").lower() == "true",
}


# ---------------------------------------------------------------------------
# State (initialised in lifespan)
# ---------------------------------------------------------------------------
STATE: Dict[str, Any] = {}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def require_backend_key(request: Request) -> None:
    if not BACKEND_API_KEY:
        raise HTTPException(status_code=503, detail="Backend API key is not configured")
    provided = request.headers.get("X-Api-Key", "")
    if not provided or not hmac.compare_digest(provided, BACKEND_API_KEY):
        raise HTTPException(status_code=401, detail="Invalid backend API key")


def _plugin_or_404(plugin_id: str):
    reg: ManagerPluginRegistry = STATE["registry"]
    rec = reg.get(plugin_id)
    if rec is None:
        raise HTTPException(status_code=404, detail=f"Plugin {plugin_id!r} not found")
    return rec


def _sub_or_404(sub_id: str):
    db: DatabaseManager = STATE["db"]
    sub = db.get_subscription(sub_id)
    if not sub:
        raise HTTPException(status_code=404, detail="Subscription not found")
    return sub


def _plugin_display_name(plugin_id: str) -> str:
    """Resolve a plugin's friendly display name, falling back to its id."""
    reg: ManagerPluginRegistry = STATE["registry"]
    rec = reg.get(plugin_id) if reg else None
    return rec.display_name if rec else plugin_id


def _serialise_subscription(sub, password_fields: List[str]) -> Dict[str, Any]:
    db: DatabaseManager = STATE["db"]
    cfg = db.mask_config(sub, password_fields)
    return {
        "id": sub.id,
        "plugin_id": sub.plugin_id,
        "name": sub.name,
        "config": cfg,
        "status": sub.status,
        "last_updated": sub.last_updated.isoformat() if sub.last_updated else None,
        "last_heartbeat": sub.last_heartbeat.isoformat() if sub.last_heartbeat else None,
        "last_error": sub.last_error,
        "progress": sub.progress,
        "sub_type": sub.sub_type,
        "cron": sub.cron,
        "description": sub.description,
        "last_message": sub.last_message,
    }


async def _broadcast_sse(event: Dict[str, Any]) -> None:
    """Fan-out an event to ALL connected SSE clients.

    Multi-client model: each browser tab holds its own EventSource and gets
    its own queue. We iterate over the live set and put the event into each
    queue. If a client's queue is full (slow consumer), we drop the event
    for that client rather than block the broadcaster — the next event or
    the next keepalive will reconnect the gap. The client's reconnect
    triggers a full snapshot, so no data is permanently lost.
    """
    sse_clients: Set[asyncio.Queue] = STATE.get("sse_clients") or set()
    # Snapshot the set so we can mutate it safely during iteration if a
    # client disconnects concurrently.
    dead: List[asyncio.Queue] = []
    for client_queue in list(sse_clients):
        try:
            client_queue.put_nowait(event)
        except asyncio.QueueFull:
            # Slow consumer: drop this event for this client. Their
            # snapshot on reconnect will resync them.
            try:
                LOG.warning("sse_queue_full", action="broadcast", result="dropped_event")
            except Exception:
                pass
        except Exception as exc:  # noqa: BLE001
            dead.append(client_queue)
    for d in dead:
        sse_clients.discard(d)


def _schedule_sse_broadcast(event: Dict[str, Any]) -> None:
    """Schedule an SSE broadcast from a sync (threadpool) endpoint.

    The HTTP handler runs in a threadpool thread, not in the FastAPI event
    loop, so a bare ``asyncio.create_task`` raises ``RuntimeError: no running
    event loop``. We hand the coroutine to the loop via
    ``run_coroutine_threadsafe``.
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        # Fallback: there's no event loop in this thread. Try the main
        # loop stored in STATE (set during lifespan).
        loop = STATE.get("event_loop")
    if loop is None:
        return
    try:
        asyncio.run_coroutine_threadsafe(_broadcast_sse(event), loop)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    # -- seed built-in plugins/sinks into the live (mounted) directories --
    try:
        seed_builtins(LOG)
    except Exception as exc:  # noqa: BLE001
        LOG.error("builtin_seed_failed", action="startup", result=str(exc))

    # -- record the running event loop so sync endpoints can schedule
    #    coroutines on it via run_coroutine_threadsafe --
    STATE["event_loop"] = asyncio.get_running_loop()

    # -- connect to Redis --
    redis_client = wait_for_redis(REDIS_URL, lambda ev, msg: LOG.info(ev, message=msg))
    queue = QueueManager(REDIS_URL)
    STATE["queue"] = queue

    # -- connect to Postgres --
    db = _wait_for_db()
    STATE["db"] = db
    # -- run alembic migrations --
    from utils.database import run_migrations
    run_migrations(DATABASE_URL)

    # -- backfill the _extra_param_* invariant on legacy subscriptions --
    db.backfill_extra_params()

    # -- build registry --
    reg = ManagerPluginRegistry(db=db, app=app, smtp_config=SMTP_CONFIG, log_file=LOG_FILE)
    STATE["registry"] = reg
    reg.reload()

    # -- build Sink registry --
    sink_registry = SinkRegistry(sinks_dir="/src/sinks", component="sink_registry", log_file=LOG_FILE)
    sink_registry.reload_all()
    for rec in sink_registry.list_records():
        db.upsert_sink(rec.service_name, rec.metadata.get("description", ""))
    STATE["sink_registry"] = sink_registry

    # -- start trigger coordinator --
    from manager.scheduler import TriggerCoordinator
    coord = TriggerCoordinator(db=db, queue=queue, registry=reg, smtp_config=SMTP_CONFIG)
    STATE["coordinator"] = coord
    coord_task = asyncio.create_task(coord.run())
    STATE["coord_task"] = coord_task

    # -- start asyncpg LISTEN bridge --
    listen_task = asyncio.create_task(
        run_notify_bridge(DATABASE_URL, NOTIFY_CHANNEL, LOG, _handle_notify)
    )
    STATE["listen_task"] = listen_task

    # -- start watchdog --
    watchdog_task = asyncio.create_task(
        run_watchdog(STATE["db"], STATE["queue"], LOG, SMTP_CONFIG)
    )
    STATE["watchdog_task"] = watchdog_task

    # -- start file watcher --
    watcher_task = asyncio.create_task(_file_watcher())
    STATE["watcher_task"] = watcher_task

    LOG.info("manager_started", action="startup", result="ok")

    try:
        yield
    finally:
        for task in (coord_task, listen_task, watchdog_task, watcher_task):
            task.cancel()
        for task in (coord_task, listen_task, watchdog_task, watcher_task):
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        db.dispose()
        LOG.info("manager_stopped", action="shutdown", result="ok")


def _wait_for_db() -> DatabaseManager:
    from utils.constants import STARTUP_RETRY_SLEEP, MAX_STARTUP_RETRIES
    last_exc: Optional[Exception] = None
    for i in range(MAX_STARTUP_RETRIES):
        try:
            db = DatabaseManager(DATABASE_URL, log_file=LOG_FILE, component="db")
            db.health_check()
            LOG.info("db_connected", action="startup", result="ok", attempt=i + 1)
            return db
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            LOG.warning("db_retry", action="startup", result=str(exc), attempt=i + 1)
            time.sleep(STARTUP_RETRY_SLEEP)
    raise RuntimeError(f"Could not connect to PostgreSQL: {last_exc}")


# ---------------------------------------------------------------------------
# LISTEN/NOTIFY bridge (asyncpg)
# ---------------------------------------------------------------------------
async def _handle_notify(payload: str) -> None:
    """Forward a pg_notify payload to SSE clients — either subscription or target."""
    db: DatabaseManager = STATE["db"]
    # Check if payload is Sink target JSON
    try:
        sink_payload = json.loads(payload)
        if isinstance(sink_payload, dict) and sink_payload.get("type") == "target":
            target_id = sink_payload["target_id"]
            t = db.get_target(target_id)
            if t is None:
                return
            subs = db.list_target_subscriptions(target_id)
            await _broadcast_sse({
                "type": "target_update",
                "data": _serialise_target(t, subs, db),
            })
            return
    except (json.JSONDecodeError, KeyError):
        pass
    # Legacy subscription notification (sub_id string)
    sub_id = payload
    sub = db.get_subscription(sub_id)
    reg: ManagerPluginRegistry = STATE["registry"]
    if sub is None:
        return
    rec = reg.get(sub.plugin_id)
    password_fields = rec.password_fields if rec else []
    await _broadcast_sse({
        "type": "subscription_update",
        "data": _serialise_subscription(sub, password_fields),
    })


def _serialise_target_subscription(s, db) -> Dict[str, Any]:
    """Serialize a single target-subscription link, enriched with the sub's name."""
    sub_row = db.get_subscription(s.subscription_id)
    return {
        "subscription_id": s.subscription_id,
        "subscription_name": sub_row.name if sub_row else "",
        "plugin_id": sub_row.plugin_id if sub_row else "",
        "status": s.status,
        "last_updated": s.last_updated.isoformat() if s.last_updated else None,
        "last_message": s.last_message,
    }


def _serialise_target(t, subs, db) -> Dict[str, Any]:
    """Serialize a target with its subscriptions and derived status."""
    svc_row = db.get_sink(t.service_id)
    service_name = svc_row.name if svc_row else ""
    service_display_name = service_name
    svc_icon = ""
    sink_reg: SinkRegistry = STATE.get("sink_registry")
    if sink_reg:
        rec = sink_reg.get(service_name)
        if rec:
            svc_icon = rec.icon
            service_display_name = rec.display_name
    if not subs:
        status = "ENABLED"
    elif any(s.status == "ERROR" for s in subs):
        status = "ERROR"
    elif any(s.status in ("IN_PROGRESS", "ENQUEUED") for s in subs):
        status = "IN_PROGRESS"
    elif any(s.status == "ENABLED" for s in subs):
        status = "ENABLED"
    elif any(s.status == "DELETED" for s in subs):
        status = "DELETED"
    else:
        status = "DISABLED"
    last_updated = None
    for s in subs:
        if s.last_updated and (last_updated is None or s.last_updated > last_updated):
            last_updated = s.last_updated
    return {
        "target_id": t.id,
        "service_id": t.service_id,
        "service_name": service_name,
        "service_display_name": service_display_name,
        "service_icon": svc_icon,
        "name": t.name,
        "api_url": t.api_url,
        "has_api_key": bool(t.api_key),
        "remote_target_id": t.remote_target_id,
        "target_extra_params": t.target_extra_params or {},
        "include_path_in_filename": bool(t.include_path_in_filename),
        "schedule_start": t.schedule_start,
        "schedule_end": t.schedule_end,
        "pages_per_batch": t.pages_per_batch,
        "access_level": t.access_level,
        "status": status,
        "last_updated": last_updated.isoformat() if last_updated else None,
        "subscriptions": [
            _serialise_target_subscription(s, db)
            for s in subs
        ],
    }


# ---------------------------------------------------------------------------
# File watcher — debounced hot-swap
# ---------------------------------------------------------------------------
async def _file_watcher() -> None:
    """Debounced hot-swap watcher for plugins and Sink services.

    Identical logic for both directories: track file mtimes, detect
    add/modify/remove, debounce, then reload. Plugins rebuild the
    ManagerPluginRegistry; Sink services rebuild the SinkRegistry and
    upsert ``sink`` rows for any newly added services.
    """
    targets = [("/src/plugins", "registry"), ("/src/sinks", "sink_registry")]
    last_mtimes: Dict[str, float] = {}
    debounce_until: float = 0.0
    pending_change: Optional[str] = None
    while True:
        try:
            await asyncio.sleep(1.0)
            changed = False
            for dir_path, state_key in targets:
                try:
                    entries = os.listdir(dir_path)
                except FileNotFoundError:
                    continue
                for fname in entries:
                    if not fname.endswith(".py") or fname.startswith("."):
                        continue
                    path = os.path.join(dir_path, fname)
                    key = f"{dir_path}/{fname}"
                    try:
                        mtime = os.path.getmtime(path)
                    except FileNotFoundError:
                        continue
                    prev = last_mtimes.get(key)
                    if prev is None or mtime > prev:
                        last_mtimes[key] = mtime
                        changed = True
                        pending_change = fname
                for key in list(last_mtimes.keys()):
                    if key.startswith(f"{dir_path}/") and os.path.basename(key) not in entries:
                        last_mtimes.pop(key, None)
                        changed = True
                        pending_change = os.path.basename(key)
            if changed:
                debounce_until = time.time() + DEBOUNCE_SECONDS
            if pending_change and time.time() >= debounce_until:
                LOG.debug("file_change_detected", action="file_watcher", result=pending_change)
                for dir_path, state_key in targets:
                    reg = STATE[state_key]
                    if state_key == "sink_registry":
                        reg.reload_all()
                        for rec in reg.list_records():
                            try:
                                STATE["db"].upsert_sink(
                                    rec.service_name, rec.metadata.get("description", ""))
                            except Exception as exc:  # noqa: BLE001
                                LOG.warning("sink_service_upsert_failed",
                                            action="file_watcher", service=rec.service_name, error=str(exc))
                    else:
                        reg.reload()
                pending_change = None
                debounce_until = 0.0
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            LOG.error("file_watcher_error", action="file_watcher", result=str(exc))


# ---------------------------------------------------------------------------
# SSE handler
# ---------------------------------------------------------------------------
# The actual SSE generator is defined further down as
# ``_sse_generator_with_queue``; each browser tab gets its own queue so we
# can fan-out events to all clients independently.


# ---------------------------------------------------------------------------
# FastAPI app + routes
# ---------------------------------------------------------------------------
app = FastAPI(title="AutoKB Manager", lifespan=lifespan)

# CORS is not strictly required (Web UI reverse-proxies), but we add it
# to make local debugging easier.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def auth_middleware(request: Request, call_next: Callable[[Request], Awaitable[Any]]):
    # All /api/* routes require the backend key. /health is public.
    if request.url.path.startswith("/api/") and request.url.path != "/api/health":
        require_backend_key(request)
    response = await call_next(request)
    return response


# NOTE: Explicit /api/plugins/* routes are registered FIRST, before the
# catch-all custom-route dispatcher below. FastAPI matches routes in
# registration order, so the catch-all `/api/plugins/{plugin_id}/{path:path}`
# would otherwise intercept requests for the built-in endpoints like
# /api/plugins/{plugin_id}/schema and return "No custom route for ...".
@app.get("/api/plugins")
def api_list_plugins():
    reg: ManagerPluginRegistry = STATE["registry"]
    return reg.list_metadata()


@app.get("/api/plugins/{plugin_id}")
def api_plugin_details(plugin_id: str):
    rec = _plugin_or_404(plugin_id)
    return {
        "plugin_id": rec.plugin_id,
        "name": rec.name,
        "display_name": rec.display_name,
        "icon": rec.icon,
        "description": rec.description,
        "sub_type": rec.sub_type,
    }


@app.get("/api/plugins/{plugin_id}/schema")
def api_plugin_schema(plugin_id: str):
    rec = _plugin_or_404(plugin_id)
    return {
        "plugin_id": rec.plugin_id,
        "schema": rec.augmented_schema,
        "password_fields": rec.password_fields,
    }


@app.get("/api/health")
def api_health():
    db: DatabaseManager = STATE["db"]
    queue: QueueManager = STATE["queue"]
    reg: ManagerPluginRegistry = STATE["registry"]
    db_ok = db.health_check()
    redis_ok = False
    try:
        queue.client.ping()
        redis_ok = True
    except Exception:
        redis_ok = False
    return {
        "status": "ok" if (db_ok and redis_ok and reg) else "degraded",
        "db": db_ok,
        "redis": redis_ok,
        "registry_loaded": bool(reg.list_records()),
        "sink_registry_loaded": bool(STATE.get("sink_registry") and STATE["sink_registry"].list_records()),
    }


@app.api_route("/api/plugins/{plugin_id}/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
async def api_plugin_custom_route(plugin_id: str, path: str, request: Request):
    """Dispatch to a plugin's custom route (defined in ``get_custom_routes()``)."""
    reg: ManagerPluginRegistry = STATE["registry"]
    rec = reg.get(plugin_id)
    if rec is None:
        raise HTTPException(status_code=404, detail=f"Plugin {plugin_id!r} not found")
    method = request.method
    sub_path = "/" + path if not path.startswith("/") else path
    for route in rec.cls().get_custom_routes() or []:
        if route.path == sub_path and (route.method or "GET").upper() == method:
            handler = route.handler
            value = handler()
            import asyncio as _asyncio
            if _asyncio.iscoroutine(value):
                value = await value
            return JSONResponse(content=value)
    raise HTTPException(status_code=404, detail=f"No custom route for {method} {sub_path}")


@app.get("/api/subscriptions")
def api_list_subscriptions(plugin_id: Optional[str] = Query(default=None)):
    db: DatabaseManager = STATE["db"]
    reg: ManagerPluginRegistry = STATE["registry"]
    subs = db.list_subscriptions(plugin_id=plugin_id, include_deleted=False)
    out = []
    for sub in subs:
        rec = reg.get(sub.plugin_id)
        password_fields = rec.password_fields if rec else []
        d = _serialise_subscription(sub, password_fields)
        d["plugin_display_name"] = rec.display_name if rec else sub.plugin_id
        out.append(d)
    return out


@app.post("/api/subscriptions/{plugin_id}")
async def api_create_subscription(plugin_id: str, body: Dict[str, Any] = Body(...)):
    db: DatabaseManager = STATE["db"]
    reg: ManagerPluginRegistry = STATE["registry"]
    rec = _plugin_or_404(plugin_id)

    name = body.get("name")
    name = _validate_subscription_name(name)

    # Output directory collision check
    target_dir = os.path.join("/output", rec.plugin_id, name)
    if os.path.isdir(target_dir):
        raise HTTPException(
            status_code=409,
            detail=f"Output directory already exists for subscription name '{name}'",
        )

    # Pre-validate name uniqueness
    if db.get_subscription_by_name(plugin_id, name):
        raise HTTPException(status_code=409, detail="Subscription name already exists")

    config = body.get("config", {}) or {}
    cron = body.get("cron")
    if not cron:
        cron = "0 0 * * 0" if rec.sub_type == SUB_TYPE_SCHEDULED else "0 0 * * *"
    if cron in ("0 * * * *", "0 0 * * *", "0 0 * * 0"):
        import random as _random
        if cron == "0 * * * *":
            cron = f"{_random.randint(0, 59)} * * * *"
        elif cron == "0 0 * * *":
            cron = f"{_random.randint(0, 59)} {_random.randint(0, 23)} * * *"
        else:
            cron = f"{_random.randint(0, 59)} {_random.randint(0, 23)} * * {_random.randint(0, 6)}"
    if cron and not is_valid_cron(cron):
        raise HTTPException(status_code=400, detail=f"Invalid cron expression: {cron}")

    # Validate config against the plugin schema (full validation incl.
    # required, type, pattern, minLength, etc.)
    from utils.misc_utils import validate_config_against_schema
    try:
        validate_config_against_schema(
            config, rec.augmented_schema, rec.password_fields, enforce_required_password=True
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Config validation failed: {exc}")

    sub = db.create_subscription(
        plugin_id=plugin_id,
        name=name,
        config=config,
        sub_type=rec.sub_type,
        cron=cron,
        description=body.get("description"),
        password_field_names=rec.password_fields,
    )

    # Enqueue
    queue: QueueManager = STATE["queue"]
    db.try_enqueue(sub.id)
    queue.push_primary(sub.id, operation="FULL")
    LOG.info(
        "subscription_created",
        sub_id=sub.id, name=sub.name, plugin_id=plugin_id, action="enqueue", source="manual_trigger",
    )
    # Start the long-running monitor loop for EVENT_BASED subs so the
    # scheduler can invoke monitor() on every iteration. Must be called
    # from the event loop thread (asyncio.create_task inside start_monitor
    # is unsafe from a worker thread), so this handler is async.
    if rec.sub_type == SUB_TYPE_EVENT_BASED:
        coord = STATE["coordinator"]
        coord.start_monitor(sub.id)
    return _serialise_subscription(sub, rec.password_fields)


@app.put("/api/subscriptions/{sub_id}")
async def api_edit_subscription(sub_id: str, body: Dict[str, Any] = Body(...)):
    db: DatabaseManager = STATE["db"]
    reg: ManagerPluginRegistry = STATE["registry"]
    sub = _sub_or_404(sub_id)
    if sub.status == STATE_DELETED:
        raise HTTPException(status_code=400, detail="Cannot edit a DELETED subscription")

    rec = reg.get(sub.plugin_id)
    if rec is None:
        raise HTTPException(status_code=404, detail="Plugin no longer loaded")

    config = body.get("config", sub.config)
    cron = body.get("cron", sub.cron)
    if cron and not is_valid_cron(cron):
        raise HTTPException(status_code=400, detail=f"Invalid cron expression: {cron}")

    # Strip empty password fields before validation so the merge_passwords
    # logic in update_subscription can preserve the existing encrypted values.
    if rec.password_fields:
        config = {k: v for k, v in config.items() if k not in rec.password_fields or v not in (None, "")}

    try:
        validate_config_against_schema(
            config, rec.augmented_schema, rec.password_fields, enforce_required_password=False
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Config validation failed: {exc}")

    sub = db.update_subscription(
        sub_id,
        config=config,
        cron=cron,
        password_field_names=rec.password_fields,
        merge_passwords=True,
    )
    if sub is None:
        raise HTTPException(status_code=400, detail="Could not update subscription")

    # Restart monitor if EVENT_BASED
    if rec.sub_type == SUB_TYPE_EVENT_BASED:
        coord = STATE["coordinator"]
        coord.restart_monitor(sub_id)

    LOG.info("subscription_edited", sub_id=sub_id, name=sub.name, plugin_id=sub.plugin_id)
    return _serialise_subscription(sub, rec.password_fields)


@app.get("/api/subscriptions/activity")
def api_subscriptions_activity_batch(hours: int = 24):
    db: DatabaseManager = STATE["db"]
    return db.count_recent_events_batch(hours=hours)


@app.get("/api/subscriptions/{sub_id}")
def api_get_subscription(sub_id: str):
    db: DatabaseManager = STATE["db"]
    reg: ManagerPluginRegistry = STATE["registry"]
    sub = _sub_or_404(sub_id)
    rec = reg.get(sub.plugin_id)
    password_fields = rec.password_fields if rec else []
    return _serialise_subscription(sub, password_fields)


@app.delete("/api/subscriptions/{sub_id}")
def api_delete_subscription(sub_id: str):
    db: DatabaseManager = STATE["db"]
    queue: QueueManager = STATE["queue"]
    sub = _sub_or_404(sub_id)
    if sub.status == STATE_DELETED:
        raise HTTPException(status_code=409, detail="Subscription already deleted")
    ok = db.delete_subscription(sub_id)
    if not ok:
        raise HTTPException(status_code=409, detail="Subscription already deleted")
    # Push a DELETED cleanup task to the P-Queue
    queue.push_primary(sub_id, operation="FULL")
    # Cancel any running monitor
    coord = STATE["coordinator"]
    coord.cancel_monitor(sub_id)
    # Push a subscription_deleted event directly to SSE
    _schedule_sse_broadcast({
        "type": "subscription_deleted",
        "data": {"id": sub_id, "plugin_id": sub.plugin_id, "name": sub.name},
    })
    LOG.info("subscription_deleted", sub_id=sub_id, name=sub.name, plugin_id=sub.plugin_id)
    return {"ok": True}


@app.post("/api/subscriptions/{sub_id}/trigger")
def api_trigger_subscription(sub_id: str):
    db: DatabaseManager = STATE["db"]
    queue: QueueManager = STATE["queue"]
    sub = _sub_or_404(sub_id)
    if sub.status not in TRIGGERABLE_STATES:
        raise HTTPException(status_code=400, detail=f"Cannot trigger subscription in state {sub.status}")
    db.try_enqueue(sub_id)
    queue.push_primary(sub_id, operation="FULL")
    LOG.debug("subscription_triggered", sub_id=sub_id, name=sub.name, source="manual_trigger")
    return {"ok": True}


def _set_status_db(sub_id: str, new_status: str):
    """Synchronous DB work for set_subscription_status — runs in threadpool."""
    db: DatabaseManager = STATE["db"]
    sub = db.get_subscription(sub_id)
    if not sub:
        raise HTTPException(status_code=404, detail="Subscription not found")
    if sub.status == STATE_DELETED:
        raise HTTPException(status_code=400, detail="Cannot modify a DELETED subscription")
    if new_status == STATE_ENABLED and sub.cron and not is_valid_cron(sub.cron):
        raise HTTPException(status_code=400, detail=f"Invalid cron expression: {sub.cron}")
    ok, err = db.set_subscription_status(sub_id, new_status)
    if not ok:
        raise HTTPException(status_code=400, detail=err or "Could not change status")
    return sub


@app.put("/api/subscriptions/{sub_id}/status")
async def api_set_status(sub_id: str, body: Dict[str, Any] = Body(...)):
    new_status = body.get("status")
    if new_status not in (STATE_ENABLED, STATE_DISABLED):
        raise HTTPException(status_code=400, detail="status must be ENABLED or DISABLED")
    sub = await asyncio.to_thread(_set_status_db, sub_id, new_status)
    # Manage monitor loop for EVENT_BASED subs (must be on event loop thread
    # because start_monitor uses asyncio.create_task).
    reg: ManagerPluginRegistry = STATE["registry"]
    rec = reg.get(sub.plugin_id)
    if rec and rec.sub_type == SUB_TYPE_EVENT_BASED:
        coord = STATE["coordinator"]
        if new_status == STATE_ENABLED:
            coord.start_monitor(sub_id)
        else:
            coord.cancel_monitor(sub_id)
    return {"ok": True, "status": new_status}


@app.get("/api/subscriptions/{sub_id}/activity")
def api_subscription_activity(sub_id: str, hours: int = 24):
    db: DatabaseManager = STATE["db"]
    return {"subscription_id": sub_id, "count": db.count_recent_events(sub_id, hours=hours)}


@app.get("/api/logging")
def api_logging():
    db: DatabaseManager = STATE["db"]
    # 100k-row safety cap. With ~670 events/hour from the test suite this
    # covers ~6 days of history, which is "all" in practice; the cap
    # prevents a runaway plugin from blowing up the JSON payload. The
    # Recent Activity page renders all rows without virtualization (§7.11
    # has client-side virtualization on the future-work backlog), so a
    # smaller cap would be a UI problem long before it is a wire problem.
    rows = db.list_event_log(limit=100000)
    reg: ManagerPluginRegistry = STATE["registry"]
    return [
        {
            "id": e.id,
            "subscription_id": e.subscription_id,
            "subscription_name": name,
            "plugin_id": plugin_id,
            "plugin_display_name": _plugin_display_name(plugin_id),
            "executed_at": e.executed_at.isoformat(),
            "exit_code": e.exit_code,
            "exit_string": e.exit_string,
        }
        for e, name, plugin_id in rows
    ]


@app.delete("/api/logging")
def api_clear_logging():
    db: DatabaseManager = STATE["db"]
    n = db.clear_event_log()
    return {"deleted": n}


# ---------------------------------------------------------------------------
# Dev Lab endpoints
# ---------------------------------------------------------------------------
@app.post("/api/dev_lab/validate")
def api_dev_lab_validate(body: Dict[str, Any] = Body(...)):
    code = body.get("code", "")
    plugin_name = body.get("name", "")
    if not plugin_name:
        raise HTTPException(status_code=400, detail="Plugin name is required")
    if len(plugin_name) > MAX_PLUGIN_NAME_LEN:
        raise HTTPException(
            status_code=400,
            detail=f"Plugin name too long: {len(plugin_name)} chars (max {MAX_PLUGIN_NAME_LEN})",
        )
    _require_display_name(body)
    return _validate_plugin_code(code, plugin_name)


@app.get("/api/dev_lab/load/{plugin_id}")
def api_dev_lab_load(plugin_id: str):
    """Return the on-disk source code of an existing plugin for the Edit Plugin flow.

    Used by the Developer Lab to pre-populate the form when the user clicks
    the yellow "Edit Plugin" button on a plugin's subscription list (see
    DesignSpecification §7.5 and §7.9). The returned ``code`` is the raw
    contents of ``/src/plugins/{plugin_id}.py``; the registry's
    ``file_path`` is the source of truth.
    """
    reg: ManagerPluginRegistry = STATE["registry"]
    rec = _plugin_or_404(plugin_id)
    try:
        with open(rec.file_path, "r", encoding="utf-8") as f:
            code = f.read()
    except FileNotFoundError:
        raise HTTPException(
            status_code=404,
            detail=f"Plugin source file not found: {rec.file_path}",
        )
    return {"ok": True, "name": rec.plugin_id, "display_name": rec.display_name, "code": code}


@app.post("/api/dev_lab/save")
def api_dev_lab_save(body: Dict[str, Any] = Body(...)):
    code = body.get("code", "")
    plugin_name = body.get("name", "")
    icon_b64 = body.get("icon_base64")
    if not plugin_name:
        raise HTTPException(status_code=400, detail="Plugin name is required")
    if len(plugin_name) > MAX_PLUGIN_NAME_LEN:
        raise HTTPException(
            status_code=400,
            detail=f"Plugin name too long: {len(plugin_name)} chars (max {MAX_PLUGIN_NAME_LEN})",
        )
    display_name = _require_display_name(body)
    result = _validate_plugin_code(code, plugin_name)
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=result.get("error", "Validation failed"))
    sanitized = result["plugin_id"]
    code = _set_metadata_display_name_in_source(code, display_name, "BaseSubscription")
    target_path = f"/src/plugins/{sanitized}.py"
    tmp_path = f"/tmp/.{sanitized}.py.tmp"
    with open(tmp_path, "w") as f:
        f.write(code)
    # Final import sanity check + compute the new schema hash.
    # (Inlined rather than a helper so the temp-file cleanup and the
    # HTTPException-raising control flow stay close to the write site.)
    try:
        import importlib.util
        import sys as _sys
        if "/src" not in _sys.path:
            _sys.path.insert(0, "/src")
        from importlib.machinery import SourceFileLoader
        loader = SourceFileLoader(f"_dev_{sanitized}", tmp_path)
        spec = importlib.util.spec_from_loader(f"_dev_{sanitized}", loader)
        if spec is None or spec.loader is None:
            raise ValueError(f"Could not build spec for {tmp_path}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        new_cls = _find_plugin_class_in_module(module)
        if new_cls is None:
            raise ValueError("No BaseSubscription subclass found in saved code")
        new_hash = schema_hash(new_cls().get_schema())
    except Exception as exc:  # noqa: BLE001
        try:
            os.remove(tmp_path)
        except FileNotFoundError:
            pass
        import traceback as _tb
        raise HTTPException(
            status_code=400,
            detail=f"Validation failed: {type(exc).__name__}: {exc} | {_tb.format_exc()[-500:]}",
        )

    # Edit Plugin mode: if a plugin with this id is already loaded, the
    # new schema must match the existing one EXACTLY (see DesignSpecification
    # §7.9 "Edit Plugin mode — schema-stability invariant"). The check is
    # performed BEFORE the atomic rename so a rejected edit leaves the
    # on-disk file and the in-memory registry completely untouched.
    #
    # The DB is the source of truth for the stored schema hash, not the
    # in-memory registry — the latter can be empty if the watcher refused
    # to load the file (e.g. a previously-broken file that the operator
    # is now trying to fix), in which case the in-memory check would
    # spuriously allow a schema-breaking save. We prefer the DB hash when
    # present, and fall back to the in-memory record only when the DB has
    # no entry.
    reg: ManagerPluginRegistry = STATE["registry"]
    db: DatabaseManager = STATE["db"]
    db_state = db.get_plugin_state(sanitized)
    existing_hash: Optional[str] = None
    if db_state is not None:
        existing_hash = db_state.schema_hash
    elif reg.get(sanitized) is not None:
        existing_hash = reg.get(sanitized).schema_hash_value
    mode = "edit" if existing_hash is not None else "create"
    if existing_hash is not None and existing_hash != new_hash:
        try:
            os.remove(tmp_path)
        except FileNotFoundError:
            pass
        raise HTTPException(
            status_code=400,
            detail=(
                f"Cannot edit existing plugin {sanitized!r}: config (schema) has changed. "
                f"Existing hash: {existing_hash[:12]}, new hash: {new_hash[:12]}. "
                f"Editing an existing plugin requires the config to remain identical. "
                f"To change the config, create a new plugin."
            ),
        )

    # Atomic move — os.replace fails with EXDEV if /tmp and the target dir
    # are on different filesystems, so fall back to a copy+remove.
    import shutil
    try:
        os.replace(tmp_path, target_path)
    except OSError:
        shutil.copyfile(tmp_path, target_path)
        try:
            os.remove(tmp_path)
        except FileNotFoundError:
            pass
    # NOTE: We intentionally do NOT touch the plugin state hash here. The
    # watcher's reload will compare the new file content to the stored
    # hash; in Edit Plugin mode the hashes are guaranteed equal (we just
    # checked), so the reload completes without a breaking-change error.
    # Save icon if provided
    if icon_b64:
        import base64
        try:
            icon_bytes = base64.b64decode(icon_b64)
            icon_path = f"/assets/{sanitized}.png"
            with open(icon_path, "wb") as f:
                f.write(icon_bytes)
        except Exception:
            pass
    return {"ok": True, "path": target_path, "mode": mode, "plugin_id": sanitized}


# ---------------------------------------------------------------------------
# Plugin management
# ---------------------------------------------------------------------------
@app.delete("/api/plugins/{plugin_id}")
def api_delete_plugin(plugin_id: str):
    db: DatabaseManager = STATE["db"]
    reg: ManagerPluginRegistry = STATE["registry"]
    rec = _plugin_or_404(plugin_id)
    count = db.count_subscriptions_for_plugin(plugin_id)
    if count > 0:
        raise HTTPException(
            status_code=409,
            detail=f"Cannot delete plugin with {count} active subscriptions",
        )
    # Delete the file, the in-memory record, and the DB state
    try:
        os.remove(rec.file_path)
    except FileNotFoundError:
        pass
    try:
        db.delete_plugin_state(plugin_id)
    except Exception:
        pass
    # Remove from registry
    reg.records.pop(plugin_id, None)
    # Remove output directory
    out_dir = f"/output/{plugin_id}"
    import shutil
    if os.path.isdir(out_dir):
        shutil.rmtree(out_dir, ignore_errors=True)
    return {"ok": True}


# ---------------------------------------------------------------------------
# SSE endpoint
# ---------------------------------------------------------------------------
@app.get("/api/events")
async def api_events():
    # Per-client queue; each browser tab gets its own. Fan-out broadcasts
    # write to every queue in STATE["sse_clients"].
    client_queue: asyncio.Queue = asyncio.Queue(maxsize=1000)
    sse_clients: Set[asyncio.Queue] = STATE.setdefault("sse_clients", set())
    sse_clients.add(client_queue)
    LOG.debug("sse_client_connected", action="sse", result="ok", total_clients=len(sse_clients))

    async def gen():
        try:
            async for chunk in sse_generator(STATE["db"], STATE["registry"], client_queue, _serialise_subscription):
                yield chunk
        finally:
            sse_clients.discard(client_queue)
            LOG.debug("sse_client_disconnected", action="sse", result="ok", total_clients=len(sse_clients))

    return StreamingResponse(gen(), media_type="text/event-stream")


# ---------------------------------------------------------------------------
# Sink Dev Lab endpoints
# ---------------------------------------------------------------------------

@app.post("/api/sink_dev_lab/validate")
def api_sink_dev_lab_validate(body: Dict[str, Any] = Body(...)):
    code = body.get("code", "")
    service_name = body.get("name", "")
    if not service_name:
        raise HTTPException(status_code=400, detail="Sink service name is required")
    if len(service_name) > MAX_PLUGIN_NAME_LEN:
        raise HTTPException(
            status_code=400,
            detail=f"Sink service name too long: {len(service_name)} chars (max {MAX_PLUGIN_NAME_LEN})",
        )
    _require_display_name(body)
    return _validate_sink_code(code, service_name)


@app.get("/api/sink_dev_lab/load/{service_name}")
def api_sink_dev_lab_load(service_name: str):
    """Return the on-disk source code of an existing Sink service for the
    Edit Destination flow. Mirrors ``api_dev_lab_load`` for plugins."""
    sink_reg: SinkRegistry = STATE.get("sink_registry")
    rec = sink_reg.get(service_name) if sink_reg is not None else None
    if rec is None:
        raise HTTPException(status_code=404, detail=f"Sink service {service_name!r} not found")
    try:
        with open(rec.file_path, "r", encoding="utf-8") as f:
            code = f.read()
    except FileNotFoundError:
        raise HTTPException(
            status_code=404,
            detail=f"Sink service source file not found: {rec.file_path}",
        )
    return {"ok": True, "name": rec.service_name, "display_name": rec.display_name, "code": code}


@app.post("/api/sink_dev_lab/save")
def api_sink_dev_lab_save(body: Dict[str, Any] = Body(...)):
    code = body.get("code", "")
    service_name = body.get("name", "")
    icon_b64 = body.get("icon_base64")
    if not service_name:
        raise HTTPException(status_code=400, detail="Sink service name is required")
    if len(service_name) > MAX_PLUGIN_NAME_LEN:
        raise HTTPException(
            status_code=400,
            detail=f"Sink service name too long: {len(service_name)} chars (max {MAX_PLUGIN_NAME_LEN})",
        )
    display_name = _require_display_name(body)
    result = _validate_sink_code(code, service_name)
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=result.get("error", "Validation failed"))
    sanitized = result["service_name"]
    code = _set_metadata_display_name_in_source(code, display_name, "BaseSink")
    target_path = f"/src/sinks/{sanitized}.py"
    tmp_path = f"/tmp/.{sanitized}.py.tmp"
    with open(tmp_path, "w") as f:
        f.write(code)
    # Final import sanity check.
    try:
        import importlib.util
        import sys as _sys
        if "/src" not in _sys.path:
            _sys.path.insert(0, "/src")
        from importlib.machinery import SourceFileLoader
        loader = SourceFileLoader(f"_sink_dev_{sanitized}", tmp_path)
        spec = importlib.util.spec_from_loader(f"_sink_dev_{sanitized}", loader)
        if spec is None or spec.loader is None:
            raise ValueError(f"Could not build spec for {tmp_path}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        new_cls = _find_sink_class_in_module(module)
        if new_cls is None:
            raise ValueError("No BaseSink subclass found in saved code")
        if getattr(new_cls, "__abstractmethods__", None):
            raise ValueError(f"Sink service class {new_cls.__name__} is abstract; implement all abstract methods")
    except Exception as exc:  # noqa: BLE001
        try:
            os.remove(tmp_path)
        except FileNotFoundError:
            pass
        import traceback as _tb
        raise HTTPException(
            status_code=400,
            detail=f"Validation failed: {type(exc).__name__}: {exc} | {_tb.format_exc()[-500:]}",
        )

    mode = "edit" if os.path.isfile(target_path) else "create"

    # Atomic move — os.replace fails with EXDEV if /tmp and the target dir
    # are on different filesystems, so fall back to a copy+remove.
    import shutil
    try:
        os.replace(tmp_path, target_path)
    except OSError:
        shutil.copyfile(tmp_path, target_path)
        try:
            os.remove(tmp_path)
        except FileNotFoundError:
            pass

    # Save icon if provided
    if icon_b64:
        import base64
        try:
            icon_bytes = base64.b64decode(icon_b64)
            icon_path = f"/assets/{sanitized}.png"
            with open(icon_path, "wb") as f:
                f.write(icon_bytes)
        except Exception:
            pass
    return {"ok": True, "path": target_path, "mode": mode, "service_name": sanitized}


# ---------------------------------------------------------------------------
# Sink / Target API endpoints
# ---------------------------------------------------------------------------

@app.get("/api/sinks")
def api_list_sinks():
    db: DatabaseManager = STATE["db"]
    sink_reg: SinkRegistry = STATE.get("sink_registry")
    services = db.list_sinks()
    result = []
    for svc in services:
        targets = db.list_targets(service_id=svc.id)
        icon = ""
        display_name = svc.name or ""
        defaults = {"api_url": "", "has_api_key_default": False}
        if sink_reg:
            rec = sink_reg.get(svc.name)
            if rec:
                icon = rec.icon
                display_name = rec.display_name
                try:
                    defaults = rec.cls.get_defaults()
                except Exception:
                    pass
        result.append({
            "service_id": svc.id,
            "name": svc.name,
            "display_name": display_name,
            "description": svc.description or "",
            "icon": icon,
            "default_api_url": defaults.get("api_url", ""),
            "has_api_key_default": defaults.get("has_api_key_default", False),
            "target_count": len(targets),
        })
    return result


@app.delete("/api/sinks/{service_id}")
def api_delete_sink(service_id: str):
    """Delete a Sink service. Mirrors ``api_delete_plugin``: only allowed
    when the service has zero attached targets. Removes the file, the
    in-memory record, and the DB row."""
    db: DatabaseManager = STATE["db"]
    sink_reg: SinkRegistry = STATE.get("sink_registry")
    svc = db.get_sink(service_id)
    if svc is None:
        raise HTTPException(status_code=404, detail="Sink not found")
    targets = db.list_targets(service_id=service_id)
    if targets:
        raise HTTPException(
            status_code=409,
            detail=f"Cannot delete Sink with {len(targets)} attached target(s)",
        )
    # Delete the file, the in-memory record, and the DB row
    if sink_reg is not None:
        rec = sink_reg.get(svc.name)
        if rec is not None:
            try:
                os.remove(rec.file_path)
            except FileNotFoundError:
                pass
    try:
        db.delete_sink(service_id)
    except Exception:
        pass
    if sink_reg is not None:
        sink_reg.records.pop(svc.name, None)
    # Remove output directory
    import shutil
    out_dir = f"/output/{svc.name}"
    if os.path.isdir(out_dir):
        shutil.rmtree(out_dir, ignore_errors=True)
    return {"ok": True}


@app.get("/api/sinks/{service_id}/targets")
def api_list_sink_targets(service_id: str):
    db: DatabaseManager = STATE["db"]
    t_list = db.list_targets(service_id=service_id)
    svc_row = db.get_sink(service_id)
    service_name = svc_row.name if svc_row else ""
    result = []
    for t in t_list:
        subs = db.list_target_subscriptions(t.id)
        result.append(_serialise_target(t, subs, db))
    return result


@app.get("/api/targets")
def api_list_targets():
    db: DatabaseManager = STATE["db"]
    all_t = db.list_targets()
    result = []
    for t in all_t:
        subs = db.list_target_subscriptions(t.id)
        result.append(_serialise_target(t, subs, db))
    return result


@app.get("/api/targets/{target_id}")
def api_target_detail(target_id: str):
    db: DatabaseManager = STATE["db"]
    t = db.get_target(target_id)
    if t is None:
        raise HTTPException(status_code=404, detail="Target not found")
    subs = db.list_target_subscriptions(target_id)
    # Enrich with subscription names
    enriched = []
    for s in subs:
        sub_row = db.get_subscription(s.subscription_id)
        enriched.append({
            "subscription_id": s.subscription_id,
            "subscription_name": sub_row.name if sub_row else "",
            "plugin_id": sub_row.plugin_id if sub_row else "",
            "status": s.status,
            "last_updated": s.last_updated.isoformat() if s.last_updated else None,
            "last_message": s.last_message,
        })
    data = _serialise_target(t, subs, db)
    data["subscriptions"] = enriched
    return data


@app.post("/api/sinks/{service_id}/targets")
def api_create_target(service_id: str, body: Dict[str, Any] = Body(...)):
    db: DatabaseManager = STATE["db"]
    queue: QueueManager = STATE["queue"]
    svc = db.get_sink(service_id)
    if svc is None:
        raise HTTPException(status_code=404, detail="Sink not found")
    name = body.get("name", "").strip()
    _validate_target_name(name)
    api_url = body.get("api_url", "").strip()
    if not api_url:
        raise HTTPException(status_code=400, detail="api_url is required")
    api_key = body.get("api_key", "").strip()
    t_extra = body.get("target_extra_params", {})
    if isinstance(t_extra, str):
        try:
            t_extra = json.loads(t_extra)
        except json.JSONDecodeError:
            raise HTTPException(status_code=400, detail="target_extra_params must be valid JSON")
    sub_ids = body.get("subscription_ids", [])
    if not isinstance(sub_ids, list):
        raise HTTPException(status_code=400, detail="subscription_ids must be a list")
    include_path = body.get("include_path_in_filename", False)
    if not isinstance(include_path, bool):
        raise HTTPException(status_code=400, detail="include_path_in_filename must be a boolean")
    schedule_start = body.get("schedule_start")
    schedule_end = body.get("schedule_end")
    _validate_schedule_times(schedule_start, schedule_end)
    pages_per_batch = _validate_pages_per_batch(body.get("pages_per_batch"))
    access_level = _validate_access_level(body.get("access_level"))

    t = db.create_target(service_id, name, api_url, api_key, t_extra,
                         include_path_in_filename=include_path,
                         schedule_start=schedule_start,
                         schedule_end=schedule_end,
                         pages_per_batch=pages_per_batch,
                         access_level=access_level)
    # Provision the remote resource synchronously before any queue item is
    # pushed — recon must never create the remote target itself.
    try:
        _ensure_target_remote(t, db, STATE.get("sink_registry"), LOG)
    except HTTPException:
        db.delete_target_row(t.id)
        raise
    t = db.get_target(t.id)
    if sub_ids:
        db.link_target_subscriptions(t.id, sub_ids, status="ENQUEUED")
        for sid in sub_ids:
            queue.push_primary(sid, operation="SINK_ONLY")

    subs = db.list_target_subscriptions(t.id)
    return _serialise_target(t, subs, db)


@app.put("/api/targets/{target_id}")
def api_update_target(target_id: str, body: Dict[str, Any] = Body(...)):
    db: DatabaseManager = STATE["db"]
    queue: QueueManager = STATE["queue"]
    t = db.get_target(target_id)
    if t is None:
        raise HTTPException(status_code=404, detail="Target not found")
    api_url = body.get("api_url")
    api_key = body.get("api_key")
    t_extra = body.get("target_extra_params")

    if isinstance(t_extra, str):
        try:
            t_extra = json.loads(t_extra)
        except json.JSONDecodeError:
            raise HTTPException(status_code=400, detail="target_extra_params must be valid JSON")
    include_path = body.get("include_path_in_filename")
    if include_path is not None and not isinstance(include_path, bool):
        raise HTTPException(status_code=400, detail="include_path_in_filename must be a boolean")
    schedule_start = body.get("schedule_start")
    schedule_end = body.get("schedule_end")
    if schedule_start is not None or schedule_end is not None:
        _validate_schedule_times(schedule_start, schedule_end)
    pages_per_batch = (
        _validate_pages_per_batch(body.get("pages_per_batch"))
        if "pages_per_batch" in body else None
    )
    access_level = (
        _validate_access_level(body.get("access_level"))
        if "access_level" in body else None
    )

    t = db.update_target(target_id, api_url=api_url, api_key=api_key,
                         target_extra_params=t_extra,
                         include_path_in_filename=include_path,
                         schedule_start=schedule_start,
                         schedule_end=schedule_end,
                         pages_per_batch=pages_per_batch,
                         access_level=access_level)
    # Provision the remote resource synchronously before any queue item is
    # pushed (no-op when remote_target_id is already set).
    _ensure_target_remote(t, db, STATE.get("sink_registry"), LOG)

    # Diff subscriptions
    new_sub_ids = body.get("subscription_ids", [])
    if not isinstance(new_sub_ids, list):
        raise HTTPException(status_code=400, detail="subscription_ids must be a list")

    current_subs = db.list_target_subscriptions(target_id)
    current_ids = {s.subscription_id for s in current_subs}
    new_ids = set(new_sub_ids)

    added = new_ids - current_ids
    removed = current_ids - new_ids

    if added:
        db.link_target_subscriptions(target_id, list(added), status="ENQUEUED")
    if removed:
        db.set_target_subscriptions_status(target_id, list(removed), status="DELETED")

    # Enqueue ALL related sub_ids
    all_related = current_ids | new_ids
    for sid in all_related:
        queue.push_primary(sid, operation="SINK_ONLY")

    subs = db.list_target_subscriptions(target_id)
    return _serialise_target(t, subs, db)


@app.delete("/api/targets/{target_id}")
def api_delete_target(target_id: str, force: bool = False):
    db: DatabaseManager = STATE["db"]
    t = db.get_target(target_id)
    if t is None:
        raise HTTPException(status_code=404, detail="Target not found")
    sink_reg: SinkRegistry = STATE.get("sink_registry")
    if force:
        # Best-effort: try remote removal, then delete local records regardless.
        try:
            _remove_orphan_target(target_id, db, sink_reg, LOG)
        except Exception as exc:
            LOG.warning("target_force_delete_remote_failed", target_id=target_id, error=str(exc))
        db.delete_target_subscriptions_for_target(target_id)
    else:
        # Strict: remote dataset must be removed first, else retain everything.
        try:
            _remove_remote_target_strict(target_id, db, sink_reg, LOG)
        except Exception as exc:
            LOG.warning("target_delete_remote_failed", target_id=target_id, error=str(exc))
            raise HTTPException(
                status_code=502,
                detail=f"Remote dataset deletion failed; target retained: {exc}",
            )
        db.delete_target_datafiles_for_target(target_id)
        db.delete_target_subscriptions_for_target(target_id)
        db.delete_target_row(target_id)
    _schedule_sse_broadcast({
        "type": "target_deleted",
        "data": {"target_id": target_id, "service_id": t.service_id},
    })
    return {"deleted": True}


@app.post("/api/targets/{target_id}/update")
def api_trigger_target_update(target_id: str):
    db: DatabaseManager = STATE["db"]
    queue: QueueManager = STATE["queue"]
    subs = db.list_target_subscriptions(target_id)
    sub_ids = list({s.subscription_id for s in subs})
    for sid in sub_ids:
        queue.push_primary(sid, operation="SINK_ONLY")
    return {"enqueued": len(sub_ids)}


@app.post("/api/targets/{target_id}/status")
def api_set_target_status(target_id: str, body: Dict[str, Any] = Body(...)):
    db: DatabaseManager = STATE["db"]
    queue: QueueManager = STATE["queue"]
    t = db.get_target(target_id)
    if t is None:
        raise HTTPException(status_code=404, detail="Target not found")
    new_status = body.get("status", "").upper()
    if new_status not in ("ENABLED", "DISABLED"):
        raise HTTPException(status_code=400, detail="status must be ENABLED or DISABLED")
    subs = db.list_target_subscriptions(target_id)
    sub_ids = [s.subscription_id for s in subs]
    if sub_ids:
        db.set_target_subscriptions_status(target_id, sub_ids, status=new_status)
        for sid in sub_ids:
            queue.push_primary(sid, operation="SINK_ONLY")
    return {"updated": len(sub_ids)}


@app.post("/api/targets/{target_id}/subscriptions/{subscription_id}/status")
def api_set_target_subscription_status(target_id: str, subscription_id: str, body: Dict[str, Any] = Body(...)):
    """Enable/disable a single target-subscription link."""
    db: DatabaseManager = STATE["db"]
    queue: QueueManager = STATE["queue"]
    link = db.get_target_subscription(target_id, subscription_id)
    if link is None:
        raise HTTPException(status_code=404, detail="Subscription not linked to this target")
    new_status = body.get("status", "").upper()
    if new_status not in ("ENABLED", "DISABLED"):
        raise HTTPException(status_code=400, detail="status must be ENABLED or DISABLED")
    db.set_target_subscription_status(target_id, subscription_id, status=new_status)
    queue.push_primary(subscription_id, operation="SINK_ONLY")
    return {"updated": 1}


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("MANAGER_PORT", "80"))
    log_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "default": {
                "()": "logging.Formatter",
                "fmt": "%(asctime)s.%(msecs)03d [%(levelname)s] - %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
        },
        "handlers": {
            "default": {
                "formatter": "default",
                "class": "logging.StreamHandler",
                "stream": "ext://sys.stderr",
            },
        },
        "loggers": {
            "uvicorn": {"handlers": ["default"], "level": "INFO", "propagate": False},
            "uvicorn.error": {"level": "INFO"},
        },
    }
    uvicorn.run("manager.manager:app", host="0.0.0.0", port=port,
                log_level="info", access_log=False, log_config=log_config)
