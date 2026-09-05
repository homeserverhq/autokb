"""Long-running Manager background service loops.

Watchdog, the asyncpg LISTEN/NOTIFY bridge, and the SSE snapshot generator.
The Manager wires them up with explicit dependencies so the loops stay
self-contained and testable (extracted from the Manager monolith).
"""

import asyncio
import json
import time
import traceback

from utils.constants import (
    SSE_KEEPALIVE_SECONDS,
    STATE_ERROR,
    WATCHDOG_INTERVAL,
    WATCHDOG_TIMEOUT_S,
)
from utils.misc_utils import send_smtp_notification


async def run_watchdog(db, queue, log, smtp_config) -> None:
    """Force-release stale safety locks and mark stuck runs ERROR.

    Only ``IN_PROGRESS`` subscriptions with a stale heartbeat are considered
    (ENQUEUED rows waiting on a free worker are left alone).
    """
    while True:
        try:
            await asyncio.sleep(WATCHDOG_INTERVAL)
            stale = db.list_stale_in_progress(WATCHDOG_TIMEOUT_S)
            for row in stale:
                sub_id = row[0]
                sub = db.get_subscription(sub_id)
                if not sub:
                    continue
                log.warning(
                    "watchdog_force_releasing",
                    sub_id=sub_id,
                    name=sub.name,
                    action="watchdog",
                    result="releasing_lock",
                )
                queue.force_release_lock(sub_id)
                rc = db.update_status(
                    sub_id, STATE_ERROR, last_error="Watchdog: no heartbeat", guard="success_to_enabled"
                )
                if rc:
                    log.warning(
                        "watchdog_marked_error",
                        sub_id=sub_id,
                        name=sub.name,
                        action="watchdog",
                        result="error",
                    )
                    try:
                        send_smtp_notification(
                            subject=f"[AutoKB] Watchdog: {sub.name}",
                            body=(
                                f"Watchdog force-released lock for subscription {sub.name!r} (id={sub_id}).\n"
                                "No heartbeat was received within the watchdog timeout."
                            ),
                            **smtp_config,
                        )
                    except Exception:
                        pass
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            log.error("watchdog_error", action="watchdog", result=str(exc), traceback=traceback.format_exc())


async def run_notify_bridge(database_url: str, channel: str, log, on_payload) -> None:
    """Connect to Postgres via asyncpg and forward ``pg_notify`` payloads.

    ``on_payload(payload)`` is an async callable invoked for every notification
    on ``channel``. The connection is kept alive, with exponential backoff on
    disconnects.
    """
    import asyncpg
    backoff = 1.0
    while True:
        try:
            conn = await asyncpg.connect(database_url)
            await conn.add_listener(
                channel,
                lambda _c, _pid, _ch, payload: asyncio.create_task(on_payload(payload)),
            )
            log.info("listening_started", action="asyncpg_listen", result="ok", channel=channel)
            backoff = 1.0
            # Keep the connection alive
            while True:
                await asyncio.sleep(60)
                await conn.execute("SELECT 1")
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            log.warning("listen_disconnected", action="asyncpg_listen", result=str(exc))
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30.0)


async def sse_generator(db, reg, client_queue, serialise_subscription):
    """Yield SSE frames for one client: a full snapshot, then live events.

    Snapshot items are yielded directly (never enqueued up front into the
    bounded per-client queue) so any number of subscriptions streams without
    deadlocking. ``serialise_subscription(sub, password_fields)`` decouples the
    generator from the Manager's serializers.
    """
    snapshot = []
    for sub in db.list_subscriptions(include_deleted=False):
        rec = reg.get(sub.plugin_id)
        password_fields = rec.password_fields if rec else []
        d = serialise_subscription(sub, password_fields)
        d["plugin_display_name"] = rec.display_name if rec else sub.plugin_id
        snapshot.append({"type": "subscription_update", "data": d})
    # Sentinel so the client can detect the snapshot is complete.
    snapshot.append({"type": "snapshot_complete"})
    last_keepalive = time.time()
    for ev in snapshot:
        yield f"data: {json.dumps(ev)}\n\n"
        last_keepalive = time.time()
    try:
        while True:
            try:
                ev = await asyncio.wait_for(client_queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                if time.time() - last_keepalive > SSE_KEEPALIVE_SECONDS:
                    yield ":keepalive\n\n"
                    last_keepalive = time.time()
                continue
            yield f"data: {json.dumps(ev)}\n\n"
            last_keepalive = time.time()
    except asyncio.CancelledError:
        return