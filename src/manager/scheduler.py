"""Trigger coordinator: cron + EVENT_BASED monitor loops.

Runs inside the Manager process as an asyncio task. Responsibilities:

* Periodically scan SCHEDULED subscriptions and push them to the P-Queue
  when their cron expression is due.
* Maintain an ``asyncio.Task`` per EVENT_BASED subscription that calls
  the plugin's ``monitor()`` coroutine. Return value ``True`` enqueues
  a run. Exceptions are logged and retried after ``MONITOR_ERROR_SLEEP``.
* On Manager startup, recover subscriptions stuck in ``IN_PROGRESS``,
  ``ENQUEUED``, or ``DELETED`` (re-enqueue them) and start monitors for
  any active ``EVENT_BASED`` subscriptions.
"""

import asyncio
import time
import traceback
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, List, Optional, Set

from utils.constants import (
    DEBOUNCE_PHASE_SECONDS,
    MONITOR_ERROR_SLEEP,
    NOTIFY_CHANNEL,
    P_QUEUE_KEY,
    STATE_DELETED,
    STATE_DISABLED,
    STATE_ENABLED,
    STATE_ENQUEUED,
    STATE_IN_PROGRESS,
    STATE_ERROR,
    SUB_TYPE_EVENT_BASED,
    SUB_TYPE_SCHEDULED,
    TRIGGERABLE_STATES,
    WATCHDOG_TIMEOUT_S,
    ACCESS_PRIVATE,
)
from utils.database import DatabaseManager, EventLog, Subscription
from utils.misc_utils import (
    cron_due,
    get_logger,
    is_valid_cron,
    send_smtp_notification,
)
from utils.queue_utils import QueueManager
from utils.registry import PluginRecord

from manager.registry import ManagerPluginRegistry


LOG_FILE = "/logs/manager.log"


def _same_minute(a: datetime, b: datetime) -> bool:
    """True iff two datetimes fall in the same UTC minute bucket.

    Used to debounce cron fallback paths so a cron that matches the
    current minute (e.g. ``* * * * *``) fires at most once per minute,
    not once per monitor tick.
    """
    # Normalize both to UTC. ``a`` may come back from the DB in a
    # local timezone (e.g. -05:00) while ``b`` is timezone.utc; if we
    # compared raw .hour fields we'd say 06:45 EST != 11:45 UTC even
    # though they are the same instant.
    if a.tzinfo is None:
        a = a.replace(tzinfo=timezone.utc)
    else:
        a = a.astimezone(timezone.utc)
    if b.tzinfo is None:
        b = b.replace(tzinfo=timezone.utc)
    else:
        b = b.astimezone(timezone.utc)
    return (a.year == b.year and a.month == b.month and a.day == b.day
            and a.hour == b.hour and a.minute == b.minute)


class TriggerCoordinator:
    """Coordinates cron triggers and event-based monitor loops."""

    def __init__(self, db: DatabaseManager, queue: QueueManager,
                 registry: ManagerPluginRegistry, smtp_config: Optional[Dict[str, Any]] = None):
        self._db = db
        self._queue = queue
        self._registry = registry
        self._log = get_logger("scheduler", LOG_FILE)
        self._smtp = smtp_config or {}
        self._monitor_tasks: Dict[str, asyncio.Task] = {}
        self._monitor_cancel_events: Dict[str, asyncio.Event] = {}
        self._stopped = False

    # ----- lifecycle -----
    async def run(self) -> None:
        # Startup recovery
        self._startup_recovery()
        # Main loop
        while not self._stopped:
            try:
                await self._tick()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                self._log.error("scheduler_tick_error", action="tick", result=str(exc),
                                traceback=traceback.format_exc())
            await asyncio.sleep(1.0)

    def _startup_recovery(self) -> None:
        stuck = self._db.list_stuck_in_flight()
        in_progress = 0
        enqueued = 0
        deleted = 0
        for sub in stuck:
            if sub.status == STATE_IN_PROGRESS:
                in_progress += 1
            elif sub.status == STATE_ENQUEUED:
                enqueued += 1
            elif sub.status == STATE_DELETED:
                deleted += 1
            # Re-enqueue (P-Queue)
            self._db.try_enqueue(sub.id)
            self._queue.push_primary(sub.id)
        if stuck:
            self._log.info(
                "startup_recovery",
                action="recovery",
                result=f"recovered={len(stuck)} in_progress={in_progress} enqueued={enqueued} deleted={deleted}",
            )
        # Validate cron expressions
        for sub in self._db.list_subscriptions(include_deleted=True):
            if sub.status == STATE_DELETED:
                continue
            if sub.cron and not is_valid_cron(sub.cron):
                rc = self._db.update_status(
                    sub.id, STATE_ERROR, last_error=f"Invalid cron expression: {sub.cron}", guard="error_safe"
                )
                if rc:
                    self._log.warning(
                        "invalid_cron_on_startup",
                        sub_id=sub.id, name=sub.name, action="recovery",
                        result=f"invalid cron: {sub.cron}",
                    )
                    try:
                        send_smtp_notification(
                            subject=f"[AutoKB] Invalid cron: {sub.name}",
                            body=f"Subscription {sub.name!r} has invalid cron expression {sub.cron!r}.",
                            **self._smtp,
                        )
                    except Exception:
                        pass
        # Start monitors for active EVENT_BASED subscriptions
        for sub in self._db.list_event_based_active():
            self.start_monitor(sub.id)

    # ----- main loop tick -----
    async def _tick(self) -> None:
        # 1. SCHEDULED subscriptions: cron evaluation
        subs = self._db.list_subscriptions(include_deleted=True)
        for sub in subs:
            if sub.status != STATE_ENABLED:
                continue
            rec = self._registry.get(sub.plugin_id)
            if rec is None:
                continue
            if rec.sub_type != SUB_TYPE_SCHEDULED:
                continue
            if not sub.cron:
                continue
            if self._should_fire(sub, rec):
                self._enqueue(sub, source="cron")

    def _should_fire(self, sub: Subscription, rec: PluginRecord) -> bool:
        # We use minute-resolution matching; the rate-limit is a function
        # of the cron expression's minute field. For very fast iteration
        # in tests, we also support "firing on the minute" for any
        # well-formed cron. We do not re-fire within the same minute.
        if not is_valid_cron(sub.cron or ""):
            return False
        if not cron_due(sub.cron):
            return False
        # Per-minute guard: skip if the sub already ran in the current
        # minute. Without this, a "* * * * *" cron would re-enqueue on
        # every tick for the entire matching minute.
        if sub.last_heartbeat and _same_minute(sub.last_heartbeat, datetime.now(timezone.utc)):
            return False
        return True

    def _enqueue(self, sub: Subscription, source: str) -> None:
        if sub.status not in TRIGGERABLE_STATES:
            return
        self._db.try_enqueue(sub.id)
        self._queue.push_primary(sub.id)
        self._log.debug(
            "subscription_enqueued",
            sub_id=sub.id, name=sub.name, action="enqueue", source=source,
        )

    # ----- manual trigger from API -----
    def trigger(self, sub_id: str) -> bool:
        sub = self._db.get_subscription(sub_id)
        if not sub:
            return False
        self._enqueue(sub, source="manual_trigger")
        return True

    # ----- monitor loop management -----
    def start_monitor(self, sub_id: str) -> None:
        """Start a monitor() task for an EVENT_BASED subscription."""
        if sub_id in self._monitor_tasks and not self._monitor_tasks[sub_id].done():
            return
        cancel = self._monitor_cancel_events.setdefault(sub_id, asyncio.Event())
        cancel.clear()
        self._monitor_tasks[sub_id] = asyncio.create_task(self._monitor_loop(sub_id, cancel))
        sub = self._db.get_subscription(sub_id)
        if sub:
            self._log.debug("monitor_started", sub_id=sub_id, name=sub.name, plugin=sub.plugin_id)

    def cancel_monitor(self, sub_id: str) -> None:
        ev = self._monitor_cancel_events.get(sub_id)
        if ev is not None:
            ev.set()
        task = self._monitor_tasks.pop(sub_id, None)
        if task and not task.done():
            task.cancel()
        sub = self._db.get_subscription(sub_id)
        if sub:
            self._log.debug("monitor_cancelled", sub_id=sub_id, name=sub.name, action="cancel")

    def restart_monitor(self, sub_id: str) -> None:
        self.cancel_monitor(sub_id)
        self.start_monitor(sub_id)

    async def _monitor_loop(self, sub_id: str, cancel: asyncio.Event) -> None:
        sub = self._db.get_subscription(sub_id)
        if not sub:
            return
        rec = self._registry.get(sub.plugin_id)
        if rec is None:
            return
        # The plugin needs an instance with config; we instantiate fresh
        # for each monitor loop.
        try:
            instance = rec.cls()
        except Exception as exc:  # noqa: BLE001
            self._log.error("monitor_instantiate_failed", sub_id=sub_id, error=str(exc))
            return

        config = self._db.decrypt_config(sub, rec.password_fields)
        # Cron fallback: also run periodically
        cron = sub.cron

        while not cancel.is_set():
            # Refresh the in-memory sub from the DB so we see updates to
            # last_heartbeat (set by try_enqueue) and any other fields
            # changed out-of-band. Without this, the per-minute cron guard
            # in (2) below would never trip because sub.last_heartbeat
            # would remain stale (None for never-run subs).
            fresh = self._db.get_subscription(sub_id)
            if fresh is None:
                break
            sub = fresh
            # 1. Run the monitor() coroutine
            try:
                self._log.debug("monitor_iteration_start", sub_id=sub_id)
                timeout = getattr(instance, 'metadata', {}).get('monitor_timeout', 300.0)
                triggered = await asyncio.wait_for(
                    instance.monitor(config, cancel), timeout=timeout
                )
                self._log.debug("monitor_iteration_end", sub_id=sub_id, result=str(triggered))
                if triggered:
                    self._enqueue(sub, source="monitor")
            except asyncio.TimeoutError:
                # Plugin author should respect cancel_token, but if they
                # block forever we still keep the supervisor alive.
                self._log.debug("monitor_timeout", sub_id=sub_id)
            except asyncio.CancelledError:
                break
            except NotImplementedError:
                # Plugin does not implement monitor(); just rely on cron.
                pass
            except Exception as exc:  # noqa: BLE001
                self._log.error(
                    "monitor_exception",
                    sub_id=sub_id, error=str(exc),
                    traceback=traceback.format_exc(),
                )
                # Sleep before retrying to prevent tight-loop spinning.
                try:
                    await asyncio.wait_for(cancel.wait(), timeout=MONITOR_ERROR_SLEEP)
                    break
                except asyncio.TimeoutError:
                    pass
                continue

            # 2. Cron fallback path. Per-minute guard so a "* * * * *"
            # cron fires at most once per minute, not once per 2s tick.
            if cron and is_valid_cron(cron) and cron_due(cron):
                if sub.last_heartbeat and _same_minute(sub.last_heartbeat, datetime.now(timezone.utc)):
                    pass  # already fired this minute
                else:
                    self._enqueue(sub, source="cron_fallback")
            # 3. Sleep before next iteration
            try:
                await asyncio.wait_for(cancel.wait(), timeout=2.0)
                break
            except asyncio.TimeoutError:
                continue
