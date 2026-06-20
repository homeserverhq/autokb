"""The Worker — multiprocessing pool that executes plugin code.

The worker is split into:
  * ``worker.py`` (the entrypoint, the Level-1 outer loop, the Redis
    P-Queue consumer).
  * ``execution_engine.py`` (the per-subscription state machine and the
    Managed Execution Wrapper that spawns ``getData()`` as a child
    process with heartbeat monitoring).
"""

import asyncio
import json
import multiprocessing as mp
import os
import queue
import signal
import sys
import threading
import time
import traceback
from typing import Any, Dict, List, Optional

# Ensure /src is on sys.path so sibling package imports work when this
# module is invoked as a script (`python /src/worker/worker.py`).
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_PARENT = os.path.dirname(_THIS_DIR)
sys.path = [p for p in sys.path if os.path.realpath(p) != os.path.realpath(_THIS_DIR)]
if _PARENT not in sys.path:
    sys.path.insert(0, _PARENT)
# Set __package__ so sibling imports work even when this file is the entrypoint.
if __name__ == "__main__" and __package__ in (None, ""):
    __package__ = "worker"

from utils.constants import (
    DEBOUNCE_PHASE_SECONDS,
    HEARTBEAT_TIMEOUT,
    LOCK_TTL,
    P_QUEUE_KEY,
    S_QUEUE_KEY,
    STATE_DELETED,
    STATE_DISABLED,
    STATE_ENABLED,
    STATE_ENQUEUED,
    STATE_ERROR,
    STATE_IN_PROGRESS,
)
from utils.constants import EXIT_SUCCESS
from utils.database import DatabaseManager, EventLog, Subscription
from utils.misc_utils import get_logger
from utils.queue_utils import QueueManager, wait_for_redis
from utils.registry import PluginRegistry
from worker.execution_engine import execute_subscription, ExecutionResult


LOG_FILE = "/logs/worker.log"
LOG = get_logger("worker", LOG_FILE)

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://autokb:autokb@autokb-db:5432/autokb")
REDIS_URL = os.environ.get("REDIS_URL", "redis://autokb-redis:6379/0")
WORKER_COUNT = int(os.environ.get("WORKER_COUNT", "4"))


# ---------------------------------------------------------------------------
# Worker entrypoint
# ---------------------------------------------------------------------------
def _main() -> None:
    log = get_logger("worker", LOG_FILE)
    log.info("worker_starting", action="startup", result="ok", workers=WORKER_COUNT)

    # Connect Redis and Postgres
    wait_for_redis(REDIS_URL, lambda ev, msg: log.info(ev, message=msg))
    queue = QueueManager(REDIS_URL)

    db = _wait_for_db(log)

    # Build the plugin registry fresh
    registry = PluginRegistry(plugins_dir="/src/plugins", component="plugin_loader", log_file=LOG_FILE)
    registry.reload_all()

    log.info("registry_loaded", action="startup", result="ok", plugins=len(registry.list_records()))

    # Start the multiprocessing pool
    processes: List[mp.Process] = []
    for i in range(WORKER_COUNT):
        p = mp.Process(target=_worker_loop, args=(i, db, registry, DATABASE_URL, REDIS_URL), daemon=False)
        p.start()
        processes.append(p)
        log.info("worker_spawned", action="spawn", result=f"pid={p.pid} idx={i}")

    # Wait for processes; if one dies, respawn it.
    while True:
        time.sleep(5.0)
        for i, p in enumerate(processes):
            if not p.is_alive():
                log.warning("worker_died", action="respawn", result=f"idx={i}")
                p = mp.Process(target=_worker_loop, args=(i, db, registry, DATABASE_URL, REDIS_URL), daemon=False)
                p.start()
                processes[i] = p


def _wait_for_db(log) -> DatabaseManager:
    from utils.constants import STARTUP_RETRY_SLEEP, MAX_STARTUP_RETRIES
    last_exc: Optional[Exception] = None
    for i in range(MAX_STARTUP_RETRIES):
        try:
            db = DatabaseManager(DATABASE_URL, log_file=LOG_FILE, component="db")
            db.health_check()
            log.info("db_connected", action="startup", result="ok", attempt=i + 1)
            return db
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            log.warning("db_retry", action="startup", result=str(exc), attempt=i + 1)
            time.sleep(STARTUP_RETRY_SLEEP)
    raise RuntimeError(f"Could not connect to PostgreSQL: {last_exc}")


def _worker_loop(worker_idx: int, parent_db: DatabaseManager,
                 registry: PluginRegistry, db_url: str, redis_url: str) -> None:
    """Level-1 worker loop. Each iteration handles one subscription at a time
    (the invariant: one Worker per Subscription)."""
    log = get_logger(f"worker-{worker_idx}", LOG_FILE)
    log.info("worker_started", action="startup", result=f"idx={worker_idx}")

    # The child process should NOT reuse the parent's DB or Redis connections
    parent_db.dispose(close=True)
    queue = QueueManager(redis_url)
    db = DatabaseManager(db_url, log_file=LOG_FILE, component=f"db-{worker_idx}")
    # Re-build the registry (this process has a fresh interpreter state)
    registry = PluginRegistry(plugins_dir="/src/plugins", component=f"plugin_loader-{worker_idx}", log_file=LOG_FILE)
    registry.reload_all()

    iteration = 0
    while True:
        iteration += 1
        try:
            sub_id = queue.pop_primary(timeout=5)
            if sub_id is None:
                continue
            log.debug("queue_popped", sub_id=sub_id, action="queue", result=f"iter={iteration}")
            # Collapse: drain the P-Queue of this sub_id
            queue.drain_primary(sub_id)

            # Try to acquire the safety lock
            if not queue.acquire_lock(sub_id, blocking=True):
                # Lock not acquired; push to S-Queue
                queue.push_secondary(sub_id)
                log.debug("lock_busy_secondary", sub_id=sub_id, action="lock", result="busy")
                continue

            # We have the lock — enter the inner loop
            log.debug("execution_claimed", sub_id=sub_id, action="claim", result="ok")
            try:
                _process_sub_inner(worker_idx, sub_id, queue, db, registry, log)
            finally:
                queue.release_lock(sub_id)
                log.debug("lock_released", sub_id=sub_id, action="lock", result="released")
        except KeyboardInterrupt:
            log.info("worker_interrupted", action="shutdown", result="ok")
            return
        except Exception as exc:  # noqa: BLE001
            log.error("worker_loop_error", action="loop", result=str(exc), traceback=traceback.format_exc())
            time.sleep(0.5)


def _process_sub_inner(worker_idx: int, sub_id: str, queue: QueueManager,
                       db: DatabaseManager, registry: PluginRegistry, log) -> None:
    """Inner loop for a single subscription_id.

    The inner loop drains both queues, runs the subscription, debounces,
    and re-evaluates for new triggers.
    """
    while True:
        # 1. Drain both queues
        n_drained = queue.drain_both(sub_id)
        if n_drained:
            log.debug("queues_drained", sub_id=sub_id, action="drain", result=f"count={n_drained}")

        sub = db.get_subscription(sub_id)
        if sub is None:
            log.debug("subscription_missing", sub_id=sub_id, action="cleanup", result="skipped")
            return

        # 2. DELETED state — cleanup
        if sub.status == STATE_DELETED:
            log.info("subscription_cleanup_starting", sub_id=sub_id, name=sub.name)
            _cleanup_subscription(sub, log)
            return

        # 3. DISABLED / ERROR — skip and release
        if sub.status in (STATE_DISABLED, STATE_ERROR):
            log.debug("subscription_skipped", sub_id=sub_id, name=sub.name, action="skip", result=f"status={sub.status}")
            return

        # 4. Claim — set to IN_PROGRESS (with non-NULL last_heartbeat)
        rc = db.mark_execution_start(sub_id)
        if rc == 0:
            log.debug("claim_failed", sub_id=sub_id, name=sub.name, action="claim", result="lost_race")
            return
        # Re-fetch the latest sub state
        sub = db.get_subscription(sub_id)
        if sub is None:
            return
        log.debug("execution_post_claim", sub_id=sub_id, name=sub.name, action="claim",
                 result=f"status={sub.status} last_heartbeat={sub.last_heartbeat}")

        log.debug("execution_starting", sub_id=sub_id, name=sub.name, action="execute", result="ok")
        rec = registry.get_or_load(sub.plugin_id)
        if rec is None:
            rc = db.update_status(sub_id, STATE_ERROR, last_error=f"Plugin {sub.plugin_id!r} not loaded", guard="error_safe")
            log.error("plugin_not_loaded", sub_id=sub_id, name=sub.name, action="execute", result="missing_plugin")
            try:
                db.record_execution(sub_id, 1, f"Plugin {sub.plugin_id!r} not loaded")
            except Exception:
                pass
            return

        # 5. Run the execution
        result = execute_subscription(sub, rec, db, log)

        # 6. Handle result
        if result.outcome == "deleted":
            # Cleanup after execution completed (cancellation case)
            _cleanup_subscription(sub, log)
            return
        if result.outcome == "skipped_disabled":
            return

        if result.outcome == "success":
            # Check current status: if DISABLED/DELETED skip EventLog; otherwise
            # record success and update status to ENABLED. If a trigger fired
            # during the execution and flipped the status to ENQUEUED, leave
            # it ENQUEUED so the re-eval step below picks it up — otherwise
            # we'd clobber the queued follow-up execution and the next
            # claim step would fail with ``lost_race`` (status=ENABLED does
            # not match ``mark_execution_start``'s WHERE clause).
            cur = db.get_subscription(sub_id)
            if cur is not None and cur.status not in (STATE_DISABLED, STATE_DELETED):
                db.record_execution(sub_id, EXIT_SUCCESS, "")
                if cur.status == STATE_ENQUEUED:
                    log.info(
                        "execution_completed_preserved_queue", sub_id=sub_id, name=sub.name,
                        action="complete", result="exit_code=0 success status_kept=ENQUEUED",
                    )
                else:
                    db.update_status(sub_id, STATE_ENABLED, guard="success_to_enabled")
                    log.info("execution_completed", sub_id=sub_id, name=sub.name, action="complete", result="exit_code=0 success")
            else:
                log.debug("execution_completed_skipped_log", sub_id=sub_id, name=sub.name, action="complete",
                         result=f"status={cur.status if cur else 'gone'}")
        elif result.outcome == "timeout":
            # Watcher already set status=ERROR, recorded EventLog, sent SMTP
            log.info("execution_timed_out", sub_id=sub_id, name=sub.name, action="complete", result="exit_code=2")
            return  # do not re-eval after a timeout — status is ERROR
        elif result.outcome == "schema_validation":
            db.update_status(sub_id, STATE_ERROR, last_error=result.exit_string, guard="error_safe")
            db.record_execution(sub_id, 3, result.exit_string)
            log.warning("execution_config_rejected", sub_id=sub_id, name=sub.name, action="complete", result=f"exit_code=3 error={result.exit_string}")
        elif result.outcome == "error":
            db.update_status(sub_id, STATE_ERROR, last_error=result.exit_string, guard="error_safe")
            db.record_execution(sub_id, 1, result.exit_string)
            log.warning("execution_failed", sub_id=sub_id, name=sub.name, action="complete", result=f"exit_code=1 error={result.exit_string}")
        elif result.outcome == "load_error":
            db.update_status(sub_id, STATE_ERROR, last_error=result.exit_string, guard="error_safe")
            db.record_execution(sub_id, 1, result.exit_string)
            log.warning("execution_load_error", sub_id=sub_id, name=sub.name, action="complete", result=f"exit_code=1 error={result.exit_string}")

        # 7. Debounce phase
        time.sleep(DEBOUNCE_PHASE_SECONDS)

        # 8. Re-eval: any new instances queued?
        if queue.has_in_queue(sub_id):
            # A trigger fired during execution / success path. The sub_id
            # is in some queue but the sub status may be ENABLED (the
            # success path overwrites any ENQUEUED state set by the
            # trigger). Make sure the status is ENQUEUED so the next
            # ``mark_execution_start`` claim succeeds. We only flip
            # ENABLED → ENQUEUED; other states (DELETED, DISABLED, etc.)
            # are left alone.
            db.ensure_enqueued(sub_id)
            log.debug("requeue_detected", sub_id=sub_id, action="re_eval", result="loop_continue")
            continue

        # 9. No more triggers; release and exit
        # Ensure the status is back to ENABLED (or the appropriate non-running state)
        cur = db.get_subscription(sub_id)
        if cur is not None and cur.status in (STATE_IN_PROGRESS, STATE_ENQUEUED):
            db.update_status(sub_id, STATE_ENABLED, guard="success_to_enabled")
        return


def _cleanup_subscription(sub: Subscription, log) -> None:
    """Remove the output directory and the DB row for a DELETED subscription.

    Retries the DB row deletion up to 3 times. On final failure, logs an
    error and sends an SMTP notification so operators can intervene.
    """
    import shutil
    from worker.execution_engine import _send_smtp_for_worker
    out_dir = f"/output/{sub.plugin_id}/{sub.name}"
    if os.path.isdir(out_dir):
        try:
            shutil.rmtree(out_dir)
            log.debug("output_directory_removed", sub_id=sub.id, action="cleanup", result=out_dir)
        except Exception as exc:  # noqa: BLE001
            log.error("output_directory_remove_failed", sub_id=sub.id, action="cleanup", result=str(exc))
    max_attempts = 3
    for attempt in range(1, max_attempts + 1):
        db = None
        try:
            db = DatabaseManager(DATABASE_URL, log_file=LOG_FILE, component="db-cleanup")
            db.delete_subscription_row(sub.id)
            log.debug("subscription_row_deleted", sub_id=sub.id, name=sub.name,
                      action="cleanup", result=f"attempt={attempt}")
            return
        except Exception as exc:  # noqa: BLE001
            log.error("subscription_row_delete_failed", sub_id=sub.id, name=sub.name,
                      action="cleanup", result=f"attempt={attempt} error={exc}")
            if attempt < max_attempts:
                time.sleep(1.0)
        finally:
            if db is not None:
                db.dispose(close=True)
    log.error("subscription_cleanup_permanent_failure", sub_id=sub.id, name=sub.name,
              plugin_id=sub.plugin_id, action="cleanup",
              result=f"failed after {max_attempts} attempts — row remains in DB")
    try:
        _send_smtp_for_worker(
            subject=f"[AutoKB] P1: Subscription cleanup failed: {sub.name}",
            body=(
                f"Subscription {sub.name!r} (id={sub.id}, plugin={sub.plugin_id}) "
                f"was deleted but the DB row could not be removed after {max_attempts} attempts.\n"
                "The subscription is stuck in DELETED state. Manual database cleanup is required."
            ),
        )
    except Exception:  # noqa: BLE001
        pass


if __name__ == "__main__":
    _main()
