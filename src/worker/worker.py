"""The Worker — multiprocessing pool that executes plugin code."""

import json
import multiprocessing as mp
import os
import queue
import shutil
import signal
import sys
import threading
import time
import traceback
from typing import Any, Dict, List, Optional

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_PARENT = os.path.dirname(_THIS_DIR)
sys.path = [p for p in sys.path if os.path.realpath(p) != os.path.realpath(_THIS_DIR)]
if _PARENT not in sys.path:
    sys.path.insert(0, _PARENT)
if __name__ == "__main__" and __package__ in (None, ""):
    __package__ = "worker"

from utils.constants import (
    DEBOUNCE_PHASE_SECONDS,
    HEARTBEAT_TIMEOUT,
    LOCK_TTL,
    OPERATION_DKB_ONLY,
    OPERATION_FULL,
    P_QUEUE_KEY,
    S_QUEUE_KEY,
    STATE_DELETED,
    STATE_DISABLED,
    STATE_ENABLED,
    STATE_ENQUEUED,
    STATE_ERROR,
    STATE_IN_PROGRESS,
    WORKER_STARTUP_DELAY_S,
)
from utils.constants import EXIT_SUCCESS
from utils.database import DatabaseManager, EventLog, Subscription
from utils.dkb_registry import DKBRegistry
from utils.misc_utils import get_logger
from utils.queue_utils import QueueManager, wait_for_redis
from utils.registry import PluginRegistry
from worker.execution_engine import execute_subscription, ExecutionResult, _send_smtp_for_worker
from worker.dkb_recon import reconcile_subscription_datastores


LOG_FILE = "/logs/worker.log"
LOG = get_logger("worker", LOG_FILE)

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://autokb:autokb@autokb-db:5432/autokb")
REDIS_URL = os.environ.get("REDIS_URL", "redis://autokb-redis:6379/0")
WORKER_COUNT = int(os.environ.get("WORKER_COUNT", "4"))


def _main() -> None:
    log = get_logger("worker", LOG_FILE)
    log.info("worker_starting", action="startup", result="ok", workers=WORKER_COUNT,
             startup_delay=WORKER_STARTUP_DELAY_S)

    time.sleep(WORKER_STARTUP_DELAY_S)

    wait_for_redis(REDIS_URL, lambda ev, msg: log.info(ev, message=msg))
    queue = QueueManager(REDIS_URL)

    db = _wait_for_db(log)

    registry = PluginRegistry(plugins_dir="/src/plugins", component="plugin_loader", log_file=LOG_FILE)
    registry.reload_all()
    log.info("registry_loaded", action="startup", result="ok", plugins=len(registry.list_records()))

    dkb_registry = DKBRegistry(dkbs_dir="/src/dkbservices", component="dkb_registry", log_file=LOG_FILE)
    dkb_registry.reload_all()
    log.info("dkb_registry_loaded", action="startup", result="ok", services=len(dkb_registry.list_records()))

    processes: List[mp.Process] = []
    for i in range(WORKER_COUNT):
        p = mp.Process(
            target=_worker_loop,
            args=(i, db, registry, dkb_registry, DATABASE_URL, REDIS_URL),
            daemon=False,
        )
        p.start()
        processes.append(p)
        log.info("worker_spawned", action="spawn", result=f"pid={p.pid} idx={i}")

    while True:
        time.sleep(5.0)
        for i, p in enumerate(processes):
            if not p.is_alive():
                log.warning("worker_died", action="respawn", result=f"idx={i}")
                p = mp.Process(
                    target=_worker_loop,
                    args=(i, db, registry, dkb_registry, DATABASE_URL, REDIS_URL),
                    daemon=False,
                )
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
        except Exception as exc:
            last_exc = exc
            log.warning("db_retry", action="startup", result=str(exc), attempt=i + 1)
            time.sleep(STARTUP_RETRY_SLEEP)
    raise RuntimeError(f"Could not connect to PostgreSQL: {last_exc}")


def _worker_loop(worker_idx: int, parent_db: DatabaseManager,
                 registry: PluginRegistry, dkb_registry: DKBRegistry,
                 db_url: str, redis_url: str) -> None:
    log = get_logger(f"worker-{worker_idx}", LOG_FILE)
    log.info("worker_started", action="startup", result=f"idx={worker_idx}")

    parent_db.dispose(close=True)
    queue = QueueManager(redis_url)
    db = DatabaseManager(db_url, log_file=LOG_FILE, component=f"db-{worker_idx}")
    registry = PluginRegistry(plugins_dir="/src/plugins", component=f"plugin_loader-{worker_idx}", log_file=LOG_FILE)
    registry.reload_all()
    dkb_registry = DKBRegistry(dkbs_dir="/src/dkbservices", component=f"dkb_registry-{worker_idx}", log_file=LOG_FILE)
    dkb_registry.reload_all()

    iteration = 0
    while True:
        iteration += 1
        try:
            item = queue.pop_primary(timeout=5)
            if item is None:
                continue
            sub_id = item["sub_id"]
            popped_op = item.get("operation", OPERATION_FULL)
            log.debug("queue_popped", sub_id=sub_id, action="queue", result=f"iter={iteration}")

            # Resolve operation: if popped item or any queued item is FULL → FULL
            has_full = popped_op == OPERATION_FULL or queue.any_full_for(sub_id)
            op = OPERATION_FULL if has_full else OPERATION_DKB_ONLY
            queue.drain_all(sub_id)

            if not queue.acquire_lock(sub_id, blocking=True):
                queue.push_secondary(sub_id, operation=op)
                log.debug("lock_busy_secondary", sub_id=sub_id, action="lock", result="busy")
                continue

            log.debug("execution_claimed", sub_id=sub_id, action="claim", result=f"op={op}")
            try:
                _process_sub_inner(worker_idx, sub_id, op, queue, db, registry, dkb_registry, log)
            finally:
                queue.release_lock(sub_id)
                log.debug("lock_released", sub_id=sub_id, action="lock", result="released")
        except KeyboardInterrupt:
            log.info("worker_interrupted", action="shutdown", result="ok")
            return
        except Exception as exc:
            log.error("worker_loop_error", action="loop", result=str(exc), traceback=traceback.format_exc())
            time.sleep(0.5)


def _process_sub_inner(worker_idx: int, sub_id: str, operation: str,
                       queue: QueueManager, db: DatabaseManager,
                       registry: PluginRegistry, dkb_registry: DKBRegistry, log) -> None:
    """Inner loop for a single subscription_id.

    * ``operation=FULL`` — run upstream, debounce, re-eval, recon, re-eval.
    * ``operation=DKB_ONLY`` — run downstream recon only (no upstream).
    """
    while True:
        queue.drain_all(sub_id)

        sub = db.get_subscription(sub_id)
        if sub is None:
            log.debug("subscription_missing", sub_id=sub_id, action="cleanup", result="skipped")
            return

        # --- DKB_ONLY: downstream recon only ---
        if operation == OPERATION_DKB_ONLY:
            if sub.status not in (STATE_ENABLED, STATE_ENQUEUED, STATE_IN_PROGRESS):
                log.debug("dkb_only_skipped", sub_id=sub_id, name=sub.name,
                          action="skip", result=f"status={sub.status}")
                return
            db.try_enqueue(sub_id)
            rc = db.mark_execution_start(sub_id)
            if rc == 0:
                return
            sub = db.get_subscription(sub_id)
            if sub is None:
                return
            try:
                reconcile_subscription_datastores(sub, db, dkb_registry, queue, log)
            finally:
                cur = db.get_subscription(sub_id)
                if cur and cur.status in (STATE_IN_PROGRESS, STATE_ENQUEUED):
                    db.update_status(sub_id, STATE_ENABLED, guard="success_to_enabled")
            return

        # --- FULL: upstream + recon ---

        if sub.status == STATE_DELETED:
            log.info("subscription_cleanup_starting", sub_id=sub_id, name=sub.name)
            _cleanup_subscription(sub, dkb_registry, db, log)
            return

        if sub.status in (STATE_DISABLED, STATE_ERROR):
            log.debug("subscription_skipped", sub_id=sub_id, name=sub.name,
                      action="skip", result=f"status={sub.status}")
            return

        rc = db.mark_execution_start(sub_id)
        if rc == 0:
            log.debug("claim_failed", sub_id=sub_id, name=sub.name, action="claim", result="lost_race")
            return
        sub = db.get_subscription(sub_id)
        if sub is None:
            return

        log.debug("execution_starting", sub_id=sub_id, name=sub.name, action="execute", result="ok")
        rec = registry.get_or_load(sub.plugin_id)
        if rec is None:
            rc = db.update_status(sub_id, STATE_ERROR, last_error=f"Plugin {sub.plugin_id!r} not loaded",
                                  guard="error_safe")
            log.error("plugin_not_loaded", sub_id=sub_id, name=sub.name, action="execute",
                      result="missing_plugin")
            try:
                db.record_execution(sub_id, 1, f"Plugin {sub.plugin_id!r} not loaded")
            except Exception:
                pass
            return

        result = execute_subscription(sub, rec, db, log)

        if result.outcome == "deleted":
            _cleanup_subscription(sub, dkb_registry, db, log)
            return
        if result.outcome == "skipped_disabled":
            return

        if result.outcome == "success":
            cur = db.get_subscription(sub_id)
            if cur is not None and cur.status not in (STATE_DISABLED, STATE_DELETED):
                db.record_execution(sub_id, EXIT_SUCCESS, "")
                if cur.status == STATE_ENQUEUED:
                    log.info("execution_completed_preserved_queue", sub_id=sub_id, name=sub.name,
                             action="complete", result="exit_code=0 success status_kept=ENQUEUED")
                else:
                    db.update_status(sub_id, STATE_ENABLED, guard="success_to_enabled")
                    log.info("execution_completed", sub_id=sub_id, name=sub.name,
                             action="complete", result="exit_code=0 success")
            else:
                log.debug("execution_completed_skipped_log", sub_id=sub_id, name=sub.name,
                          action="complete", result=f"status={cur.status if cur else 'gone'}")
        elif result.outcome == "timeout":
            log.info("execution_timed_out", sub_id=sub_id, name=sub.name,
                     action="complete", result="exit_code=2")
            return
        elif result.outcome == "schema_validation":
            db.update_status(sub_id, STATE_ERROR, last_error=result.exit_string, guard="error_safe")
            db.record_execution(sub_id, 3, result.exit_string)
            log.warning("execution_config_rejected", sub_id=sub_id, name=sub.name,
                        action="complete", result=f"exit_code=3 error={result.exit_string}")
        elif result.outcome == "error":
            db.update_status(sub_id, STATE_ERROR, last_error=result.exit_string, guard="error_safe")
            db.record_execution(sub_id, 1, result.exit_string)
            log.warning("execution_failed", sub_id=sub_id, name=sub.name,
                        action="complete", result=f"exit_code=1 error={result.exit_string}")
        elif result.outcome == "load_error":
            db.update_status(sub_id, STATE_ERROR, last_error=result.exit_string, guard="error_safe")
            db.record_execution(sub_id, 1, result.exit_string)
            log.warning("execution_load_error", sub_id=sub_id, name=sub.name,
                        action="complete", result=f"exit_code=1 error={result.exit_string}")

        # 7. Debounce phase
        time.sleep(DEBOUNCE_PHASE_SECONDS)

        # 8. Re-eval #1: any new instances queued?
        if queue.has_in_queue(sub_id):
            db.ensure_enqueued(sub_id)
            log.debug("requeue_detected", sub_id=sub_id, action="re_eval", result="loop_continue")
            operation = OPERATION_FULL  # any queue item → full
            continue

        # 9. DKB recon (downstream sync)
        sub = db.get_subscription(sub_id)
        if sub and sub.status in (STATE_ENABLED, STATE_ENQUEUED, STATE_IN_PROGRESS):
            reconcile_subscription_datastores(sub, db, dkb_registry, queue, log)

        # 10. Re-eval #2: check if a FULL item appeared during recon
        if queue.any_full_for(sub_id):
            db.ensure_enqueued(sub_id)
            log.debug("requeue_post_recon", sub_id=sub_id, action="re_eval", result="full_detected")
            operation = OPERATION_FULL
            queue.drain_all(sub_id)
            continue

        # No more triggers; clean exit
        cur = db.get_subscription(sub_id)
        if cur is not None and cur.status in (STATE_IN_PROGRESS, STATE_ENQUEUED):
            db.update_status(sub_id, STATE_ENABLED, guard="success_to_enabled")
        return


def _cleanup_subscription(sub: Subscription, dkb_registry: DKBRegistry, parent_db: DatabaseManager,
                          log) -> None:
    """Remove output dir, DKB remote files + rows, then the subscription DB row.

    Uses Q5 ordering: output dir removal → DKB cleanup → subscription row deletion.
    """
    out_dir = f"/output/{sub.plugin_id}/{sub.name}"
    if os.path.isdir(out_dir):
        try:
            shutil.rmtree(out_dir)
            log.debug("output_directory_removed", sub_id=sub.id, action="cleanup", result=out_dir)
        except Exception as exc:
            log.error("output_directory_remove_failed", sub_id=sub.id, action="cleanup", result=str(exc))

    # DKB cleanup: remove remote files + rows for every datastore_subscription
    _cleanup_subscription_datastores(sub, dkb_registry, parent_db, log)

    # Delete the subscription DB row (retry loop from original code)
    max_attempts = 3
    for attempt in range(1, max_attempts + 1):
        db = None
        try:
            db = DatabaseManager(DATABASE_URL, log_file=LOG_FILE, component="db-cleanup")
            db.delete_subscription_row(sub.id)
            log.debug("subscription_row_deleted", sub_id=sub.id, name=sub.name,
                      action="cleanup", result=f"attempt={attempt}")
            return
        except Exception as exc:
            log.error("subscription_row_delete_failed", sub_id=sub.id, name=sub.name,
                      action="cleanup", result=f"attempt={attempt} error={exc}")
            if attempt < max_attempts:
                time.sleep(1.0)
        finally:
            if db is not None:
                db.dispose(close=True)
    log.error("subscription_cleanup_permanent_failure", sub_id=sub.id, name=sub.name,
              plugin_id=sub.plugin_id, action="cleanup",
              result=f"failed after {max_attempts} attempts")
    try:
        _send_smtp_for_worker(
            subject=f"[AutoKB] P1: Subscription cleanup failed: {sub.name}",
            body=(
                f"Subscription {sub.name!r} (id={sub.id}, plugin={sub.plugin_id}) "
                f"was deleted but the DB row could not be removed after {max_attempts} attempts.\n"
                "Manual database cleanup is required."
            ),
        )
    except Exception:
        pass


def _cleanup_subscription_datastores(sub: Subscription, dkb_registry: DKBRegistry,
                                      db: DatabaseManager, log) -> None:
    """Remove all DKB artifacts for a deleted subscription."""
    from worker.dkb_recon import _get_service, _remove_orphan_datastore

    ds_links = db.list_datastores_for_subscription(sub.id)
    datastores_seen = set()
    for ds_link in ds_links:
        datastore_id = ds_link.datastore_id
        datastores_seen.add(datastore_id)
        ds_df_rows = db.list_datafiles_for_datastore(datastore_id)
        for ds_df in ds_df_rows:
            try:
                ds_row = db.get_datastore(datastore_id)
                if ds_row:
                    svc = _get_service(ds_row, db, dkb_registry, log)
                    if svc:
                        svc.base_remove_datafile(ds_df.datafile_id)
            except Exception as exc:
                log.warning("dkb_cleanup_remove_failed", datafile_id=ds_df.datafile_id, error=str(exc))
        db.delete_datastore_subscription(datastore_id, sub.id)

    # For each datastore now orphaned, call remove_datastore + delete row
    for did in datastores_seen:
        remaining = db.count_datastore_subscriptions_for_datastore(did)
        if remaining == 0:
            _remove_orphan_datastore(did, db, dkb_registry, log)

    # Delete orphan akb_datafile rows for this subscription
    for df in db.list_datafiles_for_subscription(sub.id):
        try:
            db.delete_datafile(df.id)
        except Exception:
            pass


if __name__ == "__main__":
    _main()
