"""SINK reconciliation engine — orchestrates file-to-remote sync for a subscription.

Called by the worker from two paths:
  1. At the end of a FULL subscription run (after debounce + re-eval).
  2. Directly from a SINK_ONLY queue operation.
"""

import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from utils.constants import (
    STATE_DELETED,
    STATE_DISABLED,
    STATE_ENABLED,
    STATE_ENQUEUED,
    STATE_ERROR,
    STATE_IN_PROGRESS,
)
from utils.database import DatabaseManager
from utils.sink_registry import SinkRegistry
from utils.sink_base import compute_file_hash
from utils.misc_utils import get_logger


LOG_FILE = "/logs/worker.log"
_MTIME_TOLERANCE = 0.001  # 1 ms


def _hb(db: DatabaseManager, sub_id: str, queue, pct: int, message: str = None) -> None:
    db.update_heartbeat_and_progress(sub_id, pct)
    if message:
        db.update_last_message(sub_id, message)
    if queue:
        try:
            queue.refresh_lock(sub_id)
        except Exception:
            pass


def _send_error_email(sub_name: str, target_name: str, error: str) -> None:
    from worker.execution_engine import _send_smtp_for_worker
    try:
        _send_smtp_for_worker(
            subject=f"[AutoKB] SINK target error: {target_name}",
            body=(
                f"Subscription: {sub_name}\n"
                f"Target: {target_name}\n"
                f"Error: {error}"
            ),
        )
    except Exception:
        pass


# Mtime matching helpers
def _fs_mtime(path: str) -> float:
    return os.path.getmtime(path)


def _mtimes_match(fs_mtime: float, db_mtime_dt) -> bool:
    if db_mtime_dt is None:
        return False
    if not db_mtime_dt.tzinfo:
        db_ts = db_mtime_dt.replace(tzinfo=timezone.utc).timestamp()
    else:
        db_ts = db_mtime_dt.timestamp()
    return abs(fs_mtime - db_ts) < _MTIME_TOLERANCE


def _file_matches_db(path: str, df) -> bool:
    try:
        fs_stat = os.stat(path)
    except OSError:
        return False
    if fs_stat.st_size != df.size:
        return False
    return _mtimes_match(fs_stat.st_mtime, df.mtime)


def reconcile_subscription_targets(
    sub, db: DatabaseManager, sink_registry: "SinkRegistry",
    queue=None, log=None,
) -> None:
    """Run SINK reconciliation for *all* targets linked to this subscription.

    ``sub`` — the Subscription row object (must have .id, .name, .plugin_id).
    ``queue`` — optional QueueManager for lock refresh heartbeats.
    ``log`` — optional logger.
    """
    if log is None:
        log = get_logger("sink_recon", LOG_FILE)
    sub_id = sub.id
    sub_name = sub.name

    # Gather target_subscriptions for this sub
    ds_links = db.list_targets_for_subscription(sub_id)
    if not ds_links:
        return  # no downstream targets

    output_dir = os.path.join("/output", sub.plugin_id, sub_name)

    # Collect FS files (recursive, one row per file)
    fs_files: Dict[str, os.stat_result] = {}
    if os.path.isdir(output_dir):
        for root, _dirs, fnames in os.walk(output_dir):
            for fname in fnames:
                fpath = os.path.join(root, fname)
                try:
                    st = os.stat(fpath)
                    fs_files[fpath] = st
                except OSError:
                    pass

    add_count = 0
    update_count = 0
    remove_count = 0
    error_ds_names = []
    prog = 0

    for ds_link in ds_links:
        ds_status = ds_link.status
        target_id = ds_link.target_id

        if ds_status in (STATE_DISABLED, STATE_ERROR):
            continue

        if ds_status == STATE_DELETED:
            # Remove all target_datafile rows for this deleted link
            ds_df_rows = db.list_datafiles_for_target(target_id)
            for ds_df in ds_df_rows:
                remove_count += 1
                _invoke_remove(
                    ds_link, target_id, ds_df, db, sink_registry, log, sub_name, error_ds_names
                )
                prog += 1
                _hb(db, sub_id, queue, pct=prog % 100)
            db.delete_target_subscription(target_id, sub_id)
            # After removal, check if this was the last subscription for the target
            remaining = db.count_target_subscriptions_for_target(target_id)
            if remaining == 0:
                _remove_orphan_target(target_id, db, sink_registry, log)
            continue

        # --- ENABLED / ENQUEUED: full reconcile ---
        try:
            # 1. Ensure remote_target_id exists
            ds_row = db.get_target(target_id)
            if ds_row is None:
                continue
            svc = _get_service(ds_row, db, sink_registry, log)
            if svc is None:
                continue
            if not svc.remote_target_id:
                try:
                    svc.base_add_target()
                except Exception as exc:
                    _transition_ds_error(
                        ds_link, target_id, sub_id, db, log,
                        sub_name, svc.name, str(exc), error_ds_names,
                    )
                    continue

            pass1_count = _reconcile_pass1(
                fs_files, ds_link, target_id, sub_id, db, svc, queue, log,
            )
            add_count += pass1_count[0]
            update_count += pass1_count[1]
            remove_count += pass1_count[2]
            prog += 1
            _hb(db, sub_id, queue, prog % 100)

            # Set status ENABLED if it was ENQUEUED
            if ds_status == STATE_ENQUEUED:
                db.set_target_subscription_status(target_id, sub_id, STATE_ENABLED)

            msg = f"Reconciled: +{pass1_count[0]} added, ~{pass1_count[1]} updated, -{pass1_count[2]} removed"
            db.set_target_subscription_status(
                target_id, sub_id, STATE_ENABLED, message=msg,
            )

        except Exception as exc:
            _transition_ds_error(
                ds_link, target_id, sub_id, db, log,
                sub_name,
                (ds_row.name if ds_row else target_id),
                str(exc), error_ds_names,
            )

    # --- Pass II: update akb_datafile stats for the subscription ---
    _pass2_akb_datafiles(sub, db, output_dir, fs_files, log)

    log.info(
        "sink_recon_complete",
        sub_id=sub_id, name=sub_name,
        added=add_count, updated=update_count, removed=remove_count,
        errors=len(error_ds_names),
    )


def _get_service(ds_row, db, sink_registry, log):
    """Get service name and instantiate the SINK service for a target."""
    svc_row = db.get_sink(ds_row.service_id)
    if svc_row is None:
        log.warning("sink_service_not_found", target_id=ds_row.id, service_id=ds_row.service_id)
        return None
    api_key = db.decrypt_target_api_key(ds_row)
    # We need to pass decrypted api_key to the service instance.
    # Temporarily patch the row's api_key attribute.
    import copy
    patched = copy.copy(ds_row)
    patched.api_key = api_key
    svc = sink_registry.load_service_for_recon(svc_row.name, patched, db)
    if svc is None:
        log.warning("sink_class_not_found", target_id=ds_row.id, service=svc_row.name)
    return svc


def _reconcile_pass1(
    fs_files, ds_link, target_id, sub_id, db, svc, queue, log,
) -> tuple:
    """Pass I.1 (FS→target) and I.2 (target→FS removal)."""
    add_count = 0
    update_count = 0
    remove_count = 0
    ds_df_rows = db.list_datafiles_for_target(target_id)
    ds_df_by_datafile = {r.datafile_id: r for r in ds_df_rows}
    processed_datafile_ids = set()

    for fpath, st in fs_files.items():
        size = st.st_size
        mtime = st.st_mtime

        # get (or create) akb_datafile
        df = db.get_datafile_by_path(fpath)
        if df is None:
            # unknown file → add
            try:
                svc.base_add_datafile(sub_id, fpath)
                add_count += 1
            except Exception as exc:
                log.warning("sink_add_failed", path=fpath, error=str(exc))
            continue

        processed_datafile_ids.add(df.id)
        ds_df = ds_df_by_datafile.get(df.id)

        if _file_matches_db(fpath, df):
            if ds_df and ds_df.hash != df.hash:
                try:
                    svc.base_update_datafile(df.id, df.hash)
                    update_count += 1
                except Exception as exc:
                    log.warning("sink_update_failed", datafile_id=df.id, error=str(exc))
            continue

        try:
            real_hash = compute_file_hash(fpath)
        except Exception as exc:
            log.warning("sink_hash_failed", path=fpath, error=str(exc))
            continue

        if real_hash == df.hash:
            if ds_df and ds_df.hash != df.hash:
                try:
                    svc.base_update_datafile(df.id, df.hash)
                    update_count += 1
                except Exception as exc:
                    log.warning("sink_update_failed", datafile_id=df.id, error=str(exc))
        else:
            try:
                svc.base_update_datafile(df.id, real_hash)
                update_count += 1
            except Exception as exc:
                log.warning("sink_update_failed", datafile_id=df.id, hash=real_hash, error=str(exc))

    for ds_df in ds_df_rows:
        if ds_df.datafile_id not in processed_datafile_ids:
            try:
                svc.base_remove_datafile(ds_df.datafile_id)
                remove_count += 1
            except Exception as exc:
                log.warning("sink_remove_failed", datafile_id=ds_df.datafile_id, error=str(exc))

    return add_count, update_count, remove_count


def _pass2_akb_datafiles(sub, db, output_dir, fs_files, log) -> None:
    """Update akb_datafile stats from the filesystem state."""
    sub_id = sub.id
    checked_at = datetime.now(timezone.utc)

    # Fetch all akb_datafile rows for this sub
    datafiles = db.list_datafiles_for_subscription(sub_id)
    for df in datafiles:
        if df.path not in fs_files:
            # File missing → try delete (swallow FK errors from disabled targets)
            try:
                db.delete_datafile(df.id)
            except Exception:
                pass
            continue
        st = fs_files[df.path]
        if _file_matches_db(df.path, df):
            db.update_datafile_last_checked(df.id)
        else:
            try:
                real_hash = compute_file_hash(df.path)
            except Exception as exc:
                log.warning("sink_pass2_hash_failed", path=df.path, error=str(exc))
                db.update_datafile_last_checked(df.id)
                continue
            db.update_datafile_stats(df.id, st.st_size, st.st_mtime, real_hash)


def _invoke_remove(ds_link, target_id, ds_df, db, sink_registry, log, sub_name, error_ds_names):
    """Remove a single target_datafile row (caller owns exception handling)."""
    ds_row = db.get_target(target_id)
    if ds_row is None:
        return
    svc = _get_service(ds_row, db, sink_registry, log)
    if svc is None:
        return
    try:
        svc.base_remove_datafile(ds_df.datafile_id)
    except Exception as exc:
        _transition_ds_error(
            ds_link, target_id, ds_link.subscription_id, db, log,
            sub_name, ds_row.name, str(exc), error_ds_names,
        )


def _transition_ds_error(ds_link, target_id, sub_id, db, log,
                          sub_name, ds_name, error, error_ds_names) -> None:
    """Set a target_subscription to ERROR and send one email on transition."""
    if f"{target_id}_{sub_id}" in error_ds_names:
        return  # already handled this pass
    if ds_link and ds_link.status != STATE_ERROR:
        try:
            db.set_target_subscription_status(target_id, sub_id, STATE_ERROR, message=error)
        except Exception:
            pass
        try:
            _send_error_email(sub_name, ds_name, error)
        except Exception:
            pass
        error_ds_names.append(f"{target_id}_{sub_id}")

    log.warning("sink_ds_error", target_id=target_id, sub_id=sub_id, error=error)


def _remove_orphan_target(target_id: str, db: DatabaseManager,
                              sink_registry: SinkRegistry, log) -> None:
    """Called when the last subscription is removed from a target."""
    ds_row = db.get_target(target_id)
    if ds_row is None:
        return
    svc = _get_service(ds_row, db, sink_registry, log)
    if svc and svc.remote_target_id:
        try:
            svc.remove_target()
        except Exception as exc:
            log.warning("sink_remove_target_failed", target_id=target_id, error=str(exc))
    db.delete_target_datafiles_for_target(target_id)
    db.delete_target_row(target_id)
