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
from utils.misc_utils import SinkCancelledError, get_logger


LOG_FILE = "/logs/worker.log"
_MTIME_TOLERANCE = 0.001  # 1 ms
_SINK_HEARTBEAT_INTERVAL = 15.0  # seconds between heartbeat/lock refreshes during long passes


def _make_cancel_check(db: DatabaseManager, sub_id: str, target_id: str, queue, state: Dict[str, int]):
    """Build the cancellation + heartbeat callback for one target's recon pass.

    ``state`` is a mutable ``{"n": ...}`` counter the recon loop bumps per
    file; the callback uses it for the heartbeat progress value.

    The returned closure is installed on the sink via ``set_cancel_check``
    and consulted by ``_check_cancel`` at every checkpoint. It re-reads the
    target-subscription link and subscription status from the DB on every
    call (cheap SELECTs) and returns a ``SinkCancelledError`` kind when the
    link/sub is no longer active:

      * ``"link_removed"``  — link row gone or DELETED
      * ``"link_disabled"`` — link row DISABLED
      * ``"sub_gone"``      — subscription gone or DELETED
      * ``"sub_disabled"``  — subscription DISABLED

    It also refreshes the sub lock + heartbeat, throttled to
    ``_SINK_HEARTBEAT_INTERVAL``, so a long single-target upload pass stays
    alive for the watchdog without spamming SSE updates.
    """
    last_refresh = {"ts": 0.0}

    def _check() -> Optional[str]:
        now = time.time()
        if now - last_refresh["ts"] >= _SINK_HEARTBEAT_INTERVAL:
            last_refresh["ts"] = now
            try:
                db.update_heartbeat_and_progress(sub_id, state.get("n", 0) % 100)
            except Exception:
                pass
            if queue:
                try:
                    queue.refresh_lock(sub_id)
                except Exception:
                    pass
        try:
            link = db.get_target_subscription(target_id, sub_id)
        except Exception:
            link = None
        if link is None or link.status == STATE_DELETED:
            return "link_removed"
        if link.status == STATE_DISABLED:
            return "link_disabled"
        try:
            cur = db.get_subscription(sub_id)
        except Exception:
            cur = None
        if cur is None or cur.status == STATE_DELETED:
            return "sub_gone"
        if cur.status == STATE_DISABLED:
            return "sub_disabled"
        return None

    return _check


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

    # Mark active (ENABLED/ENQUEUED) links IN_PROGRESS so the parent target
    # immediately reflects a pending sync via SSE. DISABLED/ERROR/DELETED
    # links are untouched. The per-target pass below sets each link back to
    # ENABLED on completion.
    for link in ds_links:
        if link.status in (STATE_ENABLED, STATE_ENQUEUED):
            try:
                db.set_target_subscription_status(link.target_id, sub_id, STATE_IN_PROGRESS)
                link.status = STATE_IN_PROGRESS
            except Exception:
                pass

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
    # per-link Pass I result summaries, finalized only after Pass III (so the
    # link reflects the whole recon — including blocking batch upserts and any
    # healing — instead of reporting "done" prematurely).
    summaries = {}

    for ds_link in ds_links:
        ds_status = ds_link.status
        target_id = ds_link.target_id

        if ds_status in (STATE_DISABLED, STATE_ERROR):
            continue

        if ds_status == STATE_DELETED:
            # Remove this subscription's target_datafile rows for the deleted link
            remove_count += _remove_deleted_link(
                ds_link, target_id, sub_id, db, sink_registry, log,
                sub_name, error_ds_names, queue,
            )
            continue

        # --- ENABLED / ENQUEUED: full reconcile ---
        state = {"n": 0}
        try:
            # 1. Ensure remote_target_id exists
            ds_row = db.get_target(target_id)
            if ds_row is None:
                continue
            svc = _get_service(ds_row, db, sink_registry, log)
            if svc is None:
                continue
            # 1b. Install the cancellation + heartbeat callback so the sink can
            # abort mid-pass (and keep the lock/heartbeat alive) if the user
            # removes/disables the link or the subscription mid-recon.
            svc.set_cancel_check(_make_cancel_check(db, sub_id, target_id, queue, state))
            # The remote target must have been provisioned synchronously at
            # target create/update time (manager._ensure_target_remote). Recon
            # NEVER creates the remote target — it only reads the id from the
            # DB. If it is missing here, the link transitions to ERROR so the
            # user is notified and can re-provision via Update.
            if not svc.remote_target_id:
                log.warning(
                    "remote_target_missing", sub_id=sub_id, name=sub_name,
                    target_id=target_id, service=svc.name,
                    action="error", result="remote_target_id null; mark link ERROR",
                )
                _transition_ds_error(
                    ds_link, target_id, sub_id, db, log,
                    sub_name, ds_row.name,
                    "Remote target not provisioned (remote_target_id is null). "
                    "Re-create or Update this target to provision the remote resource.",
                    error_ds_names,
                )
                continue

            pass1_count = _reconcile_pass1(
                fs_files, ds_link, target_id, sub_id, db, svc, queue, log, state,
            )
            add_count += pass1_count[0]
            update_count += pass1_count[1]
            remove_count += pass1_count[2]
            prog += 1
            _hb(db, sub_id, queue, prog % 100)
            summaries[target_id] = pass1_count

        except SinkCancelledError as exc:
            # The link/sub was removed or disabled mid-recon — this is NOT an
            # error, so never transition to ERROR or send the notification email.
            abort_all = _handle_recon_cancel(
                exc.kind, ds_link, target_id, sub_id, db, sink_registry, log,
                sub_name, error_ds_names, queue,
            )
            if abort_all:
                _finalize_link_statuses(db, sub_id, summaries, {}, log)
                log.info(
                    "sink_recon_aborted",
                    sub_id=sub_id, name=sub_name, target_id=target_id, reason=exc.kind,
                )
                return  # sub-level cancel — stop everything, skip Pass II
            continue

        except Exception as exc:
            _transition_ds_error(
                ds_link, target_id, sub_id, db, log,
                sub_name,
                (ds_row.name if ds_row else target_id),
                str(exc), error_ds_names,
            )

    # --- Pass II: update akb_datafile stats for the subscription ---
    try:
        _pass2_akb_datafiles(sub, db, output_dir, fs_files, log)
    except Exception as exc:
        log.error("sink_pass2_failed", sub_id=sub_id, error=str(exc))
        try:
            db.update_status(sub_id, STATE_ERROR, last_error=f"Sink pass 2 failed: {exc}",
                             guard="error_safe")
        except Exception:
            pass

    # --- Pass III: heal remote drift — DB rows missing on the remote ---
    try:
        healed_map = _pass3_heal_remote(
            sub, ds_links, db, sink_registry, queue, log, error_ds_names,
        )
    except Exception as exc:
        healed_map = {}
        log.error("sink_pass3_failed", sub_id=sub_id, error=str(exc))

    healed_total = sum(healed_map.values())
    _finalize_link_statuses(db, sub_id, summaries, healed_map, log)

    log.info(
        "sink_recon_complete",
        sub_id=sub_id, name=sub_name,
        added=add_count, updated=update_count + healed_total, removed=remove_count,
        healed=healed_total, errors=len(error_ds_names),
    )


def _finalize_link_statuses(db, sub_id, summaries, healed_map, log) -> None:
    """Write each processed link back to ENABLED with its final Reconciled message.

    Called only after the entire recon (including blocking batch upserts and any
    Pass III healing) has finished, so the link never reports success early.
    Healed files are reported as "updated" (merged count), per product intent.
    """
    for tid, counts in summaries.items():
        add_n, upd_n, rem_n = counts
        upd_n += healed_map.get(tid, 0)
        msg = f"Reconciled: +{add_n} added, ~{upd_n} updated, -{rem_n} removed"
        try:
            db.set_target_subscription_status(tid, sub_id, STATE_ENABLED, message=msg)
        except Exception as exc:  # noqa: BLE001
            log.error("sink_finalize_failed", target_id=tid, error=str(exc))


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
    fs_files, ds_link, target_id, sub_id, db, svc, queue, log, state=None,
) -> tuple:
    """Pass I.1 (FS→target) and I.2 (target→FS removal)."""
    add_count = 0
    update_count = 0
    remove_count = 0
    ds_df_rows = db.list_datafiles_for_target_subscription(target_id, sub_id)
    ds_df_by_datafile = {r.datafile_id: r for r in ds_df_rows}

    # Pass I.a — Remove from remote files that no longer exist on the FS.
    for ds_df in ds_df_rows:
        svc._check_cancel()
        df = db.get_datafile(ds_df.datafile_id)
        if df is None or df.path not in fs_files:
            try:
                svc.base_remove_datafile(ds_df.datafile_id)
                remove_count += 1
            except Exception as exc:
                log.error("sink_remove_failed", datafile_id=ds_df.datafile_id, error=str(exc))
                raise

    # Pass I.b — Add / update files that are on the FS.
    planned = 0
    for fpath in fs_files:
        df = db.get_datafile_by_path(fpath)
        ds_df = ds_df_by_datafile.get(df.id) if df else None
        if df is None or ds_df is None:
            planned += 1  # add
        elif _file_matches_db(fpath, df):
            if ds_df.hash != df.hash:
                planned += 1  # update
        else:
            planned += 1  # remote hash drift -> update

    if planned > 0:
        def _progress(done, in_flight):
            shown = min(done + in_flight, planned)
            db.set_target_subscription_status(
                target_id, sub_id, STATE_IN_PROGRESS,
                message=f"Upserting {shown} of {planned} to remote sink...",
            )
        svc.set_progress_callback(_progress)

    for fpath, st in fs_files.items():
        svc._check_cancel()
        svc._check_schedule()
        if state is not None:
            state["n"] += 1
        size = st.st_size
        mtime = st.st_mtime

        df = db.get_datafile_by_path(fpath)
        ds_df = ds_df_by_datafile.get(df.id) if df else None
        if df is None or ds_df is None:
            try:
                if df is not None and _file_matches_db(fpath, df):
                    svc.base_add_datafile(sub_id, fpath, known_hash=df.hash)
                else:
                    svc.base_add_datafile(sub_id, fpath)
                add_count += 1
            except Exception as exc:
                log.error("sink_add_failed", path=fpath, error=str(exc))
                raise
            continue

        if _file_matches_db(fpath, df):
            if ds_df and ds_df.hash != df.hash:
                try:
                    svc.base_update_datafile(df.id, df.hash)
                    update_count += 1
                except Exception as exc:
                    log.error("sink_update_failed", datafile_id=df.id, error=str(exc))
                    raise
            continue

        try:
            real_hash = compute_file_hash(fpath)
        except Exception as exc:
            log.error("sink_hash_failed", path=fpath, error=str(exc))
            raise

        if real_hash == df.hash:
            if ds_df and ds_df.hash != df.hash:
                try:
                    svc.base_update_datafile(df.id, df.hash)
                    update_count += 1
                except Exception as exc:
                    log.error("sink_update_failed", datafile_id=df.id, error=str(exc))
                    raise
        else:
            try:
                svc.base_update_datafile(df.id, real_hash)
                update_count += 1
            except Exception as exc:
                log.error("sink_update_failed", datafile_id=df.id, hash=real_hash, error=str(exc))
                raise

    # Flush any batched upsert that never crossed its size threshold (and any
    # leftover after the removal loop). No-op for sinks that do not batch.
    svc.flush()

    return add_count, update_count, remove_count


def _remove_deleted_link(ds_link, target_id, sub_id, db, sink_registry, log,
                         sub_name, error_ds_names, queue=None) -> int:
    """Remove a deleted target-subscription link: remote files + join rows + link row.

    Shared by the top-of-loop ``STATE_DELETED`` branch and the mid-recon
    cancellation handler, so already-uploaded files are cleaned up promptly
    in both cases. Returns the number of files removed.
    """
    remove_count = 0
    prog = 0
    ds_df_rows = db.list_datafiles_for_target_subscription(target_id, sub_id)
    for ds_df in ds_df_rows:
        remove_count += 1
        _invoke_remove(
            ds_link, target_id, ds_df, db, sink_registry, log, sub_name, error_ds_names
        )
        prog += 1
        _hb(db, sub_id, queue, pct=prog % 100)
    db.delete_target_subscription(target_id, sub_id)
    return remove_count


def _handle_recon_cancel(kind, ds_link, target_id, sub_id, db, sink_registry, log,
                         sub_name, error_ds_names, queue=None) -> bool:
    """React to a mid-recon ``SinkCancelledError``.

    Returns True when the WHOLE recon must abort (sub-level cancel); False
    when only this target is affected (link-level cancel).
    """
    if kind == "sub_gone" or kind == "sub_disabled":
        log.warning("sink_recon_cancelled", target_id=target_id, sub_id=sub_id, reason=kind)
        return True
    if kind == "link_removed":
        log.warning("sink_recon_cancelled", target_id=target_id, sub_id=sub_id, reason=kind)
        # Run the deferred-delete cleanup inline so the files already uploaded
        # during this pass are removed from the remote + DB right away.
        cur = db.get_target_subscription(target_id, sub_id)
        if cur is None or cur.status == STATE_DELETED:
            try:
                _remove_deleted_link(
                    ds_link, target_id, sub_id, db, sink_registry, log,
                    sub_name, error_ds_names, queue,
                )
            except Exception as exc:
                log.warning("sink_recon_cancel_cleanup_failed", target_id=target_id, error=str(exc))
        return False
    if kind == "outside_schedule":
        # Upload window is closed. NOT an error — reset the link so it stays
        # ENABLED (files remain pending on the FS) and let the next recon
        # finish the job when the window reopens.
        log.info("sink_recon_deferred", target_id=target_id, sub_id=sub_id,
                 sub_name=sub_name, reason="outside_upload_window")
        cur = db.get_target_subscription(target_id, sub_id)
        if cur and cur.status in (STATE_ENABLED, STATE_ENQUEUED, STATE_IN_PROGRESS):
            try:
                db.set_target_subscription_status(
                    target_id, sub_id, STATE_ENABLED, message="Outside upload window — deferred",
                )
            except Exception:
                pass
        return False
    # link_disabled — halt uploads for this target; rows already written are
    # kept (the files really are on the remote) and the DISABLED status is
    # left untouched. The next recon finishes the job when re-enabled.
    log.warning("sink_recon_cancelled", target_id=target_id, sub_id=sub_id, reason=kind)
    return False


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
            db.update_datafile_last_checked(df.id, checked_at)
        else:
            try:
                real_hash = compute_file_hash(df.path)
            except Exception as exc:
                log.error("sink_pass2_hash_failed", path=df.path, error=str(exc))
                raise
            db.update_datafile_stats(df.id, st.st_size, st.st_mtime, real_hash, checked_at)


def _pass3_heal_remote(sub, ds_links, db, sink_registry, queue, log, error_ds_names) -> dict:
    """Pass III — reconcile our DB against the remote target.

    For every tracked datafile whose remote copy is missing on the remote
    (regardless of *why* it drifted — external deletion, a cross-KB ``DELETE``,
    a crash mid-pass), re-push it so both sides agree again. Returns a map of
    ``{target_id: files_healed}``.
    """
    sub_id = sub.id
    sub_name = sub.name
    healed_map = {}

    for ds_link in ds_links:
        if ds_link.status not in (STATE_ENABLED, STATE_IN_PROGRESS, STATE_ENQUEUED):
            continue
        target_id = ds_link.target_id
        ds_row = db.get_target(target_id)
        if ds_row is None:
            continue
        svc = _get_service(ds_row, db, sink_registry, log)
        if svc is None or not svc.remote_target_id:
            continue
        svc.set_cancel_check(_make_cancel_check(db, sub_id, target_id, queue, {"n": 0}))

        rows = db.list_datafiles_for_target_subscription(target_id, sub_id)
        if not rows:
            continue

        # Only sinks that can enumerate their remote target are healable.
        if not callable(getattr(svc, "_kb_files", None)):
            continue

        # Snapshot of remote file ids for this KB (one read per target).
        try:
            remote_ids = {f.get("id") for f in svc._kb_files(svc.remote_target_id)}
        except Exception as exc:
            log.error("sink_pass3_remote_read_failed", target_id=target_id, error=str(exc))
            _transition_ds_error(
                ds_link, target_id, sub_id, db, log,
                sub_name, ds_row.name, str(exc), error_ds_names,
            )
            continue

        link_healed = 0
        for t_df in rows:
            svc._check_cancel()
            df = db.get_datafile(t_df.datafile_id)
            if df is None:
                continue  # Pass II already removed the datafile
            if not os.path.exists(df.path):
                continue  # missing on FS — Pass I.a / Pass II handle it
            if not t_df.remote_datafile_id:
                continue  # schema enforces NOT NULL — defensive skip
            if t_df.remote_datafile_id in remote_ids:
                continue  # present on remote — already reconciled
            try:
                # Re-push the current content; the sink's duplicate-content
                # recovery rewrites the remote to point at the fresh id.
                db.set_target_subscription_status(
                    target_id, sub_id, STATE_IN_PROGRESS,
                    message=f"Healing {t_df.datafile_id}...",
                )
                svc.base_update_datafile(df.id, df.hash)
                link_healed += 1
            except Exception as exc:
                log.error("sink_pass3_heal_failed", datafile_id=df.id, target_id=target_id, error=str(exc))
                _transition_ds_error(
                    ds_link, target_id, sub_id, db, log,
                    sub_name, ds_row.name, str(exc), error_ds_names,
                )
                break
        healed_map[target_id] = link_healed

        # Send any heal upserts the sink buffered (no-op for the non-batching
        # sinks; LightRAG batches page-bounded upserts until flush()).
        svc.flush()
    return healed_map


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


def _remove_remote_target_strict(target_id: str, db: DatabaseManager,
                                 sink_registry: SinkRegistry, log) -> None:
    """Remove the remote dataset; raises on any failure.

    Does NOT delete local rows — the caller deletes them only after this
    returns without error, so a transient remote failure leaves the target
    intact for a later retry (or a force delete).
    """
    ds_row = db.get_target(target_id)
    if ds_row is None:
        return
    svc = _get_service(ds_row, db, sink_registry, log)
    if svc is None:
        raise RuntimeError("Sink service unavailable")
    if svc.remote_target_id:
        svc.remove_target()
