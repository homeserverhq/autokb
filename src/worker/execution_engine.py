"""The Managed Execution Wrapper.

Spawns ``plugin.getData()`` as a child process via ``subprocess.Popen``
and monitors its heartbeat via a small on-disk heartbeat file and an
exception-detail file.

This module is *imported* by ``worker.py``. The Level-1 process runs
this code in-process; the Level-2 child process is a fresh Python
interpreter launched by ``subprocess.Popen`` running
``worker/_child_runner.py``.

Why subprocess + heartbeat file (and not ``multiprocessing.Process``):
    * The worker entry point is a script (``python /src/worker/worker.py``)
      so multiprocessing's spawn-launched child can't import
      ``worker.execution_engine`` reliably (sys.path would contain
      ``/src/worker`` but not ``/src``).
    * The spec only requires process isolation — subprocess satisfies it.
"""

import json
import os
import subprocess
import sys
import threading
import time
import traceback
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from sqlalchemy import text

# Ensure /src is on sys.path so sibling package imports work when this
# module is invoked as a script or as a child process target.
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_PARENT = os.path.dirname(_THIS_DIR)
sys.path = [p for p in sys.path if os.path.realpath(p) != os.path.realpath(_THIS_DIR)]
if _PARENT not in sys.path:
    sys.path.insert(0, _PARENT)
# Set __package__ so sibling imports work even when this file is loaded
# as a child-process target.
if __name__ in ("__main__", "_child_main_target") and __package__ in (None, ""):
    __package__ = "worker"

from utils.constants import (
    DEBOUNCE_PHASE_SECONDS,
    HEARTBEAT_TIMEOUT,
    LOCK_TTL,
    NOTIFY_CHANNEL,
    P_QUEUE_KEY,
    REDIS_URL,
    S_QUEUE_KEY,
    STATE_DELETED,
    STATE_DISABLED,
    STATE_ENQUEUED,
    STATE_ERROR,
    STATE_IN_PROGRESS,
)
from utils.database import DatabaseManager, Subscription
from utils.misc_utils import (
    DecryptionError,
    PasswordCipher,
    SubscriptionCancelledError,
    get_logger,
    validate_config_against_schema,
)
from utils.registry import PluginRecord
from utils.plugin_base import BaseSubscription


LOG_FILE = "/logs/worker.log"

# Per-subscription sentinel paths for the subprocess heartbeats / exceptions
HB_DIR = "/tmp/autokb-heartbeats"


@dataclass
class ExecutionResult:
    outcome: str  # 'success' | 'timeout' | 'error' | 'schema_validation' | 'load_error' | 'deleted' | 'skipped_disabled'
    exit_string: str = ""
    traceback: str = ""


def _hb_path(sub_id: str) -> str:
    return os.path.join(HB_DIR, f"{sub_id}.hb")


def _err_path(sub_id: str) -> str:
    return os.path.join(HB_DIR, f"{sub_id}.err")


def _ensure_hb_dir() -> None:
    os.makedirs(HB_DIR, exist_ok=True)


def execute_subscription(sub: Subscription, rec: PluginRecord, db: DatabaseManager, log) -> ExecutionResult:
    """Run a single subscription in a child process with heartbeat monitoring.

    Returns an ``ExecutionResult`` describing the outcome. The caller
    (the Level-1 worker loop) is responsible for updating status,
    recording EventLog, and sending SMTP notifications.
    """
    # 1. Decrypt config + validate against schema
    cipher = PasswordCipher()
    try:
        config = db.decrypt_config(sub, rec.password_fields)
    except DecryptionError as exc:
        log.warning("config_decryption_failed", sub_id=sub.id, name=sub.name, action="decrypt", result=str(exc))
        return ExecutionResult(outcome="load_error", exit_string=str(exc))
    try:
        validate_config_against_schema(config, rec.augmented_schema, rec.password_fields, enforce_required_password=False)
    except ValueError as exc:
        log.warning("config_validation_failed", sub_id=sub.id, name=sub.name, action="validate", result=str(exc))
        return ExecutionResult(outcome="schema_validation", exit_string=str(exc))

    # 2. Prepare per-subscription sentinel files for heartbeat and exception
    _ensure_hb_dir()
    hb_path = _hb_path(sub.id)
    err_path = _err_path(sub.id)
    for p in (hb_path, err_path):
        try:
            os.unlink(p)
        except FileNotFoundError:
            pass

    # 3. Spawn child subprocess
    # The decrypted run payload (which contains credentials) is passed over
    # an inherited pipe fd — never on the command line, where it would be
    # visible to any same-host process via /proc/<pid>/cmdline and `ps`.
    run_payload = json.dumps({
        "file_path": rec.file_path,
        "config": config,
        "sub_id": sub.id,
        "sub_name": sub.name,
        "db_url": DATABASE_URL_FOR_CHILD,
        "password_field_names": rec.password_fields,
        "hb_path": hb_path,
        "err_path": err_path,
    }).encode("utf-8")
    read_fd, write_fd = os.pipe()
    os.set_inheritable(read_fd, True)
    child_env = {
        **os.environ,
        "PYTHONPATH": "/src",
        "PYTHONUNBUFFERED": "1",
        "PYTHONDONTWRITEBYTECODE": "1",
        "KB_NO_STDOUT_LOG": "1",
        "AUTOKB_CFG_FD": str(read_fd),
    }
    try:
        proc = subprocess.Popen(
            [sys.executable, "-B", "-m", "worker._child_runner"],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd="/",
            env=child_env,
            pass_fds=(read_fd,),
        )
    except Exception as exc:
        os.close(read_fd)
        os.close(write_fd)
        log.error("child_spawn_failed", sub_id=sub.id, name=sub.name, action="spawn", result=str(exc))
        return ExecutionResult(outcome="load_error", exit_string=f"Failed to spawn child: {exc}")
    os.close(read_fd)  # the child now owns the read end
    try:
        os.write(write_fd, run_payload)
    finally:
        os.close(write_fd)  # closing the write end signals EOF to the child
    log.debug("child_spawned", sub_id=sub.id, name=sub.name, action="spawn", result=f"pid={proc.pid}")

    # 4. Watcher thread for heartbeat timeout
    stop_event = threading.Event()
    killed_by_watcher = threading.Event()

    def _kill_child(reason: str, is_user_cancel: bool = False) -> None:
        """Terminate the child process and perform post-kill housekeeping.

        ``is_user_cancel=True`` when the watcher observed a user-initiated
        DISABLED/DELETED status: the child is force-killed (it ignored the
        cooperative cancellation) but NO EventLog / SMTP heartbeat-timeout
        alert is produced — that is reserved for genuine heartbeat timeouts.
        """
        if proc.poll() is not None:
            # Child already exited (e.g. between this tick and the kill) —
            # let the normal return-code path handle it; never double-report.
            return
        killed_by_watcher.set()
        log.warning(
            "execution_timed_out",
            sub_id=sub.id, name=sub.name, action="timeout",
            result=reason,
        )
        try:
            proc.terminate()
        except Exception:
            pass
        time.sleep(0.5)
        if proc.poll() is None:
            try:
                proc.kill()
            except Exception:
                pass
        if is_user_cancel:
            # Force-killed an uncooperative run after a user DISABLED/DELETED.
            # The force-kill IS recorded (exit_code=2, per spec §5.2) but no
            # "Heartbeat timeout" SMTP alert is sent — the user initiated the
            # cancellation, so alerting them about it is false noise. The
            # user-preserved status is left untouched.
            try:
                db.record_execution(sub.id, 2, reason)
            except Exception:
                pass
            return
        # Mark ERROR — but only if the status is not DISABLED nor
        # DELETED, to respect any concurrent user action (spec §5.2).
        try:
            with db.engine.begin() as conn:
                res = conn.execute(
                    text("UPDATE subscriptions SET status = :st, last_updated = NOW(), last_error = :err "
                         "WHERE id = :sid AND status NOT IN ('DELETED', 'DISABLED')"),
                    {"st": STATE_ERROR, "sid": sub.id,
                     "err": reason},
                )
                log.warning("watchdog_set_error", sub_id=sub.id, name=sub.name,
                            action="timeout_override", result=f"rowcount={res.rowcount}")
        except Exception as exc:
            log.warning("watchdog_set_error_failed", sub_id=sub.id, name=sub.name,
                        action="timeout_override", result=str(exc))
        try:
            with db.engine.begin() as conn:
                conn.execute(text(f"SELECT pg_notify('{NOTIFY_CHANNEL}', :sid)"), {"sid": sub.id})
        except Exception as exc:
            log.debug("watchdog_notify_failed", sub_id=sub.id, name=sub.name,
                      action="timeout_notify", result=str(exc))
        try:
            db.record_execution(sub.id, 2, reason)
        except Exception:
            pass
        try:
            _send_smtp_for_worker(
                subject=f"[AutoKB] Heartbeat timeout: {sub.name}",
                body=(
                    f"Subscription {sub.name!r} (id={sub.id}) was terminated.\n"
                    f"Reason: {reason}"
                ),
            )
        except Exception:
            pass

    def watcher_thread():
        nonlocal proc
        start_time = time.time()
        tick_s = max(HEARTBEAT_TIMEOUT / 10.0, 10)
        while not stop_event.is_set():
            for _ in range(10):
                if stop_event.is_set():
                    return
                time.sleep(tick_s)
                if stop_event.is_set():
                    return

                # --- DB status check (every tick) ---
                try:
                    cur_sub = db.get_subscription(sub.id)
                    if cur_sub is not None and cur_sub.status in (STATE_DISABLED, STATE_DELETED):
                        _kill_child(f"cancelled_by_user status={cur_sub.status}", is_user_cancel=True)
                        return
                except Exception:
                    pass

                # --- Heartbeat check (every tick) ---
                age = None
                try:
                    mtime = os.path.getmtime(hb_path)
                    age = time.time() - mtime
                except FileNotFoundError:
                    if proc.poll() is not None:
                        return
                    if time.time() - start_time <= HEARTBEAT_TIMEOUT:
                        continue
                    # No heartbeat file and timeout elapsed — fall through to kill

                if age is not None and age < HEARTBEAT_TIMEOUT:
                    # Heartbeat is fresh — refresh the safety lock TTL
                    try:
                        import redis as _redis
                        rc = _redis.Redis.from_url(REDIS_URL, decode_responses=True, socket_timeout=2)
                        rc.expire(f"autokb:lock:{sub.id}", LOCK_TTL)
                    except Exception:
                        pass
                    continue

                # --- Kill ---
                if age is not None:
                    _kill_child(f"heartbeat age={age:.1f}s exceeds HEARTBEAT_TIMEOUT={HEARTBEAT_TIMEOUT}")
                else:
                    _kill_child(f"no heartbeat file after {time.time() - start_time:.1f}s")
                return

    watcher = threading.Thread(target=watcher_thread, name=f"watcher-{sub.id[:8]}", daemon=True)
    watcher.start()

    # 5. Wait for child to complete
    try:
        proc.wait()  # watcher thread handles timeout via heartbeat
    except subprocess.TimeoutExpired:
        log.error("child_absolute_timeout", sub_id=sub.id, name=sub.name, action="wait", result="killing")
        try:
            proc.kill()
        except Exception:
            pass
        try:
            proc.wait(timeout=5)
        except Exception:
            pass
    except Exception:
        pass

    # 6. Signal watcher to stop
    stop_event.set()
    try:
        watcher.join(timeout=5.0)
    except Exception:
        pass

    # 7. Did the watcher kill the process?
    if killed_by_watcher.is_set():
        # Cleanup sentinel files before returning
        for p in (hb_path, err_path):
            try:
                os.unlink(p)
            except FileNotFoundError:
                pass
        return ExecutionResult(outcome="timeout", exit_string=f"Heartbeat timeout — process terminated after {HEARTBEAT_TIMEOUT}s")

    # 8. Cancellation: child exited 0 + status DISABLED/DELETED → skip log
    if proc.returncode == 0:
        cur = db.get_subscription(sub.id)
        if cur is not None and cur.status in (STATE_DISABLED, STATE_DELETED):
            # Cleanup sentinel files
            for p in (hb_path, err_path):
                try:
                    os.unlink(p)
                except FileNotFoundError:
                    pass
            return ExecutionResult(outcome="deleted" if cur.status == STATE_DELETED else "skipped_disabled")
        # Cleanup sentinel files
        for p in (hb_path, err_path):
            try:
                os.unlink(p)
            except FileNotFoundError:
                pass
        return ExecutionResult(outcome="success")

    # 9. Non-zero exit: read the exception file and stderr
    exit_string = f"Subscription failed with exit code {proc.returncode}"
    tb_text = ""
    err_read = False
    try:
        with open(err_path, "r", encoding="utf-8") as f:
            raw = f.read()
            if raw.strip():
                err_data = json.loads(raw)
                etype = err_data.get("exception_type", "Exception")
                emsg = err_data.get("exception_message", "")
                tb_text = err_data.get("traceback", "")
                exit_string = f"{etype}: {emsg}"
                err_read = True
    except FileNotFoundError:
        pass
    except Exception as exc:
        log.debug("err_file_read_failed", sub_id=sub.id, name=sub.name, action="read_err", result=str(exc))
    if not err_read:
        # No exception file — try to read stderr
        try:
            stderr_data = proc.stderr.read().decode("utf-8", errors="replace") if proc.stderr else ""
            if stderr_data:
                last_line = stderr_data.splitlines()[-1] if stderr_data.strip() else ""
                if last_line:
                    exit_string = last_line
                tb_text = stderr_data
        except Exception:
            pass
    if tb_text:
        log.error(
            "execution_traceback",
            sub_id=sub.id, name=sub.name, action="execute", result=exit_string, traceback=tb_text,
        )

    # 10. Cleanup sentinel files (AFTER we have read the err file)
    for p in (hb_path, err_path):
        try:
            os.unlink(p)
        except FileNotFoundError:
            pass

    return ExecutionResult(outcome="error", exit_string=exit_string, traceback=tb_text)


def registry_load_class_for_execution(rec: PluginRecord):
    """Re-load the plugin class fresh from disk for a single execution."""
    import importlib.util
    import inspect
    spec = importlib.util.spec_from_file_location(f"_plugin_exec_{rec.plugin_id}", rec.file_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load spec for {rec.file_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    for _, obj in inspect.getmembers(module, inspect.isclass):
        if obj is BaseSubscription:
            continue
        if issubclass(obj, BaseSubscription) and obj.__module__ == module.__name__:
            return obj
    raise RuntimeError(f"No BaseSubscription subclass in {rec.file_path}")


DATABASE_URL_FOR_CHILD = os.environ.get("DATABASE_URL", "postgresql://autokb:autokb@autokb-db:5432/autokb")


def _child_main(file_path: str, config: Dict[str, Any], sub_id: str, sub_name: str,
                db_url: str, password_field_names: List[str],
                hb_path: Optional[str] = None, err_path: Optional[str] = None) -> None:
    """Entry point for the Level-2 child process.

    Args ``hb_path`` and ``err_path`` are passed as part of the args
    blob by ``_child_runner.py``. They are kept as optional for
    backwards compatibility.
    """
    log = get_logger(f"worker-child", LOG_FILE)
    # Load the plugin class fresh from disk
    cls = None
    try:
        import importlib.util
        import inspect
        spec = importlib.util.spec_from_file_location(f"_plugin_exec_{os.path.basename(file_path)}", file_path)
        if spec is None or spec.loader is None:
            raise RuntimeError(f"Could not load spec for {file_path}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        for _, obj in inspect.getmembers(module, inspect.isclass):
            if obj is BaseSubscription:
                continue
            if issubclass(obj, BaseSubscription) and obj.__module__ == module.__name__:
                cls = obj
                break
        if cls is None:
            raise RuntimeError(f"No BaseSubscription subclass in {file_path}")
        instance = cls()
    except Exception as exc:  # noqa: BLE001
        _emit_exception_to_file(err_path, exc)
        log.error("child_load_failed", sub_id=sub_id, name=sub_name, action="load", result=str(exc),
                  traceback=traceback.format_exc())
        sys.exit(1)

    instance._subscription_id = sub_id
    instance._subscription_name = sub_name

    # Set up a fresh DB session inside this process
    db = DatabaseManager(db_url, log_file=LOG_FILE, component=f"db-child")
    try:
        db.engine.dispose(close=False)
    except Exception:
        pass

    def progress_callback(pct: int, message: str = None) -> None:
        # Check current subscription status — if disabled or deleted, raise
        sub = db.get_subscription(sub_id)
        cur_status = sub.status if sub is not None else "<gone>"
        if sub is None:
            raise SubscriptionCancelledError("Subscription deleted")
        if sub.status in (STATE_DISABLED,):
            log.debug("cancellation_detected", sub_id=sub_id, name=sub_name, action="cancel", result=f"pct={pct} status={sub.status}")
            raise SubscriptionCancelledError("Subscription disabled")
        if sub.status == STATE_DELETED:
            raise SubscriptionCancelledError("Subscription deleted")
        # Update heartbeat
        try:
            db.update_heartbeat_and_progress(sub_id, pct)
        except Exception:
            pass
        if message:
            try:
                db.update_last_message(sub_id, message)
            except Exception:
                pass
        # Touch heartbeat file for the watcher's mtime check
        if hb_path:
            try:
                with open(hb_path, "w") as f:
                    f.write(str(pct))
            except Exception:
                pass

    try:
        progress_callback(0)
    except SubscriptionCancelledError:
        sys.exit(0)

    try:
        try:
            instance.getData(config, progress_callback)
        except SubscriptionCancelledError:
            sys.exit(0)
        try:
            progress_callback(100)
        except SubscriptionCancelledError:
            sys.exit(0)
    except SystemExit:
        raise
    except BaseException as exc:  # noqa: BLE001
        try:
            db.update_last_message(sub_id, str(exc))
        except Exception:
            pass
        _emit_exception_to_file(err_path, exc)
        try:
            log.error("child_exception", sub_id=sub_id, name=sub_name, action="execute", result=str(exc),
                      traceback=traceback.format_exc())
        except Exception:
            pass
        sys.exit(1)
    finally:
        try:
            db.engine.dispose(close=True)
        except Exception:
            pass
        # Best-effort cleanup of heartbeat file
        if hb_path:
            try:
                os.unlink(hb_path)
            except Exception:
                pass


def _emit_exception_to_file(err_path: Optional[str], exc: BaseException) -> None:
    if not err_path:
        return
    try:
        with open(err_path, "w", encoding="utf-8") as f:
            f.write(json.dumps({
                "exception_type": type(exc).__name__,
                "exception_message": str(exc),
                "traceback": traceback.format_exc(),
            }))
    except Exception:
        pass


def _send_smtp_for_worker(subject: str, body: str) -> None:
    """Send an SMTP notification from the worker (best-effort, never raises)."""
    from utils.misc_utils import send_smtp_notification
    smtp_config = {
        "smtp_host": os.environ.get("SMTP_HOST", ""),
        "smtp_port": int(os.environ.get("SMTP_PORT", "25")),
        "smtp_user": os.environ.get("SMTP_USER", ""),
        "smtp_pass": os.environ.get("SMTP_PASS", ""),
        "from_addr": os.environ.get("SMTP_FROM", "autokb@localhost"),
        "to_addr": os.environ.get("SMTP_NOTIFY_EMAIL", ""),
        "use_tls": os.environ.get("SMTP_USE_TLS", "True").lower() == "true",
        "use_ssl": os.environ.get("SMTP_USE_SSL", "False").lower() == "true",
    }
    try:
        send_smtp_notification(subject=subject, body=body, **smtp_config)
    except Exception:
        pass
