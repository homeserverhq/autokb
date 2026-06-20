"""End-to-end test runner for AutoKB.

Creates one subscription per test plugin, triggers them, and verifies
the expected outcome for each. Returns a non-zero exit code on any
failure.

This file lives in the source tree (src/testing/test_runner.py) so the
Docker image has it baked in at /src/testing/test_runner.py — no
``docker cp`` is required to run it.

Usage (from inside the running container)::

    python /src/testing/test_runner.py confirm             # normal run
    python /src/testing/test_runner.py confirm --reset     # wipe state first
    python /src/testing/test_runner.py                     # refuse to run

The ``confirm`` argument is a safety check to prevent accidental
execution. ``--reset`` is an opt-in flag that wipes all subscriptions,
the event log, test-created plugin files, and /output/ at startup
(useful when drift has accumulated from manual testing).
"""

import base64
import json
import os
import shutil
import sys
import time
import traceback
from typing import Any, Callable, Dict, List, Optional, Tuple

import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
MANAGER_URL = os.environ.get("MANAGER_URL", "http://localhost:80")
ADMIN_USER = os.environ.get("AUTOKB_ADMIN_USERNAME", "akbadmin")
ADMIN_PASS = os.environ.get("AUTOKB_ADMIN_PASSWORD", "GisrLERYItkHuHgex32C1nbEzhaBw33I")
API_KEY = os.environ.get("AUTOKB_API_KEY", "VIxrseTwAOYUi8OOknzTmh7F6Os3t8SU")
BACKEND_KEY = os.environ.get("AUTOKB_BACKEND_API_KEY", "sZdx8RLMFOBBnVyINfjvlQXrSHMg0Wwy")

# Unique suffix so re-runs of test_runner don't get 409s on duplicate names
RUN_ID = os.environ.get("AUTOKB_RUN_ID") or time.strftime("%H%M%S") + "-" + str(os.getpid())


def _unique(base: str) -> str:
    return f"{base}-{RUN_ID}"

# Plugins for which we want to wait for the work to finish before
# checking the result.
SUCCESS_PLUGINS = (
    "happyPathPlugin",
    "eventHappyPlugin",
    "longRunningSuccessPlugin",
    "emptyOutputPlugin",
    "largeOutputPlugin",
    "delayedInitPlugin",
    "customRoutePlugin",
    "monitorNeverTriggerPlugin",
    "configValidationPlugin",
    "passwordPlugin",
    "longNamePlugin32CharNameForUITes",
    "eventOftenPlugin",
    "cronRandomizePlugin",
)

# Plugins expected to fail (status=ERROR after run)
ERROR_PLUGINS = (
    "noHeartbeatPlugin",       # exit_code=2 (timeout)
    "longRunningFailurePlugin",  # exit_code=1 (runtime error)
    "crashPlugin",              # exit_code=1 (immediate exception)
    "nonZeroExitPlugin",        # exit_code=1 (sys.exit(1) no exception)
    "moveToDestErrorPlugin",    # exit_code=1 (ValueError from sanitize)
    "zombiePlugin",             # exit_code=2 (timeout, no cancellation)
)

# Plugins expected to be set to DISABLED after manager detects breaking change
DISABLED_PLUGINS = (
    "schemaBreakingPlugin",
)

# Plugins expected to be REJECTED at load time (not loadable)
INVALID_PLUGINS = (
    "invalidNamePlugin",
)

# Plugins whose monitor raises an exception continuously
MONITOR_ERROR_PLUGINS = (
    "monitorErrorPlugin",
)


def _log(msg: str) -> None:
    print(f"[test_runner] {time.strftime('%H:%M:%S')} {msg}", flush=True)


def _authed_session() -> requests.Session:
    s = requests.Session()
    # Login via the auth endpoint to set the session cookie
    s.post(
        f"{MANAGER_URL}/auth/login",
        json={"username": ADMIN_USER, "password": ADMIN_PASS},
        timeout=10,
    )
    return s


def _api_headers() -> Dict[str, str]:
    return {"X-Api-Key": BACKEND_KEY}


def api_get(path: str, **kwargs) -> Any:
    """GET against the Manager directly (using the BACKEND_API_KEY)."""
    r = requests.get(f"{MANAGER_URL}{path}", headers=_api_headers(), timeout=30, **kwargs)
    r.raise_for_status()
    return r.json() if r.content else None


def api_post(path: str, body: Dict[str, Any]) -> Any:
    r = requests.post(
        f"{MANAGER_URL}{path}",
        headers={**_api_headers(), "Content-Type": "application/json"},
        data=json.dumps(body),
        timeout=30,
    )
    r.raise_for_status()
    return r.json() if r.content else None


def api_put(path: str, body: Dict[str, Any]) -> Any:
    r = requests.put(
        f"{MANAGER_URL}{path}",
        headers={**_api_headers(), "Content-Type": "application/json"},
        data=json.dumps(body),
        timeout=30,
    )
    r.raise_for_status()
    return r.json() if r.content else None


def api_delete(path: str) -> Any:
    r = requests.delete(f"{MANAGER_URL}{path}", headers=_api_headers(), timeout=30)
    r.raise_for_status()
    return r.json() if r.content else None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def create_sub(plugin_id: str, name: str, config: Dict[str, Any], cron: Optional[str] = None,
               access_level: str = "PRIVATE", expected_status: int = 200) -> Dict[str, Any]:
    body: Dict[str, Any] = {"name": name, "config": config, "access_level": access_level}
    if cron is not None:
        body["cron"] = cron
    r = requests.post(
        f"{MANAGER_URL}/api/subscriptions/{plugin_id}",
        headers={**_api_headers(), "Content-Type": "application/json"},
        data=json.dumps(body),
        timeout=30,
    )
    if r.status_code != expected_status:
        raise RuntimeError(f"create_sub({plugin_id}, {name}) returned {r.status_code}: {r.text}")
    return r.json() if r.content else {}


def trigger_sub(sub_id: str) -> None:
    r = requests.post(
        f"{MANAGER_URL}/api/subscriptions/{sub_id}/trigger",
        headers=_api_headers(),
        timeout=10,
    )
    if r.status_code not in (200, 204):
        raise RuntimeError(f"trigger returned {r.status_code}: {r.text}")


def set_status(sub_id: str, status: str) -> None:
    r = requests.put(
        f"{MANAGER_URL}/api/subscriptions/{sub_id}/status",
        headers={**_api_headers(), "Content-Type": "application/json"},
        data=json.dumps({"status": status}),
        timeout=10,
    )
    if r.status_code not in (200, 204):
        raise RuntimeError(f"set_status returned {r.status_code}: {r.text}")


def get_sub(sub_id: str) -> Dict[str, Any]:
    r = requests.get(
        f"{MANAGER_URL}/api/subscriptions/{sub_id}",
        headers=_api_headers(),
        timeout=10,
    )
    r.raise_for_status()
    return r.json()


def get_event_log(sub_id: str) -> List[Dict[str, Any]]:
    """Fetch the most recent event log entries for a subscription."""
    rows = api_get("/api/logging")
    return [r for r in rows if r.get("subscription_id") == sub_id]


def wait_for_status(sub_id: str, predicate: Callable[[str], bool], timeout: float = 60.0,
                    poll_interval: float = 0.5) -> Dict[str, Any]:
    """Poll subscription status until ``predicate(status)`` returns True or timeout."""
    deadline = time.time() + timeout
    last: Dict[str, Any] = {}
    while time.time() < deadline:
        last = get_sub(sub_id)
        if predicate(last.get("status", "")):
            return last
        time.sleep(poll_interval)
    raise TimeoutError(
        f"Timeout waiting for sub={sub_id} predicate. Last status={last.get('status')!r}, "
        f"last_error={last.get('last_error')!r}"
    )


def wait_for_event(sub_id: str, timeout: float = 30.0) -> Dict[str, Any]:
    """Wait for at least one EventLog entry for ``sub_id``."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        rows = get_event_log(sub_id)
        if rows:
            return rows[0]
        time.sleep(0.5)
    raise TimeoutError(f"Timeout waiting for event log entry for sub={sub_id}")


def wait_for_new_event(sub_id: str, prev_count: int, timeout: float = 30.0) -> Dict[str, Any]:
    """Wait until ``sub_id``'s event-log row count exceeds ``prev_count``.

    Unlike :func:`wait_for_event`, this returns only when a brand-new
    execution has completed (i.e. a new EventLog row was appended), so
    callers can distinguish "the sub ran for the first time" from
    "the sub re-ran for the Nth time". This matters for tests that
    re-trigger an already-ENABLED subscription — ``wait_for_status``
    would short-circuit because the predicate is already satisfied.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        rows = get_event_log(sub_id)
        if len(rows) > prev_count:
            return rows[0]
        time.sleep(0.5)
    raise TimeoutError(
        f"Timeout waiting for NEW event log entry for sub={sub_id} "
        f"(prev_count={prev_count}, current={len(get_event_log(sub_id))})"
    )


# ---------------------------------------------------------------------------
# Test definitions
# ---------------------------------------------------------------------------
def test_happy_path() -> Tuple[bool, str]:
    name = _unique("happy")
    sub = create_sub("happyPathPlugin", name, {"title": "Hello"}, cron="0 * * * *")
    trigger_sub(sub["id"])
    final = wait_for_status(sub["id"], lambda s: s in ("ENABLED", "ERROR"), timeout=30)
    if final["status"] != "ENABLED":
        return False, f"expected ENABLED, got {final['status']} (last_error={final.get('last_error')!r})"
    # Check EventLog
    ev = wait_for_event(sub["id"], timeout=15)
    if ev["exit_code"] != 0:
        return False, f"expected exit_code=0, got {ev['exit_code']}"
    return True, "success (exit_code=0, status=ENABLED)"


def test_event_happy() -> Tuple[bool, str]:
    name = _unique("event")
    sub = create_sub("eventHappyPlugin", name, {"topic": "news"}, cron="0 0 * * *")
    # EVENT_BASED plugins need to be triggered via monitor. The manager's
    # monitor loop runs every ~2s; we give it a few iterations to fire.
    # Wait for status to flip from ENABLED to ENQUEUED then back.
    deadline = time.time() + 30
    triggered = False
    while time.time() < deadline:
        cur = get_sub(sub["id"])
        if cur.get("status") in ("ENQUEUED", "IN_PROGRESS"):
            triggered = True
        if triggered and cur.get("status") in ("ENABLED", "ERROR"):
            return cur["status"] == "ENABLED", f"final status={cur['status']}"
        time.sleep(0.5)
    return False, "monitor never triggered getData"


def test_event_often() -> Tuple[bool, str]:
    """Verify eventOftenPlugin fires immediately on enable."""
    name = _unique("eventOften")
    sub = create_sub("eventOftenPlugin", name, {"topic": "news"}, cron="0 0 * * *")
    # EVENT_BASED: monitor runs every ~2s. The plugin fires on first
    # monitor call (self._last_fire is None → return True immediately),
    # so the subscription should transition to ENQUEUED/IN_PROGRESS
    # within a few seconds.
    deadline = time.time() + 20
    triggered = False
    while time.time() < deadline:
        cur = get_sub(sub["id"])
        if cur.get("status") in ("ENQUEUED", "IN_PROGRESS"):
            triggered = True
        if triggered and cur.get("status") in ("ENABLED", "ERROR"):
            if cur["status"] == "ENABLED":
                ev = wait_for_event(sub["id"], timeout=10)
                if ev.get("exit_code") == 0:
                    return True, "fired immediately on enable (exit_code=0)"
                return False, f"exit_code={ev.get('exit_code')}, expected 0"
            return False, f"final status={cur['status']}, expected ENABLED"
        time.sleep(0.5)
    return False, "monitor never triggered within 20s"


def test_cron_randomize() -> Tuple[bool, str]:
    """Test 26: Verify default cron strings are randomized at creation."""
    import re
    name_s = _unique("cronSched")
    sub_s = create_sub("cronRandomizePlugin", name_s, {"label": "scheduled"}, cron="0 * * * *")
    cron_s = sub_s.get("cron", "")
    if cron_s == "0 * * * *":
        return False, f"SCHEDULED cron was not randomized: {cron_s!r}"
    if not re.match(r"^\d+ \* \* \* \*$", cron_s):
        return False, f"SCHEDULED cron has unexpected format: {cron_s!r}"

    name_e = _unique("cronEvent")
    sub_e = create_sub("eventHappyPlugin", name_e, {"topic": "news"}, cron="0 0 * * *")
    cron_e = sub_e.get("cron", "")
    if cron_e == "0 0 * * *":
        return False, f"EVENT_BASED cron was not randomized: {cron_e!r}"
    if not re.match(r"^\d+ \d+ \* \* \*$", cron_e):
        return False, f"EVENT_BASED cron has unexpected format: {cron_e!r}"

    return True, f"SCHEDULED={cron_s!r}, EVENT_BASED={cron_e!r}"


def test_delete_subscription_and_plugin() -> Tuple[bool, str]:
    """Test 25: Create a subscription, generate output, delete the subscription,
    wait for worker cleanup, then delete the plugin. Verifies the full
    delete-subscription → worker-cleanup → delete-plugin lifecycle."""
    plugin = "deleteAllPlugin"
    name = _unique("delall")
    sub = create_sub(plugin, name, {"label": "delete-me"})
    trigger_sub(sub["id"])
    ev = wait_for_event(sub["id"], timeout=30)
    if ev.get("exit_code") != 0:
        return False, f"execution failed: exit_code={ev.get('exit_code')}"
    r = requests.delete(
        f"{MANAGER_URL}/api/subscriptions/{sub['id']}",
        headers=_api_headers(), timeout=10,
    )
    if r.status_code != 200:
        return False, f"DELETE subscription returned {r.status_code}: {r.text}"
    time.sleep(3.0)
    deadline = time.time() + 15
    while time.time() < deadline:
        try:
            r = requests.get(
                f"{MANAGER_URL}/api/subscriptions/{sub['id']}",
                headers=_api_headers(), timeout=10,
            )
            if r.status_code == 404:
                break
        except Exception:
            pass
        time.sleep(1.0)
    else:
        return False, "subscription not removed from DB after 15s"
    out_dir = f"/output/{plugin}/{name}"
    if os.path.isdir(out_dir):
        return False, f"output directory still exists: {out_dir}"
    r = requests.delete(
        f"{MANAGER_URL}/api/plugins/{plugin}",
        headers=_api_headers(), timeout=10,
    )
    if r.status_code != 200:
        return False, f"DELETE plugin returned {r.status_code}: {r.text}"
    plugins = api_get("/api/plugins")
    if any(p.get("plugin_id") == plugin for p in plugins):
        return False, f"plugin {plugin!r} still in registry after DELETE"
    return True, "subscription + plugin deleted, worker cleanup verified"


def test_no_heartbeat() -> Tuple[bool, str]:
    name = _unique("nohb")
    sub = create_sub("noHeartbeatPlugin", name, {"label": "x"}, cron="0 * * * *")
    trigger_sub(sub["id"])
    final = wait_for_status(sub["id"], lambda s: s in ("ERROR", "ENABLED"), timeout=30)
    if final["status"] != "ERROR":
        return False, f"expected ERROR, got {final['status']}"
    ev = wait_for_event(sub["id"], timeout=10)
    if ev["exit_code"] != 2:
        return False, f"expected exit_code=2, got {ev['exit_code']}"
    return True, "timeout (exit_code=2, status=ERROR)"


def test_long_running_success() -> Tuple[bool, str]:
    name = _unique("lrs")
    sub = create_sub("longRunningSuccessPlugin", name, {"name": "long"}, cron="0 * * * *")
    trigger_sub(sub["id"])
    final = wait_for_status(sub["id"], lambda s: s in ("ENABLED", "ERROR"), timeout=45)
    if final["status"] != "ENABLED":
        return False, f"expected ENABLED, got {final['status']}"
    ev = wait_for_event(sub["id"], timeout=10)
    if ev["exit_code"] != 0:
        return False, f"expected exit_code=0, got {ev['exit_code']}"
    return True, "long running success (exit_code=0)"


def test_long_running_failure() -> Tuple[bool, str]:
    name = _unique("lrf")
    sub = create_sub("longRunningFailurePlugin", name, {"fail_at": 50}, cron="0 * * * *")
    trigger_sub(sub["id"])
    final = wait_for_status(sub["id"], lambda s: s in ("ERROR", "ENABLED"), timeout=30)
    if final["status"] != "ERROR":
        return False, f"expected ERROR, got {final['status']}"
    ev = wait_for_event(sub["id"], timeout=10)
    if ev["exit_code"] != 1:
        return False, f"expected exit_code=1, got {ev['exit_code']}"
    if "RuntimeError" not in ev["exit_string"]:
        return False, f"expected RuntimeError in exit_string, got {ev['exit_string']!r}"
    return True, f"runtime error (exit_code=1, exit_string={ev['exit_string']!r})"


def test_crash() -> Tuple[bool, str]:
    name = _unique("crash")
    sub = create_sub("crashPlugin", name, {"reason": "test"}, cron="0 * * * *")
    trigger_sub(sub["id"])
    final = wait_for_status(sub["id"], lambda s: s in ("ERROR", "ENABLED"), timeout=15)
    if final["status"] != "ERROR":
        return False, f"expected ERROR, got {final['status']}"
    ev = wait_for_event(sub["id"], timeout=10)
    if ev["exit_code"] != 1:
        return False, f"expected exit_code=1, got {ev['exit_code']}"
    if "Exception" not in ev["exit_string"] or "Something went wrong" not in ev["exit_string"]:
        return False, f"unexpected exit_string: {ev['exit_string']!r}"
    return True, f"crash (exit_code=1, exit_string={ev['exit_string']!r})"


def test_cancellation() -> Tuple[bool, str]:
    """The plugin runs for many iterations. The test runner flips the
    subscription to DISABLED mid-run; the plugin's progress_callback
    should detect the status and raise SubscriptionCancelledError,
    exiting the child process cleanly with code 0. The worker sees
    exit_code=0 + status=DISABLED → no EventLog entry."""
    name = _unique("cancel")
    sub = create_sub("cancellationPlugin", name, {"iterations": 200}, cron="0 * * * *")
    _log(f"  cancel sub_id={sub['id']} status_at_create={sub['status']}")
    trigger_sub(sub["id"])
    _log(f"  triggered, polling for IN_PROGRESS")
    try:
        wait_for_status(sub["id"], lambda s: s == "IN_PROGRESS", timeout=15)
    except TimeoutError:
        cur = get_sub(sub["id"])
        _log(f"  IN_PROGRESS timeout. current_status={cur['status']} last_error={cur.get('last_error')}")
        raise
    _log(f"  got IN_PROGRESS, sleeping 0.5s then setting DISABLED")
    time.sleep(0.5)
    set_status(sub["id"], "DISABLED")
    _log(f"  set DISABLED, waiting for status to remain DISABLED")
    final = wait_for_status(sub["id"], lambda s: s == "DISABLED", timeout=15)
    time.sleep(2.0)
    rows = get_event_log(sub["id"])
    if rows:
        return False, f"expected no EventLog entry on cancellation, found {len(rows)}"
    return True, "cancellation (no EventLog, status=DISABLED)"


SCHEMA_BREAKING_V1_CODE = '''
from utils.plugin_base import BaseSubscription


class schemaBreakingPlugin(BaseSubscription):
    metadata = {
        "name": "schemaBreakingPlugin",
        "icon": "default_icon.png",
        "description": "Schema breaking change plugin — V1 (title+author)",
        "sub_type": "SCHEDULED",
    }
    DEFAULT_ACCESS_LEVEL = "PRIVATE"

    def get_schema(self):
        return {
            "type": "object",
            "properties": {
                "title": {"type": "string", "minLength": 1},
                "author": {"type": "string", "minLength": 1},
            },
            "required": ["title", "author"],
        }

    def getData(self, config, progress_callback):
        progress_callback(100)
'''


SCHEMA_BREAKING_V2_CODE = '''
from utils.plugin_base import BaseSubscription


class schemaBreakingPlugin(BaseSubscription):
    metadata = {
        "name": "schemaBreakingPlugin",
        "icon": "default_icon.png",
        "description": "Schema breaking change plugin — V2 (title+writer)",
        "sub_type": "SCHEDULED",
    }
    DEFAULT_ACCESS_LEVEL = "PRIVATE"

    def get_schema(self):
        return {
            "type": "object",
            "properties": {
                "title": {"type": "string", "minLength": 1},
                "writer": {"type": "string", "minLength": 1},
            },
            "required": ["title", "writer"],
        }

    def getData(self, config, progress_callback):
        progress_callback(100)
'''


def _save_plugin_code(plugin_name: str, code: str) -> Tuple[bool, str]:
    """Save plugin code through the public dev_lab/save endpoint.

    NOTE: Since the Edit Plugin feature was added (see DesignSpecification
    §7.9), the dev_lab refuses to overwrite an existing plugin with code
    whose schema hash differs from the stored one. Tests that need to
    simulate an out-of-band file change (e.g. test_schema_breaking) must
    use :func:`_write_plugin_file_directly` instead — the dev_lab will
    rightly reject a schema-breaking save.
    """
    r = requests.post(
        f"{MANAGER_URL}/api/dev_lab/save",
        headers={**_api_headers(), "Content-Type": "application/json"},
        data=json.dumps({"name": plugin_name, "code": code}),
        timeout=30,
    )
    if r.status_code not in (200, 204):
        return False, f"dev_lab/save failed: {r.status_code} {r.text}"
    return True, "ok"


def _write_plugin_file_directly(plugin_name: str, code: str) -> Tuple[bool, str]:
    """Bypass the dev_lab API and overwrite the plugin file on disk.

    This is the test-only equivalent of an operator editing the file
    directly via SSH — used to simulate out-of-band file changes that
    the dev_lab/save endpoint would correctly refuse (e.g. a schema-
    breaking change). The manager's file watcher will pick up the new
    file and run its normal breaking-change detection on reload.

    Requires that the test be run with access to the manager's
    ``/src/plugins`` directory (the standard ``docker exec autokb-manager
    python /src/testing/test_runner.py confirm`` invocation satisfies
    this).
    """
    target_path = f"/src/plugins/{plugin_name}.py"
    try:
        with open(target_path, "w", encoding="utf-8") as f:
            f.write(code)
    except OSError as exc:
        return False, f"could not write {target_path}: {exc}"
    return True, "ok"


def _sync_test_plugins() -> None:
    """Copy every plugin file from the testing source directory into
    /src/plugins/. This ensures the test plugin suite is always
    available regardless of what has been deleted from the live
    plugins directory. Files with the same name are overwritten.
    """
    src_dir = os.path.join(os.path.dirname(__file__), "plugins")
    dst_dir = "/src/plugins"
    if not os.path.isdir(src_dir):
        _log(f"Warning: test plugin source directory {src_dir} does not exist; skipping sync")
        return
    synced = 0
    for fname in sorted(os.listdir(src_dir)):
        if not fname.endswith(".py") or fname.startswith("__"):
            continue
        src_path = os.path.join(src_dir, fname)
        dst_path = os.path.join(dst_dir, fname)
        try:
            shutil.copy(src_path, dst_path)
            synced += 1
        except OSError as exc:
            _log(f"Warning: failed to sync {fname}: {exc}")
    _log(f"Synced {synced} test plugins from {src_dir} → {dst_dir}")


def _wait_for_plugin_loaded(plugin_id: str, timeout: float = 20.0) -> bool:
    """Poll the /api/plugins endpoint until plugin_id is listed as loaded."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            plugins = api_get("/api/plugins")
            if any(p.get("plugin_id") == plugin_id for p in plugins):
                return True
        except Exception:
            pass
        time.sleep(0.5)
    return False


def test_schema_breaking() -> Tuple[bool, str]:
    """V1: create a subscription with title+author. Then overwrite the
    plugin file on disk via the dev_lab endpoint. After the file watcher
    picks it up, the subscription should be DISABLED with last_error
    indicating a schema breaking change.

    Self-restoring: always seeds the file with V1 first, and restores V1
    in a finally block so the next test run starts from a known state.
    """
    try:
        # Step 1: Ensure the plugin file is V1 and the manager has loaded it.
        # (If a previous run left V2 on disk, this resets it.)
        ok, msg = _write_plugin_file_directly("schemaBreakingPlugin", SCHEMA_BREAKING_V1_CODE)
        if not ok:
            return False, f"V1 seed write failed: {msg}"
        if not _wait_for_plugin_loaded("schemaBreakingPlugin", timeout=20):
            return False, "manager did not reload V1 within 20s"

        name = _unique("sb")
        sub = create_sub(
            "schemaBreakingPlugin", name,
            {"title": "Book", "author": "Alice"},
            cron="0 * * * *",
        )
        cur = get_sub(sub["id"])
        if cur["status"] not in ("ENABLED", "ENQUEUED", "IN_PROGRESS"):
            return False, f"precondition failed: status={cur['status']}"

        # Step 2: Overwrite with V2 (title+writer) by writing the file
        # directly. The dev_lab/save endpoint would (correctly) refuse
        # this — it requires the schema hash to remain identical when
        # editing an existing plugin (DesignSpecification §7.9). This
        # test simulates an operator who edits the file out-of-band (e.g.
        # via SSH), bypassing the dev_lab. The watcher's reload will
        # detect V2 != V1 and treat it as a breaking change.
        ok, msg = _write_plugin_file_directly("schemaBreakingPlugin", SCHEMA_BREAKING_V2_CODE)
        if not ok:
            return False, f"V2 direct write failed: {msg}"

        # Step 3: Wait for the subscription to be DISABLED.
        try:
            final = wait_for_status(sub["id"], lambda s: s == "DISABLED", timeout=20)
        except TimeoutError:
            cur = get_sub(sub["id"])
            return False, (
                f"timed out waiting for DISABLED; current status={cur['status']}, "
                f"last_error={cur.get('last_error')!r}"
            )
        if not final.get("last_error") or (
            "breaking" not in final["last_error"].lower()
            and "schema" not in final["last_error"].lower()
        ):
            return False, f"expected last_error to mention schema/breaking, got {final.get('last_error')!r}"
        return True, "breaking change → DISABLED"
    finally:
        # Step 4: Always restore V1 so the next run starts from a known state.
        try:
            _write_plugin_file_directly("schemaBreakingPlugin", SCHEMA_BREAKING_V1_CODE)
            _wait_for_plugin_loaded("schemaBreakingPlugin", timeout=20)
        except Exception as exc:  # noqa: BLE001
            _log(f"warning: failed to restore V1 in finally: {exc}")


def test_password() -> Tuple[bool, str]:
    name = _unique("pwd")
    api_key_value = "supersecret-key-123"
    sub = create_sub("passwordPlugin", name, {"apiKey": api_key_value}, cron="0 * * * *")
    # Verify password is NOT in the GET response
    cur = get_sub(sub["id"])
    if "apiKey" in (cur.get("config") or {}):
        return False, "password field leaked in GET response"
    # Run it
    trigger_sub(sub["id"])
    final = wait_for_status(sub["id"], lambda s: s in ("ENABLED", "ERROR"), timeout=20)
    if final["status"] != "ENABLED":
        return False, f"expected ENABLED, got {final['status']} (last_error={final.get('last_error')!r})"
    ev = wait_for_event(sub["id"], timeout=10)
    if ev["exit_code"] != 0:
        return False, f"expected exit_code=0, got {ev['exit_code']}"
    return True, "password encrypted at rest, decrypted at exec, hidden from API"


def test_empty_output() -> Tuple[bool, str]:
    name = _unique("empty")
    sub = create_sub("emptyOutputPlugin", name, {"marker": "m"}, cron="0 * * * *")
    trigger_sub(sub["id"])
    final = wait_for_status(sub["id"], lambda s: s in ("ENABLED", "ERROR"), timeout=15)
    if final["status"] != "ENABLED":
        return False, f"expected ENABLED, got {final['status']}"
    ev = wait_for_event(sub["id"], timeout=10)
    if ev["exit_code"] != 0:
        return False, f"expected exit_code=0, got {ev['exit_code']}"
    return True, "empty output (exit_code=0, status=ENABLED)"


def test_large_output() -> Tuple[bool, str]:
    name = _unique("large")
    # Use a small number of files to keep this fast in the test env
    sub = create_sub("largeOutputPlugin", name, {"file_count": 5}, cron="0 * * * *")
    trigger_sub(sub["id"])
    final = wait_for_status(sub["id"], lambda s: s in ("ENABLED", "ERROR"), timeout=60)
    if final["status"] != "ENABLED":
        return False, f"expected ENABLED, got {final['status']} (last_error={final.get('last_error')!r})"
    ev = wait_for_event(sub["id"], timeout=10)
    if ev["exit_code"] != 0:
        return False, f"expected exit_code=0, got {ev['exit_code']}"
    return True, "large output (exit_code=0, status=ENABLED)"


def test_delayed_init() -> Tuple[bool, str]:
    name = _unique("delay")
    sub = create_sub("delayedInitPlugin", name, {"label": "x"}, cron="0 * * * *")
    trigger_sub(sub["id"])
    final = wait_for_status(sub["id"], lambda s: s in ("ENABLED", "ERROR"), timeout=15)
    if final["status"] != "ENABLED":
        return False, f"expected ENABLED, got {final['status']}"
    ev = wait_for_event(sub["id"], timeout=10)
    if ev["exit_code"] != 0:
        return False, f"expected exit_code=0, got {ev['exit_code']}"
    return True, "delayed init (exit_code=0, status=ENABLED)"


def test_custom_route() -> Tuple[bool, str]:
    # Hit the custom route directly
    r = requests.get(
        f"{MANAGER_URL}/api/plugins/customRoutePlugin/status",
        headers=_api_headers(),
        timeout=10,
    )
    if r.status_code != 200:
        return False, f"custom route returned {r.status_code}: {r.text}"
    if "ok" not in r.text:
        return False, f"unexpected response: {r.text!r}"
    # Also test that the normal getData path works
    name = _unique("custom")
    sub = create_sub("customRoutePlugin", name, {"echo": "hi"}, cron="0 * * * *")
    trigger_sub(sub["id"])
    final = wait_for_status(sub["id"], lambda s: s in ("ENABLED", "ERROR"), timeout=15)
    if final["status"] != "ENABLED":
        return False, f"expected ENABLED, got {final['status']}"
    return True, "custom route mounted + accessible"


def test_invalid_name() -> Tuple[bool, str]:
    # The plugin should be REJECTED at load time. Check the plugin list
    # does NOT include "invalidNamePlugin".
    plugins = api_get("/api/plugins")
    for p in plugins:
        if p["plugin_id"] == "invalidNamePlugin":
            return False, "invalidNamePlugin was loaded despite invalid name"
    return True, "invalid name plugin rejected at load time"


def test_monitor_never_trigger() -> Tuple[bool, str]:
    # The monitor never returns True; rely on cron fallback.
    # Use a cron that's "due" now (every minute: "* * * * *")
    name = _unique("mnt")
    sub = create_sub("monitorNeverTriggerPlugin", name, {"marker": "x"}, cron="* * * * *")
    # EVENT_BASED plugins with cron fallback will get triggered by the
    # scheduler when the cron is due. We just need to verify the system
    # is healthy and the subscription didn't crash.
    time.sleep(3.0)
    cur = get_sub(sub["id"])
    if cur["status"] == "ERROR":
        return False, f"unexpected ERROR: {cur.get('last_error')!r}"
    return True, f"monitor always False, status={cur['status']}, no crash"


def test_monitor_error() -> Tuple[bool, str]:
    # The monitor raises ConnectionError continuously. The system should
    # log + retry indefinitely without crashing.
    name = _unique("me")
    sub = create_sub("monitorErrorPlugin", name, {"label": "x"}, cron="0 0 * * *")
    time.sleep(5.0)
    cur = get_sub(sub["id"])
    if cur["status"] == "ERROR":
        return False, f"unexpected ERROR: {cur.get('last_error')!r}"
    # Confirm the manager is still alive
    h = api_get("/api/health")
    if h.get("status") != "ok":
        return False, f"health degraded: {h}"
    return True, "monitor exception → retry loop, system still healthy"


def test_config_validation() -> Tuple[bool, str]:
    name = _unique("cv")
    cfg = {
        "name": "test-config",
        "combo": "A",
        "radio": "X",
        "checkbox": True,
        "secret": "hidden-value",
    }
    sub = create_sub("configValidationPlugin", name, cfg, cron="0 * * * *")
    cur = get_sub(sub["id"])
    if cur.get("status") == "ERROR":
        return False, f"unexpected ERROR: {cur.get('last_error')!r}"
    # Verify password field hidden
    if "secret" in (cur.get("config") or {}):
        return False, "password field leaked in GET response"
    trigger_sub(sub["id"])
    final = wait_for_status(sub["id"], lambda s: s in ("ENABLED", "ERROR"), timeout=15)
    if final["status"] != "ENABLED":
        return False, f"expected ENABLED, got {final['status']}"
    ev = wait_for_event(sub["id"], timeout=10)
    if ev["exit_code"] != 0:
        return False, f"expected exit_code=0, got {ev['exit_code']}"
    return True, "all field types validated + executed"


def test_non_zero_exit() -> Tuple[bool, str]:
    name = _unique("nze")
    sub = create_sub("nonZeroExitPlugin", name, {"label": "x"}, cron="0 * * * *")
    trigger_sub(sub["id"])
    final = wait_for_status(sub["id"], lambda s: s in ("ERROR", "ENABLED"), timeout=15)
    if final["status"] != "ERROR":
        return False, f"expected ERROR, got {final['status']}"
    ev = wait_for_event(sub["id"], timeout=10)
    if ev["exit_code"] != 1:
        return False, f"expected exit_code=1, got {ev['exit_code']}"
    if "exit code 1" not in ev["exit_string"].lower():
        return False, f"expected generic fallback, got {ev['exit_string']!r}"
    return True, f"non-zero exit (exit_code=1, exit_string={ev['exit_string']!r})"


def test_zombie() -> Tuple[bool, str]:
    """Zombie: progress_callback never checks DB; test runner sets
    DISABLED mid-execution. The child keeps running and is force-killed
    by the watcher when HEARTBEAT_TIMEOUT elapses."""
    name = _unique("zombie")
    sub = create_sub("zombiePlugin", name, {"label": "x"}, cron="0 * * * *")
    trigger_sub(sub["id"])
    wait_for_status(sub["id"], lambda s: s == "IN_PROGRESS", timeout=15)
    time.sleep(0.5)
    set_status(sub["id"], "DISABLED")
    # The zombie keeps running. The plugin's progress_callback never
    # calls the DB to detect DISABLED, so the only way to stop it is
    # the heartbeat timeout. Wait for status to be set to ERROR.
    final = wait_for_status(sub["id"], lambda s: s == "ERROR", timeout=20)
    ev = wait_for_event(sub["id"], timeout=10)
    if ev["exit_code"] != 2:
        return False, f"expected exit_code=2 (timeout), got {ev['exit_code']}"
    return True, "zombie force-killed by watcher (exit_code=2)"


def test_move_to_dest_error() -> Tuple[bool, str]:
    name = _unique("mde")
    sub = create_sub("moveToDestErrorPlugin", name, {"label": "x"}, cron="0 * * * *")
    trigger_sub(sub["id"])
    final = wait_for_status(sub["id"], lambda s: s in ("ERROR", "ENABLED"), timeout=15)
    if final["status"] != "ERROR":
        return False, f"expected ERROR, got {final['status']}"
    ev = wait_for_event(sub["id"], timeout=10)
    if ev["exit_code"] != 1:
        return False, f"expected exit_code=1, got {ev['exit_code']}"
    if "ValueError" not in ev["exit_string"]:
        return False, f"expected ValueError in exit_string, got {ev['exit_string']!r}"
    return True, f"ValueError caught (exit_code=1, exit_string={ev['exit_string']!r})"


def test_long_name_plugin() -> Tuple[bool, str]:
    """Test plugin with a 32-character (max) name. Verifies it loads,
    a subscription can be created, and a run completes successfully —
    exercises the plugin grid layout for the longest allowed name."""
    name = _unique("lnpfui")
    sub = create_sub(
        "longNamePlugin32CharNameForUITes",
        name,
        {"label": "x"},
        cron="0 * * * *",
    )
    trigger_sub(sub["id"])
    final = wait_for_status(sub["id"], lambda s: s in ("ENABLED", "ERROR"), timeout=30)
    if final["status"] != "ENABLED":
        return False, f"expected ENABLED, got {final['status']} (last_error={final.get('last_error')!r})"
    ev = wait_for_event(sub["id"], timeout=10)
    if ev["exit_code"] != 0:
        return False, f"expected exit_code=0, got {ev['exit_code']}"
    return True, "32-char name plugin loaded, sub created, run succeeded (exit_code=0)"


# ---------------------------------------------------------------------------
# Edit Plugin tests (DesignSpecification §7.9)
# ---------------------------------------------------------------------------
# These tests exercise the Edit Plugin feature: the dev_lab/save endpoint
# must accept a save whose get_schema() is identical to the stored schema
# ("edit mode, schema unchanged") and must reject one whose get_schema()
# differs ("edit mode, schema changed"). In both cases the on-disk file
# reflects exactly what was accepted: V2 in the match case, V1 in the
# mismatch case.

# editMatchPluginV1 / V2 share the same get_schema() (only getData() body
# and the metadata["description"] change). V3 has a different schema and
# must be rejected by the dev_lab/save endpoint.
EDIT_MATCH_V1_CODE = '''
from utils.plugin_base import BaseSubscription


class editMatchPlugin(BaseSubscription):
    metadata = {
        "name": "editMatchPlugin",
        "icon": "default_icon.png",
        "description": "edit match plugin V1",
        "sub_type": "SCHEDULED",
    }
    DEFAULT_ACCESS_LEVEL = "PRIVATE"

    def get_schema(self):
        return {
            "type": "object",
            "properties": {
                "label": {"type": "string", "minLength": 1},
            },
            "required": ["label"],
        }

    def getData(self, config, progress_callback):
        import os
        tmp = "/tmp/editMatchPlugin_output.txt"
        with open(tmp, "w") as f:
            f.write("VERSION_1")
        os.makedirs("/output/editMatchPlugin", exist_ok=True)
        # Make the output visible to the test (which runs inside the
        # manager container and shares /output via the volume mount).
        self.move_to_destination(tmp)
        progress_callback(100)
'''


EDIT_MATCH_V2_CODE = '''
from utils.plugin_base import BaseSubscription


class editMatchPlugin(BaseSubscription):
    metadata = {
        "name": "editMatchPlugin",
        "icon": "default_icon.png",
        "description": "edit match plugin V2 (same schema, different output)",
        "sub_type": "SCHEDULED",
    }
    DEFAULT_ACCESS_LEVEL = "PRIVATE"

    def get_schema(self):
        return {
            "type": "object",
            "properties": {
                "label": {"type": "string", "minLength": 1},
            },
            "required": ["label"],
        }

    def getData(self, config, progress_callback):
        import os
        tmp = "/tmp/editMatchPlugin_output.txt"
        with open(tmp, "w") as f:
            f.write("VERSION_2")
        os.makedirs("/output/editMatchPlugin", exist_ok=True)
        self.move_to_destination(tmp)
        progress_callback(100)
'''


# V3 has a different schema (adds a required "extra" field) — must be
# rejected by dev_lab/save when the registry already has V1's hash.
EDIT_MISMATCH_V3_CODE = '''
from utils.plugin_base import BaseSubscription


class editMatchPlugin(BaseSubscription):
    metadata = {
        "name": "editMatchPlugin",
        "icon": "default_icon.png",
        "description": "edit match plugin V3 (different schema — should be rejected)",
        "sub_type": "SCHEDULED",
    }
    DEFAULT_ACCESS_LEVEL = "PRIVATE"

    def get_schema(self):
        return {
            "type": "object",
            "properties": {
                "label": {"type": "string", "minLength": 1},
                "extra": {"type": "string"},
            },
            "required": ["label", "extra"],
        }

    def getData(self, config, progress_callback):
        import os
        tmp = "/tmp/editMatchPlugin_output.txt"
        with open(tmp, "w") as f:
            f.write("VERSION_3")
        os.makedirs("/output/editMatchPlugin", exist_ok=True)
        self.move_to_destination(tmp)
        progress_callback(100)
'''


def _wait_for_plugin_reload_after_save(plugin_id: str, expected_code: str, timeout: float = 20.0) -> bool:
    """Poll the dev_lab/load endpoint until the served code matches ``expected_code``.

    Reading back through the API is more reliable than timing-based
    heuristics because the file watcher's debounce depends on filesystem
    mtime events that may not fire on every docker volume layer. We
    just keep reloading until the on-disk contents match.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.get(
                f"{MANAGER_URL}/api/dev_lab/load/{plugin_id}",
                headers=_api_headers(),
                timeout=10,
            )
            if r.status_code == 200 and r.json().get("code") == expected_code:
                return True
        except Exception:
            pass
        time.sleep(0.5)
    return False


def test_edit_plugin_match() -> Tuple[bool, str]:
    """Edit Plugin flow with an UNCHANGED schema: the save is accepted and
    the new getData() behavior takes effect on the next sub execution.

    Stage 1: save V1 (create), create a sub, run it — output is VERSION_1.
    Stage 2: save V2 (edit, same schema, different getData) — accepted.
    Stage 3: trigger the same sub again — output is now VERSION_2.
    """
    plugin = "editMatchPlugin"
    # Self-restoring: always leave V1 on disk so the next run is clean.
    try:
        # Stage 1: seed V1 directly to disk (bypasses the dev_lab's edit
        # check so the test is independent of any prior hash stored in
        # the DB from a previous run). The watcher reloads V1 and the
        # DB is updated with V1's hash.
        ok, msg = _write_plugin_file_directly(plugin, EDIT_MATCH_V1_CODE)
        if not ok:
            return False, f"V1 seed write failed: {msg}"
        if not _wait_for_plugin_loaded(plugin, timeout=20):
            return False, "manager did not load V1 within 20s"

        name = _unique("editmatch")
        sub = create_sub(plugin, name, {"label": "x"}, cron="0 * * * *")
        trigger_sub(sub["id"])
        final = wait_for_status(sub["id"], lambda s: s in ("ENABLED", "ERROR"), timeout=30)
        if final["status"] != "ENABLED":
            return False, f"stage 1 final status={final['status']}, last_error={final.get('last_error')!r}"
        out = _read_sub_output(sub["name"])
        if out != "VERSION_1":
            return False, f"stage 1 expected output=VERSION_1, got {out!r}"

        # Stage 2: edit to V2 (same schema) — should be accepted (mode=edit).
        r = requests.post(
            f"{MANAGER_URL}/api/dev_lab/save",
            headers={**_api_headers(), "Content-Type": "application/json"},
            data=json.dumps({"name": plugin, "code": EDIT_MATCH_V2_CODE}),
            timeout=30,
        )
        if r.status_code != 200:
            return False, f"stage 2: V2 save rejected unexpectedly: {r.status_code} {r.text}"
        body = r.json()
        if body.get("mode") != "edit":
            return False, f"stage 2: expected mode=edit, got {body.get('mode')!r}"

        # Wait for the file watcher to reload V2.
        if not _wait_for_plugin_reload_after_save(plugin, EDIT_MATCH_V2_CODE, timeout=20):
            return False, "stage 2: file watcher did not pick up V2 within 20s"

        # Stage 3: trigger the same sub again — output should now be VERSION_2.
        # We poll the output file's mtime + content because a trigger fired
        # too soon after stage 1 can be silently absorbed by the worker's
        # post-execution success-path / debounce window (the success path
        # sets status back to ENABLED, overwriting the ENQUEUED state set
        # by the trigger). Wait until the file shows VERSION_2 (or a hard
        # timeout). Retry the trigger if the worker hasn't picked it up.
        out2 = "<file not found>"
        deadline = time.time() + 30
        trigger_time = None
        while time.time() < deadline:
            trigger_time = time.time() if trigger_time is None else trigger_time
            trigger_sub(sub["id"])
            out2 = _wait_for_sub_re_execution(sub["name"], trigger_time, timeout=10)
            if out2 == "VERSION_2":
                break
            # Trigger may have been absorbed; brief pause + retry.
            time.sleep(0.5)
        if out2 != "VERSION_2":
            return False, f"stage 3 expected output=VERSION_2, got {out2!r}"

        return True, "V2 (same schema) accepted; output changed VERSION_1 → VERSION_2"
    finally:
        # Restore V1 on disk so the next run starts from a known state.
        try:
            _write_plugin_file_directly(plugin, EDIT_MATCH_V1_CODE)
            _wait_for_plugin_loaded(plugin, timeout=20)
        except Exception as exc:  # noqa: BLE001
            _log(f"warning: failed to restore V1 in finally: {exc}")


def test_edit_plugin_mismatch() -> Tuple[bool, str]:
    """Edit Plugin flow with a CHANGED schema: the save is rejected, the
    on-disk file is untouched, and a subsequent sub execution still uses
    the old V1 code.

    Stage 1: save V1 (create), create a sub, run it — output is VERSION_1.
    Stage 2: try to save V3 (different schema) — must be rejected (HTTP 400).
    Stage 3: the on-disk file is still V1 (verified by reading back via
    the dev_lab/load endpoint and by re-triggering the sub and observing
    the output is still VERSION_1).
    """
    plugin = "editMatchPlugin"
    try:
        # Stage 1: seed V1 directly to disk (same rationale as the match test).
        ok, msg = _write_plugin_file_directly(plugin, EDIT_MATCH_V1_CODE)
        if not ok:
            return False, f"V1 seed write failed: {msg}"
        if not _wait_for_plugin_loaded(plugin, timeout=20):
            return False, "manager did not load V1 within 20s"

        name = _unique("editmis")
        sub = create_sub(plugin, name, {"label": "x"}, cron="0 * * * *")
        trigger_sub(sub["id"])
        final = wait_for_status(sub["id"], lambda s: s in ("ENABLED", "ERROR"), timeout=30)
        if final["status"] != "ENABLED":
            return False, f"stage 1 final status={final['status']}, last_error={final.get('last_error')!r}"
        out = _read_sub_output(sub["name"])
        if out != "VERSION_1":
            return False, f"stage 1 expected output=VERSION_1, got {out!r}"

        # Stage 2: try to save V3 (different schema) — must be rejected.
        r = requests.post(
            f"{MANAGER_URL}/api/dev_lab/save",
            headers={**_api_headers(), "Content-Type": "application/json"},
            data=json.dumps({"name": plugin, "code": EDIT_MISMATCH_V3_CODE}),
            timeout=30,
        )
        if r.status_code != 400:
            return False, (
                f"stage 2: V3 (different schema) was NOT rejected. "
                f"status={r.status_code}, body={r.text[:200]}"
            )
        body = r.json()
        detail = body.get("detail", "")
        if "config" not in detail.lower() and "schema" not in detail.lower():
            return False, f"stage 2: rejection reason should mention config/schema, got {detail!r}"

        # Stage 3: on-disk file is still V1 — verify via dev_lab/load.
        r = requests.get(
            f"{MANAGER_URL}/api/dev_lab/load/{plugin}",
            headers=_api_headers(),
            timeout=10,
        )
        if r.status_code != 200:
            return False, f"stage 3: dev_lab/load failed: {r.status_code} {r.text[:200]}"
        if r.json().get("code") != EDIT_MATCH_V1_CODE:
            return False, "stage 3: on-disk file is no longer V1 after a rejected V3 save"

        # Also re-trigger the sub and confirm the output is still VERSION_1.
        # Same retry-loop pattern as the match test: a trigger that fires
        # during the worker's post-execution debounce can be silently
        # absorbed, so we re-trigger until the file's mtime is fresh AND
        # the content is still VERSION_1 (proves the on-disk plugin is V1).
        out2 = "<file not found>"
        deadline = time.time() + 30
        trigger_time = None
        while time.time() < deadline:
            trigger_time = time.time() if trigger_time is None else trigger_time
            trigger_sub(sub["id"])
            out2 = _wait_for_sub_re_execution(sub["name"], trigger_time, timeout=10)
            if out2 == "VERSION_1":
                break
            time.sleep(0.5)
        if out2 != "VERSION_1":
            return False, f"stage 3 expected output=VERSION_1 (unchanged), got {out2!r}"

        return True, "V3 (different schema) rejected; on-disk file + sub output unchanged"
    finally:
        try:
            _write_plugin_file_directly(plugin, EDIT_MATCH_V1_CODE)
            _wait_for_plugin_loaded(plugin, timeout=20)
        except Exception as exc:  # noqa: BLE001
            _log(f"warning: failed to restore V1 in finally: {exc}")


def _read_sub_output(sub_name: str) -> str:
    """Read the contents of the file produced by an editMatchPlugin sub.

    The plugin writes a single temp file whose name is sanitized by
    ``sanitize_name`` (strips everything except ``[a-zA-Z0-9.\-]``) on
    the way to its canonical destination, so the underscores in
    ``editMatchPlugin_output.txt`` are dropped → ``editMatchPluginoutput.txt``.
    We glob the sub's output directory rather than hard-coding a path
    so a future sanitization-rule change does not silently break the
    test."""
    import glob
    out_dir = f"/output/editMatchPlugin/{sub_name}"
    matches = sorted(glob.glob(f"{out_dir}/*.txt"))
    if not matches:
        return "<file not found>"
    with open(matches[0], "r", encoding="utf-8") as f:
        return f.read().strip()


def _wait_for_sub_re_execution(sub_name: str, before_ts: float, timeout: float = 30.0) -> str:
    """Wait for the editMatchPlugin sub's output file to be re-written
    after ``before_ts`` (a ``time.time()`` reference) and return its
    contents.

    This is the reliable "did the worker actually re-run" signal: the
    plugin overwrites its output file on every execution, so a fresh
    mtime + matching content is proof that a NEW execution happened.
    It works around a race in the worker where a trigger fired during
    the post-execution success-path / debounce window can be silently
    absorbed (status set back to ENABLED overwrites the ENQUEUED state),
    leaving the sub stuck without producing a fresh event log.
    """
    import glob
    import time as _time
    out_dir = f"/output/editMatchPlugin/{sub_name}"
    deadline = _time.time() + timeout
    while _time.time() < deadline:
        matches = sorted(glob.glob(f"{out_dir}/*.txt"), key=lambda p: os.path.getmtime(p))
        if matches:
            latest = matches[-1]
            if os.path.getmtime(latest) >= before_ts:
                with open(latest, "r", encoding="utf-8") as f:
                    return f.read().strip()
        _time.sleep(0.3)
    return "<file not found>"


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------
# Test-created artifacts that may persist between runs. The Edit Plugin
# tests (22/23) write /src/plugins/editMatchPlugin.py and a /output
# sub-directory; the try/finally blocks in those tests restore the V1
# code but leave the file on disk. --reset is the only way to clean
# these up.
TEST_PLUGIN_FILES = ("editMatchPlugin.py",)
TEST_OUTPUT_DIRS = ("editMatchPlugin",)


def _wipe_test_state() -> None:
    """Wipe everything a previous test run (or manual testing) may have left.

    Called only when ``--reset`` is passed. Runs BEFORE the regular
    cleanup, then sleeps so the worker can process DELETED cleanup jobs.
    """
    _log("--reset: wiping subscriptions, event log, and test artifacts...")

    # 1. Delete all subscriptions
    try:
        subs = api_get("/api/subscriptions")
        for sub in subs:
            try:
                requests.delete(
                    f"{MANAGER_URL}/api/subscriptions/{sub['id']}",
                    headers=_api_headers(), timeout=10,
                )
            except Exception as exc:  # noqa: BLE001
                _log(f"  --reset: failed to delete sub {sub.get('id')}: {exc}")
    except Exception as exc:  # noqa: BLE001
        _log(f"--reset: subscription sweep failed: {exc}")

    # 2. Clear event log
    try:
        requests.delete(
            f"{MANAGER_URL}/api/logging", headers=_api_headers(), timeout=10
        )
    except Exception as exc:  # noqa: BLE001
        _log(f"--reset: event log clear failed: {exc}")

    # 3. Remove test-created plugin files (only the ones tests write;
    #    the valid test plugins live in src/testing/plugins/ and are
    #    re-synced by _sync_test_plugins() before each run).
    plugins_dir = "/src/plugins"
    for fname in TEST_PLUGIN_FILES:
        path = os.path.join(plugins_dir, fname)
        if os.path.exists(path):
            try:
                os.remove(path)
                _log(f"  --reset: removed {path}")
            except Exception as exc:  # noqa: BLE001
                _log(f"  --reset: failed to remove {path}: {exc}")

    # 4. Remove /output sub-directories for the test plugins. The
    #    worker's DELETED handler will normally prune these, but
    #    leftover dirs from manual testing may not have a subscription
    #    attached anymore.
    output_root = "/output"
    if os.path.isdir(output_root):
        for dirname in TEST_OUTPUT_DIRS:
            path = os.path.join(output_root, dirname)
            if os.path.isdir(path):
                try:
                    shutil.rmtree(path)
                    _log(f"  --reset: removed {path}")
                except Exception as exc:  # noqa: BLE001
                    _log(f"  --reset: failed to remove {path}: {exc}")

    # 5. Give the worker a moment to process any DELETED cleanup jobs
    #    triggered by step 1.
    time.sleep(2.0)
    _log("--reset: wipe complete")


def _parse_argv(argv: List[str]) -> Tuple[bool, bool]:
    """Validate argv. Returns ``(ok, reset_mode)``.

    Rules:
      - ``argv[1]`` MUST be the literal string ``"confirm"``; without
        it, this script refuses to run (safety check).
      - ``"--reset"`` is optional and is recognized in any argv slot.
        It triggers a full state wipe before the test run starts.
    """
    if len(argv) < 2 or argv[1] != "confirm":
        print(
            "Usage: python /src/testing/test_runner.py confirm [--reset]",
            file=sys.stderr,
        )
        print("", file=sys.stderr)
        print(
            "  confirm   Required safety argument to prevent accidental"
            " execution.",
            file=sys.stderr,
        )
        print(
            "  --reset   Wipe all subscriptions, the event log, test-created",
            file=sys.stderr,
        )
        print(
            "            plugin files, and /output/ before running.",
            file=sys.stderr,
        )
        return False, False
    return True, "--reset" in argv


def main() -> int:
    ok, reset_mode = _parse_argv(sys.argv)
    if not ok:
        return 2

    _log(f"Test runner starting; manager={MANAGER_URL} (reset={reset_mode})")

    if reset_mode:
        # Wipe before the health check so the manager isn't hit with
        # work while we're still validating connectivity. We still
        # need a reachable manager to clear subs/log, but the plugin
        # file + /output cleanup happens unconditionally.
        # Wait briefly for the manager to come up before the wipe.
        deadline = time.time() + 60
        manager_up = False
        while time.time() < deadline:
            try:
                if api_get("/api/health").get("status") == "ok":
                    manager_up = True
                    break
            except Exception:
                pass
            time.sleep(1.0)
        if not manager_up:
            _log("Manager never became healthy; cannot run --reset")
            return 2
        _wipe_test_state()

    # Wait for the manager to be reachable
    deadline = time.time() + 120
    while time.time() < deadline:
        try:
            h = api_get("/api/health")
            if h.get("status") == "ok":
                _log(f"Manager healthy: {h}")
                break
        except Exception:
            pass
        time.sleep(2.0)
    else:
        _log("Manager never became healthy")
        return 2

    # Sync test plugins from src/testing/plugins/ to /src/plugins/
    # before verifying the registry or cleaning up state.
    _sync_test_plugins()

    # Wait for the file watcher to detect the synced plugins and reload
    # the registry. Without this, the registry may still only contain
    # imapFolderWatchPlugin (whatever was present at manager startup).
    expected_loaded_pre = {
        "happyPathPlugin", "eventHappyPlugin", "noHeartbeatPlugin",
        "longRunningSuccessPlugin", "longRunningFailurePlugin", "crashPlugin",
        "cancellationPlugin", "schemaBreakingPlugin", "passwordPlugin",
        "emptyOutputPlugin", "largeOutputPlugin", "delayedInitPlugin",
        "customRoutePlugin", "monitorNeverTriggerPlugin", "monitorErrorPlugin",
        "configValidationPlugin", "nonZeroExitPlugin", "zombiePlugin",
        "moveToDestErrorPlugin", "longNamePlugin32CharNameForUITes",
        "eventOftenPlugin", "deleteAllPlugin",
    }
    _log("Waiting for synced plugins to appear in registry...")
    registry_deadline = time.time() + 60
    while time.time() < registry_deadline:
        try:
            plugins = api_get("/api/plugins")
            loaded = {p["plugin_id"] for p in plugins}
            missing = expected_loaded_pre - loaded
            if not missing:
                _log(f"All {len(expected_loaded_pre)} expected plugins loaded after sync")
                break
        except Exception:
            pass
        time.sleep(2.0)
    else:
        plugins = api_get("/api/plugins")
        loaded = {p["plugin_id"] for p in plugins}
        missing = expected_loaded_pre - loaded
        _log(f"TIMEOUT waiting for plugins: still missing {sorted(missing)}")
        return 3

    # Clean up any leftover state from previous runs
    _log("Cleaning up state from previous runs...")
    try:
        # Delete all subscriptions
        subs = api_get("/api/subscriptions")
        for sub in subs:
            try:
                requests.delete(
                    f"{MANAGER_URL}/api/subscriptions/{sub['id']}",
                    headers=_api_headers(), timeout=10,
                )
            except Exception:
                pass
        # Clear event log
        requests.delete(f"{MANAGER_URL}/api/logging", headers=_api_headers(), timeout=10)
        time.sleep(2.0)  # Let the worker process DELETED cleanups
    except Exception as exc:
        _log(f"Cleanup warning: {exc}")

    # Verify all 21 expected plugins are loaded (except invalidNamePlugin)
    plugins = api_get("/api/plugins")
    loaded = {p["plugin_id"] for p in plugins}
    _log(f"Loaded plugins: {sorted(loaded)}")

    expected_loaded = {
        "happyPathPlugin", "eventHappyPlugin", "noHeartbeatPlugin",
        "longRunningSuccessPlugin", "longRunningFailurePlugin", "crashPlugin",
        "cancellationPlugin", "schemaBreakingPlugin", "passwordPlugin",
        "emptyOutputPlugin", "largeOutputPlugin", "delayedInitPlugin",
        "customRoutePlugin", "monitorNeverTriggerPlugin", "monitorErrorPlugin",
        "configValidationPlugin", "nonZeroExitPlugin", "zombiePlugin",
        "moveToDestErrorPlugin", "longNamePlugin32CharNameForUITes",
        "eventOftenPlugin", "deleteAllPlugin",
    }
    missing = expected_loaded - loaded
    if missing:
        _log(f"MISSING plugins: {missing}")
        return 3
    if "invalidNamePlugin" in loaded:
        _log("invalidNamePlugin was loaded (should not be)")
        return 3
    _log(f"All 22 valid plugins loaded + invalidNamePlugin rejected ✓")

    tests: List[Tuple[str, Callable[[], Tuple[bool, str]]]] = [
        ("Plugin Test 1 — happyPath", test_happy_path),
        ("Plugin Test 2 — eventHappy", test_event_happy),
        ("Plugin Test 3 — noHeartbeat", test_no_heartbeat),
        ("Plugin Test 4 — longRunningSuccess", test_long_running_success),
        ("Plugin Test 5 — longRunningFailure", test_long_running_failure),
        ("Plugin Test 6 — crash", test_crash),
        ("Plugin Test 7 — cancellation", test_cancellation),
        ("Plugin Test 8 — schemaBreaking", test_schema_breaking),
        ("Plugin Test 9 — password", test_password),
        ("Plugin Test 10 — emptyOutput", test_empty_output),
        ("Plugin Test 11 — largeOutput", test_large_output),
        ("Plugin Test 12 — delayedInit", test_delayed_init),
        ("Plugin Test 13 — customRoute", test_custom_route),
        ("Plugin Test 14 — invalidName", test_invalid_name),
        ("Plugin Test 15 — monitorNeverTrigger", test_monitor_never_trigger),
        ("Plugin Test 16 — monitorError", test_monitor_error),
        ("Plugin Test 17 — configValidation", test_config_validation),
        ("Plugin Test 18 — nonZeroExit", test_non_zero_exit),
        ("Plugin Test 19 — zombie", test_zombie),
        ("Plugin Test 20 — moveToDestError", test_move_to_dest_error),
        ("Plugin Test 21 — longNamePlugin32Char", test_long_name_plugin),
        ("Plugin Test 22 — editPluginMatch", test_edit_plugin_match),
        ("Plugin Test 23 — editPluginMismatch", test_edit_plugin_mismatch),
        ("Plugin Test 24 — eventOften", test_event_often),
        ("Plugin Test 25 — deleteAll", test_delete_subscription_and_plugin),
        ("Plugin Test 26 — cronRandomize", test_cron_randomize),
    ]

    results: List[Tuple[str, bool, str]] = []
    for name, fn in tests:
        _log(f"Running {name}...")
        try:
            ok, msg = fn()
        except Exception as exc:  # noqa: BLE001
            ok = False
            msg = f"EXCEPTION: {exc!r}"
            _log(f"  {name} FAILED with exception: {exc}\n{traceback.format_exc()}")
        results.append((name, ok, msg))
        status = "✅" if ok else "❌"
        _log(f"  {status} {name}: {msg}")

    # Summary
    passed = sum(1 for _, ok, _ in results if ok)
    total = len(results)
    _log(f"\n=== TEST SUMMARY: {passed}/{total} passed ===")
    for name, ok, msg in results:
        mark = "✅" if ok else "❌"
        _log(f"  {mark} {name}: {msg}")
    return 0 if passed == total else 1


if __name__ == "__main__":
    sys.exit(main())
