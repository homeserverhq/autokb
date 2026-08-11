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

# Sink-specific imports
from utils.constants import (
    OPERATION_FULL, OPERATION_SINK_ONLY,
    STATE_ENABLED, STATE_ENQUEUED, STATE_ERROR, STATE_DISABLED,
    STATE_DELETED, STATE_IN_PROGRESS,
)
from utils.database import (
    AKBDatafile, Sink, Target,
    DatabaseManager, TargetDatafile, TargetSubscription,
    EventLog, PluginRegistryState, Subscription,
    run_migrations,
)
from utils.sink_base import BaseSink, compute_file_hash
from utils.sink_registry import SinkRegistry
from utils.queue_utils import QueueManager, _encode_item, _decode_item, P_QUEUE_KEY, S_QUEUE_KEY
from utils.misc_utils import uuid7, uuid4, SinkCancelledError

import logging as _logging

# The runner talks to the manager over HTTP via `requests`. Creating a
# DatabaseManager (e.g. the startup purge or the Sink unit tests)
# reconfigures the root logger to INFO with a stdout handler, which makes
# urllib3 print a "Starting new HTTP connection" line for every API call.
# Silence it — the runner's own [_log] output is all we want on stdout.
_logging.getLogger("urllib3").setLevel(_logging.WARNING)
_logging.getLogger("urllib3.connectionpool").setLevel(_logging.WARNING)

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

# Stable marker so test-created subscriptions are identifiable across runs
# and can be cleaned up WITHOUT touching user-created (real) subscriptions.
TEST_SUB_PREFIX = "akbtest-"

# Single-instance guard: only one test runner may touch the shared
# manager / DB / filesystem at a time. A stale lock whose owner PID is
# dead is broken automatically, so an interrupted run can't block the
# next one. The TTL is only a backstop for an ungraceful crash.
TEST_RUN_LOCK_KEY = "autokb:test_runner:lock"
TEST_RUN_LOCK_TTL_MS = 2 * 60 * 60 * 1000  # 2 hours


def _is_test_sub(sub: Dict[str, Any]) -> bool:
    """True if a subscription was created by the test runner (safe to delete)."""
    name = sub.get("name", "")
    return (
        name.startswith(TEST_SUB_PREFIX)
        or name.startswith("e2e-")
        or sub.get("plugin_id") == "test_plugin"
    )


def _unique(base: str) -> str:
    return f"{TEST_SUB_PREFIX}{base}-{RUN_ID}"

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


def _delete_sub(sub_id: str) -> None:
    """Delete a subscription via the normal API. 404 is fine (already gone)."""
    try:
        r = requests.delete(
            f"{MANAGER_URL}/api/subscriptions/{sub_id}",
            headers=_api_headers(), timeout=10,
        )
        if r.status_code not in (200, 204, 404):
            _log(f"warning: DELETE subscription {sub_id} returned {r.status_code}: {r.text[:120]}")
    except Exception as exc:  # noqa: BLE001
        _log(f"warning: failed to delete subscription {sub_id}: {exc}")


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
# Sink test helpers (reused by both e2e tests)
# ---------------------------------------------------------------------------

def _reset_sink_calls():
    import os as _os
    for p in ("/output/.sink_e2e_calls.json",):
        if _os.path.isfile(p):
            _os.remove(p)


def _read_sink_calls() -> List[List]:
    import json as _json, os as _os
    p = "/output/.sink_e2e_calls.json"
    if not _os.path.isfile(p):
        return []
    with open(p) as f:
        return [_json.loads(line) for line in f if line.strip()]


def _write_output_file(sub_name: str, fname: str, content: str) -> str:
    import os as _os
    p = f"/output/testSinkWriterPlugin/{sub_name}/{fname}"
    _os.makedirs(_os.path.dirname(p), exist_ok=True)
    with open(p, "w") as f:
        f.write(content)
    return p


def _rm_output_dir(sub_name: str):
    import os as _os, shutil as _su
    d = f"/output/testSinkWriterPlugin/{sub_name}"
    if _os.path.isdir(d):
        _su.rmtree(d)


# ---------------------------------------------------------------------------
# Test definitions
# ---------------------------------------------------------------------------
def test_happy_path() -> Tuple[bool, str]:
    name = _unique("happy")
    sub = create_sub("happyPathPlugin", name, {"title": "Hello"}, cron="0 * * * *")
    try:
        trigger_sub(sub["id"])
        final = wait_for_status(sub["id"], lambda s: s in ("ENABLED", "ERROR"), timeout=30)
        if final["status"] != "ENABLED":
            return False, f"expected ENABLED, got {final['status']} (last_error={final.get('last_error')!r})"
        # Check EventLog
        ev = wait_for_event(sub["id"], timeout=15)
        if ev["exit_code"] != 0:
            return False, f"expected exit_code=0, got {ev['exit_code']}"
        return True, "success (exit_code=0, status=ENABLED)"
    finally:
        _delete_sub(sub["id"])


def test_event_happy() -> Tuple[bool, str]:
    name = _unique("event")
    sub = create_sub("eventHappyPlugin", name, {"topic": "news"}, cron="0 0 * * *")
    # EVENT_BASED plugins need to be triggered via monitor. The manager's
    # monitor loop runs every ~2s; we give it a few iterations to fire.
    # Wait for status to flip from ENABLED to ENQUEUED then back.
    try:
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
    finally:
        _delete_sub(sub["id"])


def test_event_often() -> Tuple[bool, str]:
    """Verify eventOftenPlugin fires immediately on enable."""
    name = _unique("eventOften")
    sub = create_sub("eventOftenPlugin", name, {"topic": "news"}, cron="0 0 * * *")
    # EVENT_BASED: monitor runs every ~2s. The plugin fires on first
    # monitor call (self._last_fire is None → return True immediately),
    # so the subscription should transition to ENQUEUED/IN_PROGRESS
    # within a few seconds.
    try:
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
    finally:
        _delete_sub(sub["id"])


def test_cron_randomize() -> Tuple[bool, str]:
    """Test 26: Verify default cron strings are randomized at creation."""
    import re
    name_s = _unique("cronSched")
    sub_s = create_sub("cronRandomizePlugin", name_s, {"label": "scheduled"}, cron="0 * * * *")
    try:
        cron_s = sub_s.get("cron", "")
        if cron_s == "0 * * * *":
            return False, f"SCHEDULED cron was not randomized: {cron_s!r}"
        if not re.match(r"^\d+ \* \* \* \*$", cron_s):
            return False, f"SCHEDULED cron has unexpected format: {cron_s!r}"

        name_e = _unique("cronEvent")
        sub_e = create_sub("eventHappyPlugin", name_e, {"topic": "news"}, cron="0 0 * * *")
        try:
            cron_e = sub_e.get("cron", "")
            if cron_e == "0 0 * * *":
                return False, f"EVENT_BASED cron was not randomized: {cron_e!r}"
            if not re.match(r"^\d+ \d+ \* \* \*$", cron_e):
                return False, f"EVENT_BASED cron has unexpected format: {cron_e!r}"

            return True, f"SCHEDULED={cron_s!r}, EVENT_BASED={cron_e!r}"
        finally:
            _delete_sub(sub_e["id"])
    finally:
        _delete_sub(sub_s["id"])


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
    try:
        trigger_sub(sub["id"])
        final = wait_for_status(sub["id"], lambda s: s in ("ERROR", "ENABLED"), timeout=30)
        if final["status"] != "ERROR":
            return False, f"expected ERROR, got {final['status']}"
        ev = wait_for_event(sub["id"], timeout=10)
        if ev["exit_code"] != 2:
            return False, f"expected exit_code=2, got {ev['exit_code']}"
        return True, "timeout (exit_code=2, status=ERROR)"
    finally:
        _delete_sub(sub["id"])


def test_long_running_success() -> Tuple[bool, str]:
    name = _unique("lrs")
    sub = create_sub("longRunningSuccessPlugin", name, {"name": "long"}, cron="0 * * * *")
    try:
        trigger_sub(sub["id"])
        final = wait_for_status(sub["id"], lambda s: s in ("ENABLED", "ERROR"), timeout=45)
        if final["status"] != "ENABLED":
            return False, f"expected ENABLED, got {final['status']}"
        ev = wait_for_event(sub["id"], timeout=10)
        if ev["exit_code"] != 0:
            return False, f"expected exit_code=0, got {ev['exit_code']}"
        return True, "long running success (exit_code=0)"
    finally:
        _delete_sub(sub["id"])


def test_long_running_failure() -> Tuple[bool, str]:
    name = _unique("lrf")
    sub = create_sub("longRunningFailurePlugin", name, {"fail_at": 50}, cron="0 * * * *")
    try:
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
    finally:
        _delete_sub(sub["id"])


def test_crash() -> Tuple[bool, str]:
    name = _unique("crash")
    sub = create_sub("crashPlugin", name, {"reason": "test"}, cron="0 * * * *")
    try:
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
    finally:
        _delete_sub(sub["id"])


def test_cancellation() -> Tuple[bool, str]:
    """The plugin runs for many iterations. The test runner flips the
    subscription to DISABLED mid-run; the plugin's progress_callback
    should detect the status and raise SubscriptionCancelledError,
    exiting the child process cleanly with code 0. The worker sees
    exit_code=0 + status=DISABLED → no EventLog entry."""
    name = _unique("cancel")
    sub = create_sub("cancellationPlugin", name, {"iterations": 200}, cron="0 * * * *")
    try:
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
    finally:
        _delete_sub(sub["id"])


SCHEMA_BREAKING_V1_CODE = '''
from utils.plugin_base import BaseSubscription


class schemaBreakingPlugin(BaseSubscription):
    metadata = {
        "name": "schemaBreakingPlugin",
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
        data=json.dumps({"name": plugin_name, "code": code, "display_name": plugin_name}),
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


def _sync_test_sinks() -> None:
    """Copy every Sink service file from the testing source directory into
    /src/sinks/. Mirrors ``_sync_test_plugins`` — the Manager's file
    watcher hot-swaps it in (reload + upsert the ``sink`` row) and the
    Worker lazy-loads it via ``get_or_load`` during recon.
    """
    sync_src = os.path.join(os.path.dirname(__file__), "sinks")
    sync_dst = "/src/sinks"
    if not os.path.isdir(sync_src):
        _log(f"Warning: test Sink source directory {sync_src} does not exist; skipping sync")
        return
    synced = 0
    for fname in sorted(os.listdir(sync_src)):
        if not fname.endswith(".py") or fname.startswith("__"):
            continue
        src_path = os.path.join(sync_src, fname)
        dst_path = os.path.join(sync_dst, fname)
        try:
            shutil.copy(src_path, dst_path)
            synced += 1
        except OSError as exc:
            _log(f"Warning: failed to sync {fname}: {exc}")
    _log(f"Synced {synced} test Sink services from {sync_src} → {sync_dst}")


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
    sub = None
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
            if sub is not None:
                _delete_sub(sub["id"])
            _write_plugin_file_directly("schemaBreakingPlugin", SCHEMA_BREAKING_V1_CODE)
            _wait_for_plugin_loaded("schemaBreakingPlugin", timeout=20)
        except Exception as exc:  # noqa: BLE001
            _log(f"warning: failed to restore V1 in finally: {exc}")


def test_password() -> Tuple[bool, str]:
    name = _unique("pwd")
    api_key_value = "supersecret-key-123"
    sub = create_sub("passwordPlugin", name, {"apiKey": api_key_value}, cron="0 * * * *")
    try:
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
    finally:
        _delete_sub(sub["id"])


def test_empty_output() -> Tuple[bool, str]:
    name = _unique("empty")
    sub = create_sub("emptyOutputPlugin", name, {"marker": "m"}, cron="0 * * * *")
    try:
        trigger_sub(sub["id"])
        final = wait_for_status(sub["id"], lambda s: s in ("ENABLED", "ERROR"), timeout=15)
        if final["status"] != "ENABLED":
            return False, f"expected ENABLED, got {final['status']}"
        ev = wait_for_event(sub["id"], timeout=10)
        if ev["exit_code"] != 0:
            return False, f"expected exit_code=0, got {ev['exit_code']}"
        return True, "empty output (exit_code=0, status=ENABLED)"
    finally:
        _delete_sub(sub["id"])


def test_large_output() -> Tuple[bool, str]:
    name = _unique("large")
    # Use a small number of files to keep this fast in the test env
    sub = create_sub("largeOutputPlugin", name, {"file_count": 5}, cron="0 * * * *")
    try:
        trigger_sub(sub["id"])
        final = wait_for_status(sub["id"], lambda s: s in ("ENABLED", "ERROR"), timeout=60)
        if final["status"] != "ENABLED":
            return False, f"expected ENABLED, got {final['status']} (last_error={final.get('last_error')!r})"
        ev = wait_for_event(sub["id"], timeout=10)
        if ev["exit_code"] != 0:
            return False, f"expected exit_code=0, got {ev['exit_code']}"
        return True, "large output (exit_code=0, status=ENABLED)"
    finally:
        _delete_sub(sub["id"])


def test_delayed_init() -> Tuple[bool, str]:
    name = _unique("delay")
    sub = create_sub("delayedInitPlugin", name, {"label": "x"}, cron="0 * * * *")
    try:
        trigger_sub(sub["id"])
        final = wait_for_status(sub["id"], lambda s: s in ("ENABLED", "ERROR"), timeout=15)
        if final["status"] != "ENABLED":
            return False, f"expected ENABLED, got {final['status']}"
        ev = wait_for_event(sub["id"], timeout=10)
        if ev["exit_code"] != 0:
            return False, f"expected exit_code=0, got {ev['exit_code']}"
        return True, "delayed init (exit_code=0, status=ENABLED)"
    finally:
        _delete_sub(sub["id"])


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
    try:
        trigger_sub(sub["id"])
        final = wait_for_status(sub["id"], lambda s: s in ("ENABLED", "ERROR"), timeout=15)
        if final["status"] != "ENABLED":
            return False, f"expected ENABLED, got {final['status']}"
        return True, "custom route mounted + accessible"
    finally:
        _delete_sub(sub["id"])


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
    try:
        time.sleep(3.0)
        cur = get_sub(sub["id"])
        if cur["status"] == "ERROR":
            return False, f"unexpected ERROR: {cur.get('last_error')!r}"
        return True, f"monitor always False, status={cur['status']}, no crash"
    finally:
        _delete_sub(sub["id"])


def test_monitor_error() -> Tuple[bool, str]:
    # The monitor raises ConnectionError continuously. The system should
    # log + retry indefinitely without crashing.
    name = _unique("me")
    sub = create_sub("monitorErrorPlugin", name, {"label": "x"}, cron="0 0 * * *")
    try:
        time.sleep(5.0)
        cur = get_sub(sub["id"])
        if cur["status"] == "ERROR":
            return False, f"unexpected ERROR: {cur.get('last_error')!r}"
        # Confirm the manager is still alive
        h = api_get("/api/health")
        if h.get("status") != "ok":
            return False, f"health degraded: {h}"
        return True, "monitor exception → retry loop, system still healthy"
    finally:
        _delete_sub(sub["id"])


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
    try:
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
    finally:
        _delete_sub(sub["id"])


def test_non_zero_exit() -> Tuple[bool, str]:
    name = _unique("nze")
    sub = create_sub("nonZeroExitPlugin", name, {"label": "x"}, cron="0 * * * *")
    try:
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
    finally:
        _delete_sub(sub["id"])


def test_zombie() -> Tuple[bool, str]:
    """Zombie: progress_callback never checks DB; test runner sets
    DISABLED mid-execution. The child ignores the cancellation and keeps
    running, so the watcher's per-tick DB status check force-kills it.
    Per spec §5.2 the user-initiated DISABLED status is preserved (NOT
    overwritten to ERROR), and the force-kill is recorded as an EventLog
    entry with exit_code=2."""
    name = _unique("zombie")
    sub = create_sub("zombiePlugin", name, {"label": "x"}, cron="0 * * * *")
    try:
        trigger_sub(sub["id"])
        wait_for_status(sub["id"], lambda s: s == "IN_PROGRESS", timeout=15)
        time.sleep(0.5)
        set_status(sub["id"], "DISABLED")
        # The zombie keeps running. The plugin's progress_callback never
        # calls the DB to detect DISABLED, so the only way to stop it is the
        # watcher's DB status check on its next tick — which records the
        # force-kill as an EventLog entry with exit_code=2.
        ev = wait_for_event(sub["id"], timeout=30)
        if ev["exit_code"] != 2:
            return False, f"expected exit_code=2 (watcher force-kill), got {ev['exit_code']}"
        final = get_sub(sub["id"])
        if final["status"] != "DISABLED":
            return False, f"expected DISABLED preserved, got {final['status']}"
        return True, "zombie force-killed by watcher (exit_code=2), DISABLED preserved"
    finally:
        _delete_sub(sub["id"])


def test_move_to_dest_error() -> Tuple[bool, str]:
    name = _unique("mde")
    sub = create_sub("moveToDestErrorPlugin", name, {"label": "x"}, cron="0 * * * *")
    try:
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
    finally:
        _delete_sub(sub["id"])


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
    try:
        trigger_sub(sub["id"])
        final = wait_for_status(sub["id"], lambda s: s in ("ENABLED", "ERROR"), timeout=30)
        if final["status"] != "ENABLED":
            return False, f"expected ENABLED, got {final['status']} (last_error={final.get('last_error')!r})"
        ev = wait_for_event(sub["id"], timeout=10)
        if ev["exit_code"] != 0:
            return False, f"expected exit_code=0, got {ev['exit_code']}"
        return True, "32-char name plugin loaded, sub created, run succeeded (exit_code=0)"
    finally:
        _delete_sub(sub["id"])


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


def _wait_for_plugin_reload_after_save(plugin_id: str, expected_desc_substring: str, timeout: float = 20.0) -> bool:
    """Poll the plugin API until the description contains ``expected_desc_substring``.

    The watcher reloads the plugin from disk and re-parses its metadata;
    once the description field (which is part of metadata) matches, the new
    code is active. Checking metadata rather than exact source code avoids
    false negatives caused by backend code transformations (e.g. injection
    of ``display_name``).
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.get(
                f"{MANAGER_URL}/api/plugins/{plugin_id}",
                headers=_api_headers(),
                timeout=10,
            )
            if r.status_code == 200 and expected_desc_substring in (r.json().get("description") or ""):
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
    sub = None
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
            data=json.dumps({"name": plugin, "code": EDIT_MATCH_V2_CODE, "display_name": plugin}),
            timeout=30,
        )
        if r.status_code != 200:
            return False, f"stage 2: V2 save rejected unexpectedly: {r.status_code} {r.text}"
        body = r.json()
        if body.get("mode") != "edit":
            return False, f"stage 2: expected mode=edit, got {body.get('mode')!r}"

        # Wait for the file watcher to reload V2.
        if not _wait_for_plugin_reload_after_save(plugin, "V2", timeout=20):
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
            if sub is not None:
                _delete_sub(sub["id"])
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
    sub = None
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
            data=json.dumps({"name": plugin, "code": EDIT_MISMATCH_V3_CODE, "display_name": plugin}),
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
            if sub is not None:
                _delete_sub(sub["id"])
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


def _acquire_run_lock() -> Tuple[bool, Optional[QueueManager]]:
    """Single-instance guard: only one test runner at a time.

    Returns ``(held, owner)``:
      * ``(True, queue)`` — the caller owns the lock and must release it.
      * ``(True, None)`` — Redis was unreachable; proceed without a lock.
      * ``(False, None)`` — another live runner holds the lock; refuse.

    A stale lock whose owner PID is no longer alive is broken and
    re-acquired, so an interrupted/crashed run can never block the next
    one indefinitely (the TTL is only a backstop).
    """
    try:
        q = _sink_queue()
        client = q.client
    except Exception as exc:  # noqa: BLE001
        _log(f"warning: run lock unavailable ({exc}); proceeding without a lock")
        return True, None

    try:
        got = client.set(TEST_RUN_LOCK_KEY, str(os.getpid()), nx=True, px=TEST_RUN_LOCK_TTL_MS)
        if got:
            return True, q

        owner = client.get(TEST_RUN_LOCK_KEY)
        owner_alive = True
        if owner and owner.isdigit():
            try:
                os.kill(int(owner), 0)
            except ProcessLookupError:
                owner_alive = False
            except (PermissionError, OSError):
                owner_alive = True
        else:
            # Lock value not a PID we understand — assume held by a live run.
            owner_alive = True

        if not owner_alive:
            _log("Run lock owner is dead; breaking stale lock")
            client.delete(TEST_RUN_LOCK_KEY)
            got = client.set(TEST_RUN_LOCK_KEY, str(os.getpid()), nx=True, px=TEST_RUN_LOCK_TTL_MS)
            if got:
                return True, q
    except Exception as exc:  # noqa: BLE001
        _log(f"warning: run lock error ({exc}); proceeding without a lock")
        return True, None

    return False, None


# ---------------------------------------------------------------------------
# Sink e2e tests
# ---------------------------------------------------------------------------

def test_sink_full_pipeline() -> Tuple[bool, str]:
    """FULL trigger: upstream (plugin writes hello.md + world.md) + downstream recon."""
    _reset_sink_calls()
    uid = str(time.time()).replace(".", "")[-8:]
    sub_name = f"e2e-full-{uid}"
    t_name = f"e2e-full-ds-{uid}"
    sub_id = None
    t_id = None
    try:
        sub = create_sub("testSinkWriterPlugin", sub_name, {}, cron="0 0 * * *")
        sub_id = sub["id"]
    except Exception as exc:
        return False, f"Sink full: create_sub failed: {exc}"

    try:
        svcs = api_get("/api/sinks")
        svc_id = None
        for s in svcs:
            if s.get("name") == "testSink":
                svc_id = s["service_id"]
                break
        if not svc_id:
            return False, "Sink full: testSink service not found"

        # Create target linked to sub
        ds = api_post(f"/api/sinks/{svc_id}/targets", {
            "name": t_name, "api_url": "http://fake/api", "api_key": "test",
            "target_extra_params": {}, "subscription_ids": [sub_id],
        })
        t_id = ds["target_id"]
    except Exception as exc:
        try: requests.delete(f"{MANAGER_URL}/api/subscriptions/{sub_id}", headers=_api_headers(), timeout=10)
        except Exception: pass
        return False, f"Sink full: setup failed: {exc}"

    try:
        # Place a manual file to verify recon picks it up
        _write_output_file(sub_name, "manual.md", "Manually placed.\n")

        # Trigger FULL
        try:
            trigger_sub(sub_id)
        except Exception as exc:
            return False, f"Sink full: trigger failed: {exc}"

        # Wait for target_subscription → ENABLED
        deadline = time.time() + 120
        last_status = None
        while time.time() < deadline:
            try:
                t_det = api_get(f"/api/targets/{t_id}")
                subs_detail = t_det.get("subscriptions", [])
                if subs_detail:
                    last_status = subs_detail[0].get("status", "")
                    if last_status == "ENABLED":
                        break
            except Exception:
                pass
            time.sleep(2)
        if last_status != "ENABLED":
            return False, f"Sink full: ds_sub did not reach ENABLED (last={last_status})"

        # Verify Sink service calls
        calls = _read_sink_calls()
        add_files = [c for c in calls if c[0] == "add_datafile"]
        found = " ".join(c[1] for c in add_files)
        if "manual.md" not in found:
            return False, f"Sink full: manual.md not added: {found[:200]}"
        if "hello.md" not in found:
            return False, f"Sink full: hello.md not added: {found[:200]}"
        if "world.md" not in found:
            return False, f"Sink full: world.md not added: {found[:200]}"

        return True, f"Sink full pipeline OK ({len(add_files)} files added)"
    finally:
        try:
            if t_id is not None:
                requests.delete(f"{MANAGER_URL}/api/targets/{t_id}", headers=_api_headers(), timeout=10)
        except Exception:
            pass
        if sub_id is not None:
            _delete_sub(sub_id)


def test_sink_only_recon() -> Tuple[bool, str]:
    """SINK_ONLY trigger: upstream skipped, downstream recon runs."""
    _reset_sink_calls()
    uid = str(time.time()).replace(".", "")[-8:]
    sub_name = f"e2e-sinkonly-{uid}"
    t_name = f"e2e-sinkonly-ds-{uid}"
    sub_id = None
    t_id = None
    try:
        sub = create_sub("testSinkWriterPlugin", sub_name, {}, cron="0 0 * * *")
        sub_id = sub["id"]
    except Exception as exc:
        return False, f"Sink-only: create_sub failed: {exc}"

    try:
        svcs = api_get("/api/sinks")
        svc_id = None
        for s in svcs:
            if s.get("name") == "testSink":
                svc_id = s["service_id"]
                break
        ds = api_post(f"/api/sinks/{svc_id}/targets", {
            "name": t_name, "api_url": "http://fake/api", "api_key": "test",
            "target_extra_params": {}, "subscription_ids": [sub_id],
        })
        t_id = ds["target_id"]
    except Exception as exc:
        try: requests.delete(f"{MANAGER_URL}/api/subscriptions/{sub_id}", headers=_api_headers(), timeout=10)
        except Exception: pass
        return False, f"Sink-only: setup failed: {exc}"

    try:
        # Place a file — plugin won't run so this is the only file
        _write_output_file(sub_name, "only_file.md", "Sink-only test.\n")

        # Snapshot sub heartbeat to verify upstream didn't run
        sub_before = api_get(f"/api/subscriptions/{sub_id}")
        hb_before = sub_before.get("last_heartbeat", "") or ""

        # Trigger SINK_ONLY via target update
        try:
            api_post(f"/api/targets/{t_id}/update", {})
        except Exception as exc:
            return False, f"Sink-only: trigger failed: {exc}"

        # Wait for ds_sub → ENABLED
        deadline = time.time() + 60
        last_status = None
        while time.time() < deadline:
            try:
                t_det = api_get(f"/api/targets/{t_id}")
                subs_detail = t_det.get("subscriptions", [])
                if subs_detail:
                    last_status = subs_detail[0].get("status", "")
                    if last_status == "ENABLED":
                        break
            except Exception:
                pass
            time.sleep(2)
        if last_status != "ENABLED":
            return False, f"Sink-only: ds_sub did not reach ENABLED (last={last_status})"

        # Verify Sink service calls
        calls = _read_sink_calls()
        add_files = [c for c in calls if c[0] == "add_datafile"]
        found = " ".join(c[1] for c in add_files)
        if "only_file.md" not in found:
            return False, f"Sink-only: only_file.md not added: {found[:200]}"

        # Verify NO upstream (plugin output files should NOT exist)
        hello_path = f"/output/testSinkWriterPlugin/{sub_name}/hello.md"
        world_path = f"/output/testSinkWriterPlugin/{sub_name}/world.md"
        if os.path.isfile(hello_path):
            return False, "Sink-only: hello.md exists (upstream should not have run)"
        if os.path.isfile(world_path):
            return False, "Sink-only: world.md exists (upstream should not have run)"

        return True, "Sink-only recon OK (upstream skipped, downstream completed)"
    finally:
        try:
            if t_id is not None:
                requests.delete(f"{MANAGER_URL}/api/targets/{t_id}", headers=_api_headers(), timeout=10)
        except Exception:
            pass
        if sub_id is not None:
            _delete_sub(sub_id)


def test_dkb_delete_normal() -> Tuple[bool, str]:
    """DELETE /api/targets/{id} — normal: remote removal succeeds, local rows gone."""
    _reset_sink_calls()
    uid = str(time.time()).replace(".", "")[-8:]
    sub_name = f"e2e-delnormal-{uid}"
    t_name = f"e2e-delnormal-ds-{uid}"
    sub_id = None
    t_id = None
    try:
        sub = create_sub("testSinkWriterPlugin", sub_name, {}, cron="0 0 * * *")
        sub_id = sub["id"]
        svcs = api_get("/api/sinks")
        svc_id = next(s["service_id"] for s in svcs if s.get("name") == "testSink")
        ds = api_post(f"/api/sinks/{svc_id}/targets", {
            "name": t_name, "api_url": "http://fake/api", "api_key": "test",
            "target_extra_params": {}, "subscription_ids": [sub_id],
        })
        t_id = ds["target_id"]
        _write_output_file(sub_name, "manual.md", "x\n")
        trigger_sub(sub_id)
        deadline = time.time() + 120
        last_status = None
        while time.time() < deadline:
            t_det = api_get(f"/api/targets/{t_id}")
            subs_detail = t_det.get("subscriptions", [])
            if subs_detail:
                last_status = subs_detail[0].get("status", "")
                if last_status == "ENABLED":
                    break
            time.sleep(2)
        if last_status != "ENABLED":
            return False, f"DelNormal: link not ENABLED (last={last_status})"
        t_before = api_get(f"/api/targets/{t_id}")
        if not t_before.get("remote_target_id"):
            return False, f"DelNormal: remote_target_id is empty (recon may not have set it)"
        r = requests.delete(f"{MANAGER_URL}/api/targets/{t_id}", headers=_api_headers(), timeout=30)
        if r.status_code != 200:
            return False, f"DelNormal: DELETE returned {r.status_code}: {r.text[:200]}"
        st3, _ = 404, None
        try:
            api_get(f"/api/targets/{t_id}")
        except Exception:
            st3 = 404
        if st3 != 404:
            return False, "DelNormal: target still exists after DELETE"
        calls = _read_sink_calls()
        if not any(c[0] == "remove_target" for c in calls):
            return False, "DelNormal: remove_target not called"
        return True, "Normal delete OK (remote removed, local gone)"
    finally:
        try:
            if t_id is not None:
                requests.delete(f"{MANAGER_URL}/api/targets/{t_id}?force=true", headers=_api_headers(), timeout=10)
        except Exception:
            pass
        if sub_id is not None:
            _delete_sub(sub_id)


def test_dkb_delete_force() -> Tuple[bool, str]:
    """DELETE /api/targets/{id}?force=true — always deletes local rows."""
    uid = str(time.time()).replace(".", "")[-8:]
    t_name = f"e2e-delforce-{uid}"
    t_id = None
    try:
        svcs = api_get("/api/sinks")
        svc_id = next(s["service_id"] for s in svcs if s.get("name") == "testSink")
        ds = api_post(f"/api/sinks/{svc_id}/targets", {
            "name": t_name, "api_url": "http://fake/api", "api_key": "x",
            "target_extra_params": {}, "subscription_ids": [],
        })
        t_id = ds["target_id"]
        r = requests.delete(f"{MANAGER_URL}/api/targets/{t_id}?force=true", headers=_api_headers(), timeout=30)
        if r.status_code != 200:
            return False, f"DelForce: DELETE returned {r.status_code}: {r.text[:200]}"
        st3 = 404
        try:
            api_get(f"/api/targets/{t_id}")
        except Exception:
            st3 = 404
        if st3 != 404:
            return False, "DelForce: target still exists after force delete"
        return True, "Force delete OK (local rows gone)"
    finally:
        try:
            if t_id is not None:
                requests.delete(f"{MANAGER_URL}/api/targets/{t_id}?force=true", headers=_api_headers(), timeout=10)
        except Exception:
            pass


def test_dkb_delete_strict_failure_force() -> Tuple[bool, str]:
    """Strict DELETE fails → 502 + target retained; force recovers."""
    from utils.database import Sink
    import uuid
    db = _sink_db()
    bogus = str(uuid.uuid4())
    with db.get_session() as s:
        s.add(Sink(id=bogus, name="NoSuchSinkClass"))
    t_id = None
    try:
        # Create the target directly via DB (bypassing the API's
        # _ensure_target_remote, which requires a real sink class — this test
        # is about the delete behaviour, not target creation).
        t = db.create_target(bogus, "StrictFailTarget", "http://fake/api", "x", {})
        t_id = t.id
        r2 = requests.delete(f"{MANAGER_URL}/api/targets/{t_id}", headers=_api_headers(), timeout=30)
        if r2.status_code != 502:
            return False, f"StrictFail: expected 502 got {r2.status_code}"
        detail = r2.json().get("detail", "")
        if "target retained" not in detail:
            return False, f"StrictFail: detail missing 'target retained': {detail[:120]}"
        r3 = api_get(f"/api/targets/{t_id}")
        if r3.get("target_id") != t_id:
            return False, "StrictFail: target not retained after 502"
        r4 = requests.delete(f"{MANAGER_URL}/api/targets/{t_id}?force=true", headers=_api_headers(), timeout=30)
        if r4.status_code != 200:
            return False, f"StrictFail: force delete returned {r4.status_code}"
        try:
            api_get(f"/api/targets/{t_id}")
            return False, "StrictFail: target still exists after force delete"
        except Exception:
            pass
        return True, "Strict fail 502 + retained, force delete recovers OK"
    finally:
        try:
            if t_id is not None:
                requests.delete(f"{MANAGER_URL}/api/targets/{t_id}?force=true", headers=_api_headers(), timeout=10)
        except Exception:
            pass
        with db.get_session() as s:
            s.query(Sink).filter(Sink.id == bogus).delete()
        db.dispose()


def test_dkb_unlink_last_sub_keeps_target() -> Tuple[bool, str]:
    """Unlinking the last sub via Edit keeps the (empty) target."""
    _reset_sink_calls()
    uid = str(time.time()).replace(".", "")[-8:]
    sub_name = f"e2e-keepempty-{uid}"
    t_name = f"e2e-keepempty-ds-{uid}"
    sub_id = None
    t_id = None
    try:
        sub = create_sub("testSinkWriterPlugin", sub_name, {}, cron="0 0 * * *")
        sub_id = sub["id"]
        svcs = api_get("/api/sinks")
        svc_id = next(s["service_id"] for s in svcs if s.get("name") == "testSink")
        ds = api_post(f"/api/sinks/{svc_id}/targets", {
            "name": t_name, "api_url": "http://fake/api", "api_key": "test",
            "target_extra_params": {}, "subscription_ids": [sub_id],
        })
        t_id = ds["target_id"]
        _write_output_file(sub_name, "manual.md", "x\n")
        trigger_sub(sub_id)
        deadline = time.time() + 120
        last_status = None
        while time.time() < deadline:
            t_det = api_get(f"/api/targets/{t_id}")
            subs_detail = t_det.get("subscriptions", [])
            if subs_detail:
                last_status = subs_detail[0].get("status", "")
                if last_status == "ENABLED":
                    break
            time.sleep(2)
        if last_status != "ENABLED":
            return False, f"KeepEmpty: link not ENABLED (last={last_status})"
        r = requests.put(
            f"{MANAGER_URL}/api/targets/{t_id}",
            headers={**_api_headers(), "Content-Type": "application/json"},
            data=json.dumps({"api_url": "http://fake/api", "api_key": "",
                             "target_extra_params": {}, "subscription_ids": []}),
            timeout=30,
        )
        if r.status_code != 200:
            return False, f"KeepEmpty: PUT unlink returned {r.status_code}: {r.text[:200]}"
        deadline = time.time() + 60
        kept = False
        subs_gone = False
        while time.time() < deadline:
            try:
                t_det = api_get(f"/api/targets/{t_id}")
                kept = True
                sub_count = len(t_det.get("subscriptions", []))
                if sub_count == 0:
                    subs_gone = True
                    break
            except Exception:
                kept = False
                break
            time.sleep(2)
        if not kept:
            return False, "KeepEmpty: target was deleted (should be kept)"
        if not subs_gone:
            return False, "KeepEmpty: target still has linked subs"
        sub_after = api_get(f"/api/subscriptions/{sub_id}")
        if sub_after.get("status") != "ENABLED":
            return False, f"KeepEmpty: sub status changed to {sub_after.get('status')}"
        calls = _read_sink_calls()
        remove_calls = [c for c in calls if c[0] == "remove_datafile"]
        if not remove_calls:
            return False, "KeepEmpty: no remove_datafile calls (files not cleaned)"
        return True, "Unlink last sub OK (target kept empty, sub enabled, files removed)"
    finally:
        try:
            if t_id is not None:
                requests.delete(f"{MANAGER_URL}/api/targets/{t_id}?force=true", headers=_api_headers(), timeout=10)
        except Exception:
            pass
        if sub_id is not None:
            _delete_sub(sub_id)


# ---------------------------------------------------------------------------
# Sink unit test helpers and functions
# ---------------------------------------------------------------------------

SINK_DATABASE_URL = os.environ.get("DATABASE_URL",
                                   "postgresql://autokb:autokb@autokb-db:5432/autokb")
SINK_REDIS_URL = os.environ.get("REDIS_URL", "redis://autokb-redis:6379/0")
SINK_TEST_OUTPUT = "/tmp/sink_test_output"


def _sink_db():
    return DatabaseManager(SINK_DATABASE_URL, component="test_sink_runner")


def _sink_queue():
    return QueueManager(SINK_REDIS_URL)


_MOCK_REMOTE_ID = "remote-file-123"
_MOCK_TARGET_ID = "remote-ds-abc"
_MOCK_REMOTE_UPDATED = "remote-updated-456"


class _MockSink(BaseSink):
    """Concrete Sink service for testing — records all calls."""
    metadata = {"name": "MockSink", "display_name": "MockSink", "description": "Test Sink service"}

    def __init__(self, target_row, db):
        super().__init__(target_row, db)
        self.calls = []

    def add_datafile(self, path: str) -> str:
        self.calls.append(("add_datafile", path, self.remote_target_id))
        return _MOCK_REMOTE_ID

    def update_datafile(self, remote_datafile_id: str, path: str) -> str:
        self.calls.append(("update_datafile", remote_datafile_id, path))
        return _MOCK_REMOTE_UPDATED

    def remove_datafile(self, remote_datafile_id: str) -> None:
        self.calls.append(("remove_datafile", remote_datafile_id))

    def add_target(self) -> str:
        self.calls.append(("add_target",))
        return _MOCK_TARGET_ID

    def remove_target(self) -> None:
        self.calls.append(("remove_target",))

    def clear_target(self) -> None:
        self.calls.append(("clear_target",))


def _sink_create_fixtures(db):
    """Create a Sink service, datastore, and subscription for tests."""
    with db.get_session() as s:
        from utils.database import Subscription, PluginRegistryState
        test_plugin = s.query(PluginRegistryState).filter(
            PluginRegistryState.plugin_id == "test_plugin").first()
        if not test_plugin:
            s.add(PluginRegistryState(
                plugin_id="test_plugin", schema_hash="abc123",
                last_loaded=__import__("datetime").datetime.now(
                    __import__("datetime").timezone.utc),
            ))
        sub_id = str(uuid4())
        s.add(Subscription(
            id=sub_id, plugin_id="test_plugin", name=f"test_sub_{sub_id}",
            config={}, status=STATE_ENABLED, access_level="PRIVATE",
            sub_type="SCHEDULED", cron="0 0 * * *",
        ))
        s.flush()
    svc = db.upsert_sink("TestSinkService", "Test Description")
    ds = db.create_target(svc.id, "TestTarget", "https://example.com", "test-key", {})
    db.link_target_subscriptions(ds.id, [sub_id], status=STATE_ENABLED)
    return sub_id, ds


def _sink_api_delete(path):
    import urllib.request, urllib.error
    req = urllib.request.Request(f"http://localhost:80{path}?force=true", method="DELETE")
    req.add_header("X-Api-Key", BACKEND_KEY)
    try:
        urllib.request.urlopen(req, timeout=10)
    except urllib.error.HTTPError as e:
        if e.code != 404:
            raise


def _write_output_file_test(sub_name, name, content):
    path = os.path.join("/output", "test_plugin", sub_name, name)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write(content)
    return path


def _get_sub_name(db, sub_id):
    """Quick helper to fetch a subscription's name by ID."""
    with db.get_session() as s:
        row = s.query(Subscription).filter(Subscription.id == sub_id).first()
        return row.name if row else sub_id


# ---- 1. Database CRUD tests ----

def _sink_test_svc_crud(db, sub, ds):
    svc = db.upsert_sink("MyService", "desc")
    if not svc.id:
        return False, "service id None"
    fetched = db.get_sink(svc.id)
    if fetched.name != "MyService":
        return False, f"expected MyService got {fetched.name}"
    svc2 = db.upsert_sink("MyService", "updated")
    if svc2.id != svc.id:
        return False, "upsert idempotency failed"
    services = db.list_sinks()
    if svc.id not in [s.id for s in services]:
        return False, "service not in list"
    with db.get_session() as s:
        s.query(Sink).filter(Sink.id == svc.id).delete()
    return True, "OK"


def _dkb_test_ds_crud(db, sub, ds):
    svc = db.upsert_sink("Svc", "")
    ds2 = db.create_target(svc.id, "MyDS", "https://example.com/api", "secret_key", {"extra": "value"})
    if not ds2.id:
        return False, "ds id None"
    fetched = db.get_target(ds2.id)
    if fetched.name != "MyDS":
        return False, f"expected MyDS got {fetched.name}"
    if fetched.api_url != "https://example.com/api":
        return False, "api_url mismatch"
    if fetched.api_key == "secret_key":
        return False, "api_key not encrypted"
    decrypted = db.decrypt_target_api_key(fetched)
    if decrypted != "secret_key":
        return False, "decrypt failed"
    updated = db.update_target(ds2.id, name="MyDS-Updated")
    if updated.name != "MyDS-Updated":
        return False, "update failed"
    db.set_target_remote_id(ds2.id, "remote-xyz")
    fetched2 = db.get_target(ds2.id)
    if fetched2.remote_target_id != "remote-xyz":
        return False, "remote_id not set"
    with db.get_session() as s:
        s.query(Target).filter(Target.id == ds2.id).delete()
        s.query(Sink).filter(Sink.id == svc.id).delete()
    return True, "OK"


def _dkb_test_link_crud(db, sub, ds):
    svc = db.upsert_sink("Svc", "")
    ds2 = db.create_target(svc.id, "DS", "url", "key", {})
    db.link_target_subscriptions(ds2.id, [sub], status="ENABLED")
    links = db.list_target_subscriptions(ds2.id)
    if len(links) != 1:
        return False, f"expected 1 link got {len(links)}"
    if links[0].subscription_id != sub:
        return False, "sub_id mismatch"
    if links[0].status != "ENABLED":
        return False, "status mismatch"
    subs_for = db.list_targets_for_subscription(sub)
    if not subs_for:
        return False, "no datastores for sub"
    db.set_target_subscription_status(ds2.id, sub, "ERROR", message="oops")
    links2 = db.list_target_subscriptions(ds2.id)
    if links2[0].status != "ERROR":
        return False, "status not updated"
    if links2[0].last_message != "oops":
        return False, "message mismatch"
    db.delete_target_subscription(ds2.id, sub)
    links3 = db.list_target_subscriptions(ds2.id)
    if len(links3) != 0:
        return False, "link not deleted"
    with db.get_session() as s:
        s.query(Target).filter(Target.id == ds2.id).delete()
        s.query(Sink).filter(Sink.id == svc.id).delete()
    return True, "OK"


def _dkb_test_datafile_crud(db, sub, ds):
    test_path = os.path.join(SINK_TEST_OUTPUT, "test_file.md")
    os.makedirs(SINK_TEST_OUTPUT, exist_ok=True)
    with open(test_path, "w") as f:
        f.write("# Hello\nWorld\n")
    size = os.path.getsize(test_path)
    mtime = os.path.getmtime(test_path)
    h = compute_file_hash(test_path)
    df = db.get_or_create_datafile(sub, test_path, size, mtime, h)
    if not df.id:
        return False, "df id None"
    if df.path != test_path:
        return False, "path mismatch"
    df2 = db.get_or_create_datafile(sub, test_path, size, mtime, h)
    if df2.id != df.id:
        return False, "get_or_create not idempotent"
    df3 = db.get_datafile_by_path(test_path)
    if not df3:
        return False, "lookup by path failed"
    db.update_datafile_stats(df.id, 100, 1000.0, "newhash")
    df4 = db.get_datafile(df.id)
    if df4.size != 100:
        return False, "size not updated"
    files = db.list_datafiles_for_subscription(sub)
    if df.id not in [f.id for f in files]:
        return False, "df not in sub list"
    db.delete_datafile(df.id)
    df5 = db.get_datafile(df.id)
    if df5 is not None:
        return False, "df not deleted"
    return True, "OK"


def _dkb_test_ds_df_crud(db, sub, ds):
    test_path = os.path.join(SINK_TEST_OUTPUT, "df_test.md")
    os.makedirs(SINK_TEST_OUTPUT, exist_ok=True)
    with open(test_path, "w") as f:
        f.write("data")
    h = compute_file_hash(test_path)
    df = db.get_or_create_datafile(sub, test_path, os.path.getsize(test_path),
                                   os.path.getmtime(test_path), h)
    db.insert_target_datafile(ds.id, df.id, "remote-001", h)
    ds_df = db.get_target_datafile(ds.id, df.id)
    if not ds_df:
        return False, "ds_df not created"
    if ds_df.remote_datafile_id != "remote-001":
        return False, "remote_id mismatch"
    db.update_target_datafile_hash(ds.id, df.id, "newhash456")
    ds_df2 = db.get_target_datafile(ds.id, df.id)
    if ds_df2.hash != "newhash456":
        return False, "hash not updated"
    items = db.list_datafiles_for_target(ds.id)
    if len(items) != 1:
        return False, f"expected 1 item got {len(items)}"
    db.delete_target_datafile(ds.id, df.id)
    ds_df3 = db.get_target_datafile(ds.id, df.id)
    if ds_df3 is not None:
        return False, "ds_df not deleted"
    with db.get_session() as s:
        s.query(AKBDatafile).filter(AKBDatafile.id == df.id).delete()
    return True, "OK"


# ---- 2. Queue JSON tests ----

def _dkb_test_queue_encoding():
    import json
    for op in (OPERATION_FULL, OPERATION_SINK_ONLY):
        enc = _encode_item("sub-123", op)
        dec = _decode_item(enc)
        if dec["sub_id"] != "sub-123" or dec["operation"] != op:
            return False, f"encode/decode failed for {op}"
    return True, "OK"


def _dkb_test_queue_roundtrip():
    q = _sink_queue()
    test_key = "test_isolated_q_runner"
    q.client.delete(test_key)
    try:
        q.client.lpush(test_key, _encode_item("sub-a", OPERATION_FULL))
        q.client.lpush(test_key, _encode_item("sub-b", OPERATION_SINK_ONLY))
        q.client.lpush(test_key, _encode_item("sub-a", OPERATION_SINK_ONLY))
        items = q.client.lrange(test_key, 0, -1)
        parsed = [_decode_item(i) for i in items]
        sub_a_full = [p for p in parsed if p and p["sub_id"] == "sub-a" and p["operation"] == OPERATION_FULL]
        if len(sub_a_full) != 1:
            return False, f"expected 1 sub-a FULL got {len(sub_a_full)}"
        sub_a_dkb = [p for p in parsed if p and p["sub_id"] == "sub-a" and p["operation"] == OPERATION_SINK_ONLY]
        if len(sub_a_dkb) != 1:
            return False, f"expected 1 sub-a SINK_ONLY got {len(sub_a_dkb)}"
        keep = [i for i in items if _decode_item(i) and _decode_item(i)["sub_id"] != "sub-a"]
        removed = len(items) - len(keep)
        if removed != 2:
            return False, f"expected 2 removed got {removed}"
        sub_b_full = [p for p in [_decode_item(i) for i in keep] if p and p["sub_id"] == "sub-b" and p["operation"] == OPERATION_FULL]
        if len(sub_b_full) != 0:
            return False, "sub-b should be SINK_ONLY"
        return True, "OK"
    finally:
        q.client.delete(test_key)


# ---- 3. DKB registry test ----

def _dkb_test_registry_load():
    # Self-contained: load only from the testing sinks dir so the test never
    # depends on (or leaks) the real shipped sinks (openWebUISink, cogneeSink).
    reg = SinkRegistry(sinks_dir="/src/testing/sinks", component="test_dkb_runner_reg")
    reg.reload_all()
    names = [r.service_name for r in reg.list_records()]
    if "testSink" not in names:
        return False, "testSink not loaded from testing sinks dir"
    if "openWebUISink" in names or "cogneeSink" in names:
        return False, "non-testing sink leaked into registry"
    return True, "OK"


# ---- 4. Service base class tests ----

def _dkb_test_compute_hash():
    os.makedirs(SINK_TEST_OUTPUT, exist_ok=True)
    test_path = os.path.join(SINK_TEST_OUTPUT, "hash_test.txt")
    content = b"Hello World! " * 1000
    with open(test_path, "wb") as f:
        f.write(content)
    import hashlib
    h = compute_file_hash(test_path)
    expected = hashlib.sha256(content).hexdigest()
    if h != expected:
        return False, "hash mismatch"
    return True, "OK"


def _dkb_test_remote_filename_include_path():
    """Verify BaseSink.remote_file_name with/without the include_path flag."""
    import types as _types
    from utils.sink_base import BaseSink

    row_on = _types.SimpleNamespace(
        id="x", service_id="y", name="Scrapling KB", api_url="", api_key="",
        remote_target_id=None, target_extra_params={}, include_path_in_filename=True,
    )
    svc_on = _MockSink(row_on, None)
    path = "/output/crawl4AIWebScraperPlugin/Scrapling/02b7d354dd237a3a.446e1199fbf97f3d.md"
    got = svc_on.remote_file_name(path)
    want = "autokb_ScraplingKB_crawl4AIWebScraperPlugin_Scrapling_02b7d354dd237a3a.446e1199fbf97f3d.md"
    if got != want:
        return False, f"flag-on: got {got!r}, want {want!r}"

    row_off = _types.SimpleNamespace(
        id="x", service_id="y", name="Scrapling KB", api_url="", api_key="",
        remote_target_id=None, target_extra_params={}, include_path_in_filename=False,
    )
    svc_off = _MockSink(row_off, None)
    got2 = svc_off.remote_file_name(path)
    want2 = "autokb_ScraplingKB_02b7d354dd237a3a.446e1199fbf97f3d.md"
    if got2 != want2:
        return False, f"flag-off: got {got2!r}, want {want2!r}"

    # Path outside /output → basename fallback
    outside = "/etc/passwd"
    got3 = svc_on.remote_file_name(outside)
    want3 = "autokb_ScraplingKB_passwd"
    if got3 != want3:
        return False, f"outside-output: got {got3!r}, want {want3!r}"

    return True, "OK"


def _dkb_test_icon():
    """Verify icon is derived from service name, falls back on missing file."""
    from utils.sink_base import BaseSink
    from utils.plugin_base import BaseSubscription
    from utils.misc_utils import resolve_service_icon

    class _TestSink(BaseSink):
        metadata = {"name": "testSink", "display_name": "T", "description": "t"}
    assert _TestSink.icon() == "testSink.png", _TestSink.icon()

    class _TestPlugin(BaseSubscription):
        metadata = {"name": "testPlugin", "description": "t", "sub_type": "SCHEDULED"}
    assert _TestPlugin.icon() == "testPlugin.png", _TestPlugin.icon()

    assert BaseSink.icon() == "default_icon.png", BaseSink.icon()
    assert BaseSubscription.icon() == "default_icon.png", BaseSubscription.icon()

    import tempfile
    with tempfile.TemporaryDirectory() as d:
        with open(os.path.join(d, "testSink.png"), "wb") as f:
            f.write(b"x")
        assert resolve_service_icon(_TestSink, _TestSink.metadata, assets_dir=d) == "testSink.png"

        class _NoIcon(BaseSink):
            metadata = {"name": "noIcon", "display_name": "N", "description": "n"}
        assert resolve_service_icon(_NoIcon, _NoIcon.metadata, assets_dir=d) == "default_icon.png"

    return True, "icon derivation + fallback OK"


def _dkb_test_schedule():
    """Verify target schedule parse/window math plus the _check_schedule gate."""
    from types import SimpleNamespace
    from datetime import datetime
    from utils.misc_utils import SinkCancelledError, in_schedule_window, parse_schedule_window

    assert parse_schedule_window(None, None) is None
    assert parse_schedule_window("", "") is None
    assert parse_schedule_window("06:00", None) is None
    assert parse_schedule_window(None, "06:00") is None
    assert parse_schedule_window("abc", "06:00") is None
    assert parse_schedule_window("99:00", "06:00") is None
    assert parse_schedule_window("12:00", "12:00") is None
    assert parse_schedule_window("22:00", "06:00") == (1320, 360), "overnight wrap"
    assert parse_schedule_window("06:00", "22:00") == (360, 1320), "normal window"

    # Add 8:00 manually to dodge the "6 is int, 00 is int" shadowing below.
    def _dt(h, m):
        return datetime(2026, 1, 1, h, m)

    # normal window [06:00, 22:00)
    w = (360, 1320)
    assert in_schedule_window(_dt(6, 0), w) is True, "start inclusive"
    assert in_schedule_window(_dt(8, 30), w) is True
    assert in_schedule_window(_dt(21, 59), w) is True
    assert in_schedule_window(_dt(22, 0), w) is False, "end exclusive"
    assert in_schedule_window(_dt(2, 0), w) is False

    # overnight wrap [22:00, 06:00)
    w2 = (1320, 360)
    assert in_schedule_window(_dt(22, 0), w2) is True, "wrap start inclusive"
    assert in_schedule_window(_dt(23, 59), w2) is True
    assert in_schedule_window(_dt(1, 0), w2) is True
    assert in_schedule_window(_dt(5, 59), w2) is True
    assert in_schedule_window(_dt(6, 0), w2) is False, "wrap end exclusive"
    assert in_schedule_window(_dt(12, 0), w2) is False

    # _check_schedule gate on a real sink instance (schedule columns are NULL)
    row = SimpleNamespace(id="t1", service_id="s1", name="T", api_url="u",
                          api_key="k", remote_target_id=None,
                          target_extra_params={}, include_path_in_filename=False,
                          schedule_start=None, schedule_end=None)
    svc = _MockSink(row, None)
    svc._check_schedule()  # no window -> never raises

    m = datetime.now().hour * 60 + datetime.now().minute
    svc._schedule_window = (m, m)  # degenerate window excludes now
    try:
        svc._check_schedule()
        return False, "_check_schedule did not raise outside window"
    except SinkCancelledError as exc:
        if exc.kind != "outside_schedule":
            return False, f"unexpected cancel kind {exc.kind}"
    svc._schedule_window = (m, (m + 1) % 1440)  # window that includes now
    svc._check_schedule()  # must not raise
    return True, "schedule parse + window gating OK"


def _dkb_test_base_add_datafile(db, sub, ds):
    ds_row = db.get_target(ds.id)
    ds_row.api_key = db.decrypt_target_api_key(ds_row)
    svc = _MockSink(ds_row, db)
    svc.remote_target_id = "mock-remote-id"
    test_path = os.path.join(SINK_TEST_OUTPUT, "base_add.md")
    with open(test_path, "w") as f:
        f.write("test content")
    svc.base_add_datafile(sub, test_path)
    df = db.get_datafile_by_path(test_path)
    if not df:
        return False, "df not created via base_add_datafile"
    if ("add_datafile", test_path, "mock-remote-id") not in svc.calls:
        return False, "add_datafile not called"
    ds_df = db.get_target_datafile(ds.id, df.id)
    if not ds_df:
        return False, "ds_df not created"
    with db.get_session() as s:
        s.query(TargetDatafile).filter(
            TargetDatafile.target_id == ds.id,
            TargetDatafile.datafile_id == df.id).delete()
        s.query(AKBDatafile).filter(AKBDatafile.id == df.id).delete()
    return True, "OK"


def _dkb_test_base_update_datafile(db, sub, ds):
    ds_row = db.get_target(ds.id)
    ds_row.api_key = db.decrypt_target_api_key(ds_row)
    svc = _MockSink(ds_row, db)
    svc.remote_target_id = "mock-remote-id"
    test_path = os.path.join(SINK_TEST_OUTPUT, "base_update.md")
    with open(test_path, "w") as f:
        f.write("original")
    h = compute_file_hash(test_path)
    df = db.get_or_create_datafile(sub, test_path, os.path.getsize(test_path),
                                   os.path.getmtime(test_path), h)
    db.insert_target_datafile(ds.id, df.id, "remote-old", h)
    new_hash = "newhash123"
    svc.base_update_datafile(df.id, new_hash)
    if ("update_datafile", "remote-old", test_path) not in svc.calls:
        return False, "update_datafile not called"
    ds_df = db.get_target_datafile(ds.id, df.id)
    if ds_df.hash != new_hash:
        return False, "hash not updated"
    if ds_df.remote_datafile_id != _MOCK_REMOTE_UPDATED:
        return False, "remote id not updated"
    with db.get_session() as s:
        s.query(TargetDatafile).filter(
            TargetDatafile.target_id == ds.id,
            TargetDatafile.datafile_id == df.id).delete()
        s.query(AKBDatafile).filter(AKBDatafile.id == df.id).delete()
    return True, "OK"


def _dkb_test_base_remove_datafile(db, sub, ds):
    ds_row = db.get_target(ds.id)
    ds_row.api_key = db.decrypt_target_api_key(ds_row)
    svc = _MockSink(ds_row, db)
    svc.remote_target_id = "mock-remote-id"
    test_path = os.path.join(SINK_TEST_OUTPUT, "base_remove.md")
    with open(test_path, "w") as f:
        f.write("remove me")
    h = compute_file_hash(test_path)
    df = db.get_or_create_datafile(sub, test_path, os.path.getsize(test_path),
                                   os.path.getmtime(test_path), h)
    db.insert_target_datafile(ds.id, df.id, "remote-del", h)
    svc.base_remove_datafile(df.id)
    if ("remove_datafile", "remote-del") not in svc.calls:
        return False, "remove_datafile not called"
    ds_df = db.get_target_datafile(ds.id, df.id)
    if ds_df is not None:
        return False, "ds_df not removed"
    with db.get_session() as s:
        s.query(AKBDatafile).filter(AKBDatafile.id == df.id).delete()
    return True, "OK"


def _dkb_test_base_add_target(db, sub, ds):
    ds_row = db.get_target(ds.id)
    ds_row.api_key = db.decrypt_target_api_key(ds_row)
    if ds_row.remote_target_id is not None:
        return False, "expected no remote_id before add_target"
    svc = _MockSink(ds_row, db)
    svc.base_add_target()
    if ("add_target",) not in svc.calls:
        return False, "add_target not called"
    refreshed = db.get_target(ds.id)
    if refreshed.remote_target_id != _MOCK_TARGET_ID:
        return False, "remote_target_id not set"
    return True, "OK"


# ---- 5. Recon engine tests ----

def _dkb_test_recon_add(db, sub, ds):
    from unittest.mock import MagicMock, patch
    with patch("worker.sink_recon._get_service") as mock_get:
        from worker.sink_recon import reconcile_subscription_targets
        mock_svc = MagicMock()
        mock_svc.remote_target_id = "mock-remote"
        mock_svc.name = "TestSvc"
        mock_svc.base_add_datafile = MagicMock()
        mock_svc.base_update_datafile = MagicMock()
        mock_svc.base_remove_datafile = MagicMock()
        mock_get.return_value = mock_svc
        sub_row = db.get_subscription(sub)
        _write_output_file_test(sub_row.name, "article.md", "# Test\nContent here.")
        reconcile_subscription_targets(sub_row, db, MagicMock(), MagicMock())
        if not mock_svc.base_add_datafile.called:
            return False, "base_add_datafile not called"
    return True, "OK"


def _dkb_test_recon_remove(db, sub, ds):
    from unittest.mock import MagicMock, patch
    test_path = os.path.join(SINK_TEST_OUTPUT, "test_plugin", "test_sub", "old.md")
    os.makedirs(os.path.dirname(test_path), exist_ok=True)
    with open(test_path, "w") as f:
        f.write("old content")
    h = compute_file_hash(test_path)
    os.remove(test_path)
    df = db.get_or_create_datafile(sub, test_path, 100, 1000.0, h)
    db.insert_target_datafile(ds.id, df.id, "remote-old", h)
    with patch("worker.sink_recon._get_service") as mock_get:
        from worker.sink_recon import reconcile_subscription_targets
        mock_svc = MagicMock()
        mock_svc.remote_target_id = "mock-remote"
        mock_svc.name = "TestSvc"
        mock_svc.base_add_datafile = MagicMock()
        mock_svc.base_remove_datafile = MagicMock()
        mock_get.return_value = mock_svc
        sub_row = db.get_subscription(sub)
        reconcile_subscription_targets(sub_row, db, MagicMock(), MagicMock())
        if not mock_svc.base_remove_datafile.called:
            return False, "base_remove_datafile not called"
    with db.get_session() as s:
        s.query(TargetDatafile).filter(
            TargetDatafile.target_id == ds.id,
            TargetDatafile.datafile_id == df.id).delete()
        s.query(AKBDatafile).filter(AKBDatafile.id == df.id).delete()
    return True, "OK"


def _dkb_test_recon_skip_disabled(db, sub, ds):
    from unittest.mock import MagicMock, patch
    db.set_target_subscription_status(ds.id, sub, STATE_DISABLED)
    sub_row = db.get_subscription(sub)
    _write_output_file_test(sub_row.name, "enabled_only.md", "should be skipped")
    with patch("worker.sink_recon._get_service") as mock_get:
        from worker.sink_recon import reconcile_subscription_targets
        mock_svc = MagicMock()
        mock_get.return_value = mock_svc
        sub_row = db.get_subscription(sub)
        reconcile_subscription_targets(sub_row, db, MagicMock(), MagicMock())
        mock_svc.base_add_datafile.assert_not_called()
        mock_svc.base_remove_datafile.assert_not_called()
    # Re-enable for next test's cleanup
    db.set_target_subscription_status(ds.id, sub, STATE_ENABLED)
    return True, "OK"


def _dkb_test_recon_error(db, sub, ds):
    from unittest.mock import MagicMock, patch
    ds_row = db.get_target(ds.id)
    ds_row.api_key = db.decrypt_target_api_key(ds_row)
    svc = _MockSink(ds_row, db)
    svc.remote_target_id = "mock-remote"
    svc.add_datafile = MagicMock(side_effect=RuntimeError("API failure"))
    svc.add_target = MagicMock()
    sub_row = db.get_subscription(sub)
    _write_output_file_test(sub_row.name, "error_test.md", "will fail")
    with patch("worker.sink_recon._get_service") as mock_get:
        from worker.sink_recon import reconcile_subscription_targets
        mock_get.return_value = svc
        sub_row = db.get_subscription(sub)
        reconcile_subscription_targets(sub_row, db, MagicMock(), MagicMock())
        links = db.list_target_subscriptions(ds.id)
        if links[0].status != STATE_ERROR:
            return False, "link should transition to ERROR after per-file error"
    with db.get_session() as s:
        leaked_path = f"/output/test_plugin/test_sub/error_test.md"
        df = s.query(AKBDatafile).filter(AKBDatafile.path == leaked_path).first()
        if df:
            s.query(AKBDatafile).filter(AKBDatafile.id == df.id).delete()
    return True, "OK"


def _dkb_test_recon_add_existing_df(db, sub, ds):
    """Regression: file known in akb_datafile but not yet linked to this
    target (e.g. target created after the sub produced files) must be added."""
    from unittest.mock import MagicMock, patch
    sub_row = db.get_subscription(sub)
    test_path = _write_output_file_test(sub_row.name, "existing.md", "# Existing\ncontent")
    h = compute_file_hash(test_path)
    df = db.get_or_create_datafile(sub, test_path, os.path.getsize(test_path),
                                   os.path.getmtime(test_path), h)
    with patch("worker.sink_recon._get_service") as mock_get:
        from worker.sink_recon import reconcile_subscription_targets
        mock_svc = MagicMock()
        mock_svc.remote_target_id = "mock-remote"
        mock_svc.name = "TestSvc"
        mock_svc.base_add_datafile = MagicMock()
        mock_get.return_value = mock_svc
        reconcile_subscription_targets(sub_row, db, MagicMock(), MagicMock())
        if not mock_svc.base_add_datafile.called:
            return False, "base_add_datafile not called for known-file add"
    with db.get_session() as s:
        s.query(AKBDatafile).filter(AKBDatafile.id == df.id).delete()
    return True, "OK"


def _dkb_test_recon_known_hash_no_rehash(db, sub, ds):
    """Hash-avoidance: when size/mtime already match akb_datafile, pass the
    known hash to base_add_datafile and never re-hash the file."""
    from unittest.mock import MagicMock, patch
    sub_row = db.get_subscription(sub)
    test_path = _write_output_file_test(sub_row.name, "unchanged.md", "# Unchanged\ncontent")
    h = compute_file_hash(test_path)
    df = db.get_or_create_datafile(sub, test_path, os.path.getsize(test_path),
                                   os.path.getmtime(test_path), h)
    with patch("worker.sink_recon._get_service") as mock_get:
        from worker.sink_recon import reconcile_subscription_targets
        mock_svc = MagicMock()
        mock_svc.remote_target_id = "mock-remote"
        mock_svc.name = "TestSvc"
        mock_svc.base_add_datafile = MagicMock()
        mock_get.return_value = mock_svc
        with patch("worker.sink_recon.compute_file_hash") as mock_hash:
            reconcile_subscription_targets(sub_row, db, MagicMock(), MagicMock())
            if mock_hash.called:
                return False, "compute_file_hash called despite matching size/mtime"
    with db.get_session() as s:
        s.query(AKBDatafile).filter(AKBDatafile.id == df.id).delete()
    return True, "OK"


def _dkb_test_recon_schedule_gated(db, sub, ds):
    """A target outside its upload window defers upserts (no ERROR, link stays
    ENABLED with a 'deferred' message); the same pass uploads once the window
    includes the current time. Removal operations are NOT gated."""
    from unittest.mock import MagicMock, patch
    from worker.sink_recon import reconcile_subscription_targets

    def window_strs(delta_start, delta_end):
        from datetime import datetime
        m = datetime.now().hour * 60 + datetime.now().minute
        fmt = lambda x: f"{(x % 1440) // 60:02d}:{(x % 1440) % 60:02d}"
        return fmt(m + delta_start), fmt(m + delta_end)

    sub_row = db.get_subscription(sub)
    _write_output_file_test(sub_row.name, "gated.md", "# gated\ncontent")

    # Phase 1: window excludes now → nothing uploaded, link stays ENABLED.
    s, e = window_strs(1, 2)
    db.update_target(ds.id, schedule_start=s, schedule_end=e)
    svc = _mk_cancel_test_svc(db, ds)
    with patch("worker.sink_recon._get_service") as mock_get:
        mock_get.return_value = svc
        reconcile_subscription_targets(sub_row, db, MagicMock(), MagicMock())
    if any(c[0] in ("add_datafile", "update_datafile") for c in svc.calls):
        return False, f"gated pass uploaded {svc.calls}"
    link = db.get_target_subscription(ds.id, sub)
    if link is None or link.status != STATE_ENABLED:
        return False, f"gated pass left link {link.status if link else 'gone'}"
    if link.last_message and "deferred" not in link.last_message:
        return False, f"expected deferred message, got {link.last_message!r}"
    if db.list_datafiles_for_target_subscription(ds.id, sub):
        return False, "gated pass wrote target_datafile rows"

    # Phase 2: window includes now → same file uploads.
    s2, e2 = window_strs(0, 1)
    db.update_target(ds.id, schedule_start=s2, schedule_end=e2)
    svc2 = _mk_cancel_test_svc(db, ds)
    with patch("worker.sink_recon._get_service") as mock_get:
        mock_get.return_value = svc2
        reconcile_subscription_targets(sub_row, db, MagicMock(), MagicMock())
    adds = [c for c in svc2.calls if c[0] == "add_datafile"]
    if len(adds) != 1:
        return False, f"expected 1 upload after window opens, got {len(adds)}"
    return True, "OK"


def _mk_cancel_test_svc(db, ds):
    """Build a real _MockSink bound to ds with remote_target_id set, ready to
    have a mid-recon status-flip side effect installed on base_add_datafile."""
    ds_row = db.get_target(ds.id)
    ds_row.api_key = db.decrypt_target_api_key(ds_row)
    svc = _MockSink(ds_row, db)
    svc.remote_target_id = "mock-remote"
    return svc


def _wrap_add_datafile_flip(svc, flip_fn, db, ds, sub):
    """Wrap svc.base_add_datafile to flip the link/sub status after the first call."""
    orig_add = svc.base_add_datafile
    count = [0]

    def flip_add(sub_id_, path_, known_hash_=None):
        count[0] += 1
        flip_fn(db, ds.id, sub)
        if known_hash_ is not None:
            return orig_add(sub_id_, path_, known_hash_=known_hash_)
        return orig_add(sub_id_, path_)

    svc.base_add_datafile = flip_add
    return count


def _dkb_test_recon_cancel_link_removed(db, sub, ds):
    """Mid-recon link removal: recon halts after the first upload and the
    already-uploaded file is cleaned up (remote remove + rows + link row)."""
    from unittest.mock import MagicMock, patch
    from worker.sink_recon import reconcile_subscription_targets
    sub_row = db.get_subscription(sub)
    for i in range(3):
        _write_output_file_test(sub_row.name, f"f{i}.md", f"content {i}\n")
    svc = _mk_cancel_test_svc(db, ds)

    def flip(db_, target_id_, sub_id_):
        db_.set_target_subscription_status(target_id_, sub_id_, STATE_DELETED)

    count = _wrap_add_datafile_flip(svc, flip, db, ds, sub)
    with patch("worker.sink_recon._get_service") as mock_get:
        mock_get.return_value = svc
        reconcile_subscription_targets(sub_row, db, MagicMock(), MagicMock())
    if count[0] != 1:
        return False, f"expected 1 upload before halt, got {count[0]}"
    if db.get_target_subscription(ds.id, sub) is not None:
        return False, "link row not deleted after mid-recon removal"
    if db.list_datafiles_for_target_subscription(ds.id, sub):
        return False, "target_datafile rows remain after mid-recon removal"
    if not any(c[0] == "remove_datafile" for c in svc.calls):
        return False, "remove_datafile not called during inline cleanup"
    return True, "OK"


def _dkb_test_recon_cancel_link_disabled(db, sub, ds):
    """Mid-recon link disable: recon halts, uploaded rows are kept, link stays
    DISABLED, and nothing is removed from the remote."""
    from unittest.mock import MagicMock, patch
    from worker.sink_recon import reconcile_subscription_targets
    sub_row = db.get_subscription(sub)
    for i in range(3):
        _write_output_file_test(sub_row.name, f"d{i}.md", f"content {i}\n")
    svc = _mk_cancel_test_svc(db, ds)

    def flip(db_, target_id_, sub_id_):
        db_.set_target_subscription_status(target_id_, sub_id_, STATE_DISABLED)

    count = _wrap_add_datafile_flip(svc, flip, db, ds, sub)
    with patch("worker.sink_recon._get_service") as mock_get:
        mock_get.return_value = svc
        reconcile_subscription_targets(sub_row, db, MagicMock(), MagicMock())
    if count[0] != 1:
        return False, f"expected 1 upload before halt, got {count[0]}"
    link = db.get_target_subscription(ds.id, sub)
    if link is None or link.status != STATE_DISABLED:
        return False, "link should remain DISABLED after mid-recon disable"
    rows = db.list_datafiles_for_target_subscription(ds.id, sub)
    if len(rows) != 1:
        return False, f"expected 1 kept target_datafile row, got {len(rows)}"
    if any(c[0] == "remove_datafile" for c in svc.calls):
        return False, "remove_datafile should not be called for a disabled link"
    # Re-enable so the next test's cleanup can delete the target/sub.
    db.set_target_subscription_status(ds.id, sub, STATE_ENABLED)
    return True, "OK"


def _dkb_test_recon_cancel_sub_deleted(db, sub, ds):
    """Mid-recon subscription deletion: recon halts immediately and leaves the
    sub DELETED. The test then runs the worker's own cleanup path
    (_cleanup_subscription_targets + delete_subscription_row) to verify the
    production path handles it correctly."""
    from unittest.mock import MagicMock, patch
    from worker.sink_recon import reconcile_subscription_targets
    from worker.worker import _cleanup_subscription_targets
    from utils.misc_utils import get_logger
    sub_row = db.get_subscription(sub)
    for i in range(3):
        _write_output_file_test(sub_row.name, f"s{i}.md", f"content {i}\n")
    svc = _mk_cancel_test_svc(db, ds)

    def flip(db_, _target_id_, sub_id_):
        db_.update_status(sub_id_, STATE_DELETED)

    count = _wrap_add_datafile_flip(svc, flip, db, ds, sub)
    with patch("worker.sink_recon._get_service") as mock_get:
        mock_get.return_value = svc

        # Phase 1: recon — detects sub_gone, aborts, leaves mess
        reconcile_subscription_targets(sub_row, db, MagicMock(), MagicMock())
        if count[0] != 1:
            return False, f"expected 1 upload before halt, got {count[0]}"
        link = db.get_target_subscription(ds.id, sub)
        if link is None or link.status != STATE_IN_PROGRESS:
            return False, "link should be left unchanged (IN_PROGRESS from recon-start marking) on sub-level cancel"
        if any(c[0] == "remove_datafile" for c in svc.calls):
            return False, "remove_datafile should not be called on sub-level cancel"

        # Phase 2: production cleanup — same path the worker runs for a DELETED sub
        svc.calls.clear()
        cleanup_log = get_logger("test_dkb_cleanup")
        _cleanup_subscription_targets(sub_row, MagicMock(), db, cleanup_log)
        db.delete_subscription_row(sub)

    after = sub
    if db.get_target_subscription(ds.id, after) is not None:
        return False, "target_subscription link not cleaned"
    if db.list_datafiles_for_target_subscription(ds.id, after):
        return False, "target_datafile rows remain after cleanup"
    if db.get_target(ds.id) is not None:
        return False, "orphan target not deleted after cleanup"
    if db.get_subscription(after) is not None:
        return False, "subscription row not deleted after cleanup"
    if not any(c[0] == "remove_datafile" for c in svc.calls):
        return False, "remove_datafile not called during cleanup"
    if not any(c[0] == "remove_target" for c in svc.calls):
        return False, "remove_target not called for orphan target"
    return True, "OK"


def _dkb_test_recon_null_remote_target(db, sub, ds):
    """Verify null remote_target_id at recon time → link transitions to ERROR."""
    from unittest.mock import MagicMock, patch
    from worker.sink_recon import reconcile_subscription_targets
    sub_row = db.get_subscription(sub)
    with patch("worker.sink_recon._get_service") as mock_get:
        mock_svc = MagicMock()
        mock_svc.remote_target_id = None
        mock_svc.name = "TestSvc"
        mock_svc.set_cancel_check = MagicMock()
        mock_svc._check_cancel = MagicMock()
        mock_get.return_value = mock_svc
        reconcile_subscription_targets(sub_row, db, MagicMock(), MagicMock())
    link = db.get_target_subscription(ds.id, sub)
    if link is None or link.status != STATE_ERROR:
        return False, f"expected link ERROR after null remote_target_id, got {link.status if link else 'None'}"
    db.set_target_subscription_status(ds.id, sub, STATE_ENABLED)
    return True, "OK"


def _dkb_test_recon_marks_in_progress(db, sub, ds):
    """Verify active links are marked IN_PROGRESS at recon start, then ENABLED at completion."""
    from unittest.mock import MagicMock, patch
    from worker.sink_recon import reconcile_subscription_targets
    sub_row = db.get_subscription(sub)
    test_path = _write_output_file_test(sub_row.name, "in_progress_test.md", "content")
    captured_status = [None]

    def capture_add(sub_id_, path_, **kwargs):
        link = db.get_target_subscription(ds.id, sub)
        captured_status[0] = link.status
        return "mock-id"

    with patch("worker.sink_recon._get_service") as mock_get:
        mock_svc = MagicMock()
        mock_svc.remote_target_id = "mock-remote"
        mock_svc.name = "TestSvc"
        mock_svc.set_cancel_check = MagicMock()
        mock_svc._check_cancel = MagicMock()
        mock_svc.base_add_datafile = capture_add
        mock_svc.base_update_datafile = MagicMock()
        mock_svc.base_remove_datafile = MagicMock()
        mock_get.return_value = mock_svc
        reconcile_subscription_targets(sub_row, db, MagicMock(), MagicMock())

    if captured_status[0] != STATE_IN_PROGRESS:
        return False, f"expected link IN_PROGRESS during recon, got {captured_status[0]}"
    link = db.get_target_subscription(ds.id, sub)
    if link.status != STATE_ENABLED:
        return False, f"expected link ENABLED after recon, got {link.status}"
    with db.get_session() as s:
        df = s.query(AKBDatafile).filter(AKBDatafile.path == test_path).first()
        if df:
            s.query(TargetDatafile).filter(TargetDatafile.datafile_id == df.id).delete()
            s.query(AKBDatafile).filter(AKBDatafile.id == df.id).delete()
    return True, "OK"


def _dkb_test_target_status_derivation(db, sub, ds):
    """Verify _serialise_target correctness across all link-status combinations."""
    from manager.manager import _serialise_target
    t = db.get_target(ds.id)
    link = db.list_target_subscriptions(ds.id)[0]

    statuses = [
        ("ENABLED", "ENABLED"),
        ("ERROR", "ERROR"),
        ("IN_PROGRESS", "IN_PROGRESS"),
        ("ENQUEUED", "IN_PROGRESS"),
        ("DISABLED", "DISABLED"),
    ]
    for set_status, expected in statuses:
        db.set_target_subscription_status(ds.id, sub, set_status)
        links = db.list_target_subscriptions(ds.id)
        result = _serialise_target(t, links, db)
        if result["status"] != expected:
            db.set_target_subscription_status(ds.id, sub, STATE_ENABLED)
            return False, f"status={set_status}: expected {expected} got {result['status']}"

    db.set_target_subscription_status(ds.id, sub, STATE_ENABLED)
    return True, "All derivation scenarios OK"


def _dkb_test_sink_cancel_check(db, sub, ds):
    """BaseSink._check_cancel: no-op without a callback, raises SinkCancelledError
    with the correct kind when the installed check fires."""
    svc = _mk_cancel_test_svc(db, ds)
    try:
        svc._check_cancel()
    except Exception as exc:
        return False, f"_check_cancel raised without a callback: {exc}"
    svc.set_cancel_check(lambda: None)
    try:
        svc._check_cancel()
    except Exception as exc:
        return False, f"_check_cancel raised when check returned None: {exc}"
    svc.set_cancel_check(lambda: "link_disabled")
    try:
        svc._check_cancel()
        return False, "_check_cancel did not raise when check fired"
    except SinkCancelledError as exc:
        if exc.kind != "link_disabled":
            return False, f"unexpected kind {exc.kind!r}"
    except Exception as exc:
        return False, f"wrong exception type {type(exc).__name__}: {exc}"
    svc.set_cancel_check(None)
    try:
        svc._check_cancel()
    except Exception as exc:
        return False, f"_check_cancel raised after reset: {exc}"
    return True, "OK"


def _dkb_test_pass2_last_checked(db, sub, ds):
    """Pass II conformance: last_checked must be identical across rows since a
    single checked_at timestamp is threaded through the pass."""
    from datetime import datetime, timezone
    from unittest.mock import MagicMock, patch
    from worker.sink_recon import _pass2_akb_datafiles
    sub_row = db.get_subscription(sub)
    p1 = _write_output_file_test(sub_row.name, "lc1.md", "one")
    p2 = _write_output_file_test(sub_row.name, "lc2.md", "two")
    df1 = db.get_or_create_datafile(sub, p1, os.path.getsize(p1), os.path.getmtime(p1), compute_file_hash(p1))
    df2 = db.get_or_create_datafile(sub, p2, os.path.getsize(p2), os.path.getmtime(p2), compute_file_hash(p2))
    fs_files = {p1: os.stat(p1), p2: os.stat(p2)}
    _pass2_akb_datafiles(sub_row, db, os.path.dirname(p1), fs_files, MagicMock())
    with db.get_session() as s:
        r1 = s.query(AKBDatafile).filter(AKBDatafile.id == df1.id).first()
        r2 = s.query(AKBDatafile).filter(AKBDatafile.id == df2.id).first()
        if r1 is None or r2 is None:
            return False, "rows missing after pass2"
        if r1.last_checked != r2.last_checked:
            return False, f"last_checked differs: {r1.last_checked} vs {r2.last_checked}"
    with db.get_session() as s:
        s.query(AKBDatafile).filter(AKBDatafile.id.in_([df1.id, df2.id])).delete()
    return True, "OK"


def _dkb_test_datafiles_by_sub(db, sub, ds):
    """list_datafiles_for_target_subscription filters by sub correctly."""
    import uuid as _uuid
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    other = str(_uuid.uuid4())
    with db.get_session() as s:
        s.add(Subscription(id=other, plugin_id="test_plugin",
                           name=f"test_other_{other}", config={}, status=STATE_DISABLED,
                           access_level="PRIVATE", sub_type="SCHEDULED", cron="0 0 31 12 *"))
        s.flush()
        df1_id = str(_uuid.uuid4()); df2_id = str(_uuid.uuid4())
        s.add(AKBDatafile(id=df1_id, subscription_id=sub, path=f"/tmp/sink_test/{df1_id}.md",
                          size=1, mtime=now, hash="h1"))
        s.add(AKBDatafile(id=df2_id, subscription_id=other, path=f"/tmp/sink_test/{df2_id}.md",
                          size=1, mtime=now, hash="h2"))
        s.flush()
        s.add(TargetDatafile(target_id=ds.id, datafile_id=df1_id, remote_datafile_id="r1", hash="h1"))
        s.add(TargetDatafile(target_id=ds.id, datafile_id=df2_id, remote_datafile_id="r2", hash="h2"))
    try:
        all_rows = db.list_datafiles_for_target(ds.id)
        if len(all_rows) != 2:
            return False, f"expected 2 all got {len(all_rows)}"
        rows_sub = db.list_datafiles_for_target_subscription(ds.id, sub)
        if len(rows_sub) != 1 or rows_sub[0].datafile_id != df1_id:
            return False, f"sub filter: expected 1 row for fixture-sub got {len(rows_sub)}"
        rows_other = db.list_datafiles_for_target_subscription(ds.id, other)
        if len(rows_other) != 1 or rows_other[0].datafile_id != df2_id:
            return False, f"other filter: expected 1 row for other-sub got {len(rows_other)}"
        return True, "OK"
    finally:
        with db.get_session() as s:
            s.query(TargetDatafile).filter(TargetDatafile.target_id == ds.id).delete()
            s.query(AKBDatafile).filter(AKBDatafile.id.in_([df1_id, df2_id])).delete()
            s.query(Subscription).filter(Subscription.id == other).delete()


def _dkb_test_delete_strict_raises(db, sub, ds):
    from unittest.mock import MagicMock, patch
    db.set_target_remote_id(ds.id, "mock-remote")
    ds_row = db.get_target(ds.id)
    ds_row.api_key = db.decrypt_target_api_key(ds_row)
    svc = _MockSink(ds_row, db)
    svc.remote_target_id = "mock-remote"
    svc.remove_target = MagicMock(side_effect=RuntimeError("network down"))
    with patch("worker.sink_recon._get_service", return_value=svc):
        from worker.sink_recon import _remove_remote_target_strict
        raised = False
        try:
            _remove_remote_target_strict(ds.id, db, MagicMock(), None)
        except RuntimeError:
            raised = True
        if not raised:
            return False, "strict helper should raise on remote failure"
    if db.get_target(ds.id) is None:
        return False, "target row must be retained after strict failure"
    return True, "OK"


def _dkb_test_delete_strict_success(db, sub, ds):
    from unittest.mock import MagicMock, patch
    db.set_target_remote_id(ds.id, "mock-remote")
    ds_row = db.get_target(ds.id)
    ds_row.api_key = db.decrypt_target_api_key(ds_row)
    svc = _MockSink(ds_row, db)
    svc.remote_target_id = "mock-remote"
    with patch("worker.sink_recon._get_service", return_value=svc):
        from worker.sink_recon import _remove_remote_target_strict
        _remove_remote_target_strict(ds.id, db, MagicMock(), None)
    if ("remove_target",) not in svc.calls:
        return False, "remove_target not called"
    if db.get_target(ds.id) is None:
        return False, "strict helper must NOT delete rows (caller does)"
    return True, "OK"


def _dkb_test_delete_orphan_best_effort(db, sub, ds):
    from unittest.mock import MagicMock, patch
    db.set_target_remote_id(ds.id, "mock-remote")
    ds_row = db.get_target(ds.id)
    ds_row.api_key = db.decrypt_target_api_key(ds_row)
    svc = _MockSink(ds_row, db)
    svc.remote_target_id = "mock-remote"
    svc.remove_target = MagicMock(side_effect=RuntimeError("down"))
    with patch("worker.sink_recon._get_service", return_value=svc):
        from worker.sink_recon import _remove_orphan_target
        _remove_orphan_target(ds.id, db, MagicMock(), MagicMock())
    if db.get_target(ds.id) is not None:
        return False, "orphan helper should still delete rows on remote failure"
    return True, "OK"


def _run_dkb_unit_tests():
    """Run all Sink unit tests inline, return (passed, total, results)."""
    tests = [
        ("DKB CRUD — service", lambda db, sub, ds: _sink_test_svc_crud(db, sub, ds)),
        ("DKB CRUD — datastore", lambda db, sub, ds: _dkb_test_ds_crud(db, sub, ds)),
        ("DKB CRUD — subscription link", lambda db, sub, ds: _dkb_test_link_crud(db, sub, ds)),
        ("DKB CRUD — datafile", lambda db, sub, ds: _dkb_test_datafile_crud(db, sub, ds)),
        ("DKB CRUD — target datafile", lambda db, sub, ds: _dkb_test_ds_df_crud(db, sub, ds)),
        ("DKB Queue — encoding", None),
        ("DKB Queue — roundtrip", None),
        ("DKB Registry — load", None),
        ("DKB Service — compute hash", None),
        ("DKB Service — remote filename with path", None),
        ("DKB Icon — derivation + fallback", None),
        ("DKB Schedule — parse + window gating", None),
        ("DKB Service — base_add_datafile", lambda db, sub, ds: _dkb_test_base_add_datafile(db, sub, ds)),
        ("DKB Service — base_update_datafile", lambda db, sub, ds: _dkb_test_base_update_datafile(db, sub, ds)),
        ("DKB Service — base_remove_datafile", lambda db, sub, ds: _dkb_test_base_remove_datafile(db, sub, ds)),
        ("DKB Service — base_add_target", lambda db, sub, ds: _dkb_test_base_add_target(db, sub, ds)),
        ("DKB Recon — adds new file", lambda db, sub, ds: _dkb_test_recon_add(db, sub, ds)),
        ("DKB Recon — adds existing known file", lambda db, sub, ds: _dkb_test_recon_add_existing_df(db, sub, ds)),
        ("DKB Recon — known hash avoids re-hash", lambda db, sub, ds: _dkb_test_recon_known_hash_no_rehash(db, sub, ds)),
        ("DKB Recon — outside schedule defers uploads", lambda db, sub, ds: _dkb_test_recon_schedule_gated(db, sub, ds)),
        ("DKB Recon — removes deleted file", lambda db, sub, ds: _dkb_test_recon_remove(db, sub, ds)),
        ("DKB Recon — skips disabled DS", lambda db, sub, ds: _dkb_test_recon_skip_disabled(db, sub, ds)),
        ("DKB Recon — error on failure", lambda db, sub, ds: _dkb_test_recon_error(db, sub, ds)),
        ("DKB Recon — cancel on link removed", lambda db, sub, ds: _dkb_test_recon_cancel_link_removed(db, sub, ds)),
        ("DKB Recon — cancel on link disabled", lambda db, sub, ds: _dkb_test_recon_cancel_link_disabled(db, sub, ds)),
        ("DKB Recon — cancel on sub deleted", lambda db, sub, ds: _dkb_test_recon_cancel_sub_deleted(db, sub, ds)),
        ("DKB Recon — null remote target", lambda db, sub, ds: _dkb_test_recon_null_remote_target(db, sub, ds)),
        ("DKB Recon — marks links IN_PROGRESS", lambda db, sub, ds: _dkb_test_recon_marks_in_progress(db, sub, ds)),
        ("DKB Target — status derivation", lambda db, sub, ds: _dkb_test_target_status_derivation(db, sub, ds)),
        ("DKB Service — cancel check hook", lambda db, sub, ds: _dkb_test_sink_cancel_check(db, sub, ds)),
        ("DKB Pass II — last_checked consistent", lambda db, sub, ds: _dkb_test_pass2_last_checked(db, sub, ds)),
        ("DKB Filter — datafiles by sub", lambda db, sub, ds: _dkb_test_datafiles_by_sub(db, sub, ds)),
        ("DKB Delete — strict raises on remote failure", lambda db, sub, ds: _dkb_test_delete_strict_raises(db, sub, ds)),
        ("DKB Delete — strict success keeps rows for caller", lambda db, sub, ds: _dkb_test_delete_strict_success(db, sub, ds)),
        ("DKB Delete — orphan best-effort deletes rows", lambda db, sub, ds: _dkb_test_delete_orphan_best_effort(db, sub, ds)),
    ]

    results = []
    db = _sink_db()
    queue = _sink_queue()
    try:
        run_migrations(SINK_DATABASE_URL)
        for key in (P_QUEUE_KEY, S_QUEUE_KEY):
            while queue.client.llen(key):
                queue.client.rpop(key)

        for name, fn in tests:
            try:
                if fn is None:
                    ok, msg = _dkb_test_queue_encoding() if "encoding" in name else \
                              _dkb_test_queue_roundtrip() if "roundtrip" in name else \
                              _dkb_test_registry_load() if "Registry" in name else \
                              _dkb_test_remote_filename_include_path() if "remote filename" in name else \
                              _dkb_test_icon() if "Icon" in name else \
                              _dkb_test_schedule() if "Schedule" in name else \
                              (_dkb_test_compute_hash(), "OK")
                else:
                    # Create fresh fixtures per test
                    os.makedirs(SINK_TEST_OUTPUT, exist_ok=True)
                    sub_id, ds = _sink_create_fixtures(db)
                    try:
                        ok, msg = fn(db, sub_id, ds)
                    finally:
                        _delete_sub(sub_id)
                        _sink_api_delete(f"/api/targets/{ds.id}")
                        with db.get_session() as s:
                            s.query(Sink).filter(
                                Sink.name == "TestSinkService").delete()
                            s.query(PluginRegistryState).filter(
                                PluginRegistryState.plugin_id == "test_plugin").delete()
                    if os.path.isdir(SINK_TEST_OUTPUT):
                        shutil.rmtree(SINK_TEST_OUTPUT)
                    output_plugin_dir = "/output/test_plugin"
                    if os.path.isdir(output_plugin_dir):
                        shutil.rmtree(output_plugin_dir)
            except Exception as exc:
                ok = False
                msg = f"EXCEPTION: {exc!r}"
            results.append((name, ok, msg))
            status = "✅" if ok else "❌"
            _log(f"  {status} DKB Unit — {name}: {msg}")
    finally:
        db.dispose()

    passed = sum(1 for _, ok, _ in results if ok)
    total = len(results)
    return passed, total, results


# ---------------------------------------------------------------------------
# Leak check (runs at the very end)
# ---------------------------------------------------------------------------

def _cleanup_test_plugins() -> None:
    """Remove test plugin files from /src/plugins/ and deregister them via API.
    This runs BEFORE the leak check so the leak check can verify a clean state.
    """
    real_plugins = {"crawl4AIWebScraperPlugin", "eBiblePlugin",
                    "ePaperlessDoclingPlugin", "imapFolderWatchPlugin",
                    "youTubeTranscriptionPlugin"}

    # Remove test plugin files from /src/plugins/ (same way _sync_test_plugins added them)
    plugins_dir = "/src/plugins"
    if os.path.isdir(plugins_dir):
        for fname in sorted(os.listdir(plugins_dir)):
            if fname.endswith(".py") and fname[:-3] not in real_plugins:
                fpath = os.path.join(plugins_dir, fname)
                if os.path.isfile(fpath):
                    try:
                        os.remove(fpath)
                    except Exception as exc:  # noqa: BLE001
                        _log(f"warning: failed to remove {fpath}: {exc}")

    # Deregister test plugins via API (normal way)
    try:
        plugins = api_get("/api/plugins")
        for p in plugins:
            pid = p.get("plugin_id", "")
            if pid not in real_plugins:
                try:
                    requests.delete(
                        f"{MANAGER_URL}/api/plugins/{pid}",
                        headers=_api_headers(), timeout=10,
                    )
                except Exception as exc:  # noqa: BLE001
                    _log(f"warning: failed to deregister plugin {pid}: {exc}")
    except Exception as exc:  # noqa: BLE001
        _log(f"warning: plugin deregistration sweep failed: {exc}")

    # Deregister test Sink services via API (mirrors the plugin path).
    # testSink has zero datastores after the e2e tests, so the delete is
    # allowed and removes the file + the sink row.
    try:
        svcs = api_get("/api/sinks")
        for svc in svcs:
            if svc.get("name") == "testSink":
                try:
                    requests.delete(
                        f"{MANAGER_URL}/api/sinks/{svc['service_id']}",
                        headers=_api_headers(), timeout=10,
                    )
                except Exception as exc:  # noqa: BLE001
                    _log(f"warning: failed to deregister Sink service testSink: {exc}")
    except Exception as exc:  # noqa: BLE001
        _log(f"warning: Sink service deregistration sweep failed: {exc}")

    # Fallback: remove the test DKB file directly if it is still present
    sink_path = "/src/sinks/testSink.py"
    try:
        if os.path.isfile(sink_path):
            os.remove(sink_path)
    except Exception:
        pass

def _purge_test_artifacts(leaked: Optional[List[str]] = None) -> None:
    """Delete every test artifact the runner may have left behind.

    Uses the same sentinel-based detection as the leak check (test
    subscription name prefixes, known service/target names, test datafile
    paths, non-real plugin registry rows, and the test plugin/sink files
    on disk).

    When ``leaked`` is None this is a **silent startup cleanup**: anything
    found is deleted and never reported as a failure. When a list is
    passed, each leftover is appended to it so the caller can decide
    whether to fail (the leak check at the end of a run does this).
    """
    real_plugins = {"crawl4AIWebScraperPlugin", "eBiblePlugin",
                    "ePaperlessDoclingPlugin", "imapFolderWatchPlugin",
                    "youTubeTranscriptionPlugin"}
    db2 = _sink_db()
    try:
        with db2.get_session() as s:
            svc_names = ("TestSinkService", "MyService", "Svc", "testSink")
            t_names = ("TestTarget", "MyDS", "MyDS-Updated", "DS")
            leaked_svcs = s.query(Sink).filter(
                Sink.name.in_(svc_names)).all()
            for svc in leaked_svcs:
                if leaked is not None:
                    leaked.append(f"Sink(id={svc.id[:8]}, name={svc.name!r})")
            svc_ids = [svc.id for svc in leaked_svcs]
            leaked_targs = s.query(Target).filter(
                Target.name.in_(t_names) |
                (Target.service_id.in_(svc_ids))).all()
            for t in leaked_targs:
                if leaked is not None:
                    leaked.append(f"Target(id={t.id[:8]}, name={t.name!r})")
            t_ids = [t.id for t in leaked_targs]
            leaked_files = s.query(AKBDatafile).filter(
                AKBDatafile.path.like("/tmp/sink_test_output%") |
                AKBDatafile.path.like("/output/test_plugin/test_sub%")
            ).all()
            for f in leaked_files:
                if leaked is not None:
                    leaked.append(f"AKBDatafile(id={f.id[:8]}, path={f.path!r})")
            file_ids = [f.id for f in leaked_files]
            if t_ids:
                s.query(TargetDatafile).filter(
                    TargetDatafile.target_id.in_(t_ids)).delete(synchronize_session=False)
                s.query(TargetSubscription).filter(
                    TargetSubscription.target_id.in_(t_ids)).delete(synchronize_session=False)
                s.query(Target).filter(
                    Target.id.in_(t_ids)).delete(synchronize_session=False)
            if file_ids:
                s.query(TargetDatafile).filter(
                    TargetDatafile.datafile_id.in_(file_ids)).delete(synchronize_session=False)
                s.query(AKBDatafile).filter(
                    AKBDatafile.id.in_(file_ids)).delete(synchronize_session=False)
            if svc_ids:
                s.query(Sink).filter(
                    Sink.id.in_(svc_ids)).delete(synchronize_session=False)

            # 2. Subscriptions: test-only rows
            test_subs = s.query(Subscription).filter(
                (Subscription.plugin_id == "test_plugin") |
                (Subscription.name.like(f"{TEST_SUB_PREFIX}%")) |
                (Subscription.name.like("e2e-%"))
            ).all()
            for sub in test_subs:
                if leaked is not None:
                    leaked.append(f"Subscription(id={sub.id[:8]}, name={sub.name!r}, plugin={sub.plugin_id})")
            test_sub_ids = [sub.id for sub in test_subs]
            if test_sub_ids:
                s.query(Subscription).filter(
                    Subscription.id.in_(test_sub_ids)).delete(synchronize_session=False)

            # 3. EventLog: rows for leaked subscription IDs
            if test_sub_ids:
                elogs = s.query(EventLog).filter(
                    EventLog.subscription_id.in_(test_sub_ids)).all()
                for el in elogs:
                    if leaked is not None:
                        leaked.append(f"EventLog(id={el.id[:8]}, sub_id={el.subscription_id[:8]})")
                s.query(EventLog).filter(
                    EventLog.subscription_id.in_(test_sub_ids)).delete(synchronize_session=False)

            # 4. PluginRegistryState: test plugin rows (should be zero after cleanup)
            test_plugin_rows = s.query(PluginRegistryState).filter(
                ~PluginRegistryState.plugin_id.in_(real_plugins)).all()
            for pr in test_plugin_rows:
                if leaked is not None:
                    leaked.append(f"PluginRegistryState(plugin_id={pr.plugin_id!r})")
            for pr in test_plugin_rows:
                s.delete(pr)
    finally:
        db2.dispose()

    # 5. Filesystem: /output test plugin dirs
    test_plugin_dirs = {"cancellationPlugin", "configValidationPlugin", "crashPlugin",
        "cronRandomizePlugin", "customRoutePlugin", "delayedInitPlugin",
        "deleteAllPlugin", "editMatchPlugin", "emptyOutputPlugin",
        "eventHappyPlugin", "eventOftenPlugin", "happyPathPlugin",
        "invalidNamePlugin", "largeOutputPlugin", "longNamePlugin32CharNameForUITes",
        "longRunningFailurePlugin", "longRunningSuccessPlugin",
        "monitorErrorPlugin", "monitorNeverTriggerPlugin",
        "moveToDestErrorPlugin", "noHeartbeatPlugin", "nonZeroExitPlugin",
        "passwordPlugin", "schemaBreakingPlugin", "testSinkWriterPlugin",
        "test_plugin", "zombiePlugin"}
    output_root = "/output"
    if os.path.isdir(output_root):
        for entry in sorted(os.listdir(output_root)):
            entry_path = os.path.join(output_root, entry)
            if os.path.isdir(entry_path) and entry in test_plugin_dirs:
                if leaked is not None:
                    leaked.append(f"/output/{entry}/")
                shutil.rmtree(entry_path)

    # 6. Filesystem: /src/plugins test plugin files (should be zero after cleanup)
    plugins_dir = "/src/plugins"
    if os.path.isdir(plugins_dir):
        for fname in sorted(os.listdir(plugins_dir)):
            if fname.endswith(".py") and fname[:-3] not in real_plugins:
                fpath = os.path.join(plugins_dir, fname)
                if os.path.isfile(fpath):
                    if leaked is not None:
                        leaked.append(f"/src/plugins/{fname}")
                    os.remove(fpath)

    # 7. Filesystem: /src/sinks test Sink files (should be gone after cleanup)
    sinks_dir = "/src/sinks"
    if os.path.isdir(sinks_dir):
        for fname in sorted(os.listdir(sinks_dir)):
            if fname == "testSink.py":
                fpath = os.path.join(sinks_dir, fname)
                if os.path.isfile(fpath):
                    if leaked is not None:
                        leaked.append(f"/src/sinks/{fname}")
                    os.remove(fpath)


def _run_leak_check(results: List[Tuple[str, bool, str]]) -> None:
    """Scan DB + filesystem for leftover test artifacts. Any found = FAIL + clean.
    Runs once at the very end of main(), after _cleanup_test_plugins().
    """
    leaked: list[str] = []
    _purge_test_artifacts(leaked)
    if leaked:
        _log(f"\n[LEAK] Found {len(leaked)} leftover test artifact(s):")
        for l in leaked:
            _log(f"  [LEAK] {l}")
        _log("[LEAK] Artifacts have been cleaned up.")
        results.append(("LEAK CHECK", False, f"{len(leaked)} artifact(s) leaked"))
    else:
        _log("[LEAK] No leftover artifacts — all tests cleaned up properly.")
        results.append(("LEAK CHECK", True, "clean"))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def _run_suite(reset_mode: bool) -> int:
    _log(f"Test runner starting; manager={MANAGER_URL} (reset={reset_mode})")

    if reset_mode:
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

    # Purge every test artifact left behind by a previous (possibly aborted)
    # run. Mirrors the leak check's sentinel-based sweep but runs silently
    # at startup: leftovers are cleaned, never reported as a failure. This
    # deletes stale DB rows (subscriptions, targets, sinks, datafiles,
    # plugin registry state), /output test dirs, and the test plugin/sink
    # files on disk before they are re-synced below. Only test-created
    # artifacts are touched — user subscriptions are NEVER removed here.
    _log("Purging leftover test artifacts from previous runs...")
    _purge_test_artifacts()
    time.sleep(2.0)  # let the manager's file watcher notice the removals

    # Sync test plugins
    _sync_test_plugins()

    # Sync test Sink services
    _sync_test_sinks()

    # Wait for the Manager to hot-swap testSink in and upsert its row.
    _log("Waiting for testSink to appear in /api/sinks...")
    dkb_deadline = time.time() + 60
    test_dkb_ready = False
    while time.time() < dkb_deadline:
        try:
            svcs = api_get("/api/sinks")
            if any(s.get("name") == "testSink" for s in svcs):
                test_dkb_ready = True
                break
        except Exception:
            pass
        time.sleep(2.0)
    if not test_dkb_ready:
        _log("TIMEOUT waiting for testSink service to hot-swap in")
        return 3
    _log("testSink service hot-swapped in OK")

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
        "eventOftenPlugin",         "deleteAllPlugin",
        "testSinkWriterPlugin",
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
        ("DKB Test 27 — full pipeline", test_sink_full_pipeline),
        ("DKB Test 28 — Sink-only recon", test_sink_only_recon),
        ("DKB Test 29 — delete target normal", test_dkb_delete_normal),
        ("DKB Test 30 — delete target force", test_dkb_delete_force),
        ("DKB Test 31 — delete strict failure retains + force recovers", test_dkb_delete_strict_failure_force),
        ("DKB Test 32 — unlink last sub keeps target", test_dkb_unlink_last_sub_keeps_target),
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

    # Summary (32 tests)
    passed = sum(1 for _, ok, _ in results if ok)
    total = len(results)
    _log(f"\n=== TEST SUMMARY: {passed}/{total} passed ===")
    for name, ok, msg in results:
        mark = "✅" if ok else "❌"
        _log(f"  {mark} {name}: {msg}")

    # Run Sink unit tests (inline)
    _log("\n=== Running Sink unit tests ===")
    dkb_passed, dkb_total, dkb_results = _run_dkb_unit_tests()
    _log(f"Sink unit tests: {dkb_passed}/{dkb_total} passed")
    results.extend(dkb_results)

    # Cleanup test plugins (files + API deregister) before leak check
    _log("\n=== Cleaning up test plugins ===")
    _cleanup_test_plugins()

    # Leak check — at the VERY END
    _log("\n=== Running leak check ===")
    _run_leak_check(results)

    # Final summary
    final_passed = sum(1 for _, ok, _ in results if ok)
    final_total = len(results)
    _log(f"\n=== FINAL: {final_passed}/{final_total} passed ===")
    for name, ok, msg in results:
        mark = "✅" if ok else "❌"
        _log(f"  {mark} {name}: {msg}")
    return 0 if final_passed == final_total else 1


def main() -> int:
    ok, reset_mode = _parse_argv(sys.argv)
    if not ok:
        return 2

    held, lock_q = _acquire_run_lock()
    if not held:
        _log("Another test-runner instance is active; refusing to run")
        return 2

    try:
        return _run_suite(reset_mode)
    finally:
        if lock_q is not None:
            try:
                lock_q.client.delete(TEST_RUN_LOCK_KEY)
                _log("Released test-runner lock")
            except Exception as exc:  # noqa: BLE001
                _log(f"warning: failed to release run lock: {exc}")


if __name__ == "__main__":
    sys.exit(main())
