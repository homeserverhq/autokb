"""End-to-end tests for the DKB (Downstream Knowledge Base) system.

Exercises the full pipeline: API → enqueue → worker (upstream + downstream)
→ verify.  Uses the ``testDKBWriterPlugin`` and ``testDKB`` service drop-ins
that are baked into the image.

Usage::

    python /src/testing/test_dkb_e2e.py confirm

The ``confirm`` argument is a safety check (same as ``test_runner.py``).
"""

import json
import os
import sys
import time
import traceback
from typing import Any, Dict, List, Optional

import requests

_SRC = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from utils.misc_utils import uuid7

API_BASE = "http://autokb-manager:80/api"
BACKEND_KEY = os.environ.get("AUTOKB_BACKEND_API_KEY", "sZdx8RLMFOBBnVyINfjvlQXrSHMg0Wwy")
HEADERS = {"Content-Type": "application/json", "X-Api-Key": BACKEND_KEY}

CALLS_FILE = "/output/.dkb_e2e_calls.json"
POLL_INTERVAL = 2
MAX_WAIT = 120
PLUGIN_ID = "testDKBWriterPlugin"
DKB_SERVICE_NAME = "testDKB"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _api(method: str, path: str, body: Any = None) -> Any:
    url = f"{API_BASE}{path}"
    kwargs: Dict[str, Any] = {"headers": HEADERS, "timeout": 30}
    if body is not None:
        kwargs["json"] = body
    resp = requests.request(method, url, **kwargs)
    if resp.status_code >= 400:
        raise RuntimeError(f"API {method} {path}: {resp.status_code} {resp.text[:200]}")
    if resp.status_code == 204:
        return None
    return resp.json()


def _reset_calls():
    if os.path.isfile(CALLS_FILE):
        os.remove(CALLS_FILE)


def _read_calls() -> List[List]:
    """Return all recorded calls from the test DKB service, oldest first."""
    if not os.path.isfile(CALLS_FILE):
        return []
    with open(CALLS_FILE) as f:
        return [json.loads(line) for line in f if line.strip()]


def _write_output_file(sub_name: str, name: str, content: str) -> str:
    path = os.path.join("/output", PLUGIN_ID, sub_name, name)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write(content)
    return path


def _rm_output_dir(sub_name: str):
    d = os.path.join("/output", PLUGIN_ID, sub_name)
    if os.path.isdir(d):
        import shutil
        shutil.rmtree(d)


# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------

class DKBE2EFixture:
    """Create and tear down a subscription + datastore for one test."""

    def __init__(self, test_name: str):
        uid = str(uuid7())
        self.sub_name = f"e2e-{test_name}-{uid[:12]}"
        self.ds_name = f"e2e-{test_name}-ds-{uid[:12]}"
        self.sub_id: Optional[str] = None
        self.ds_id: Optional[str] = None
        self.service_id: Optional[str] = None

    def setup(self):
        # Find the testDKB service
        services = _api("GET", "/dkb_services")
        for svc in services:
            if svc["name"] == DKB_SERVICE_NAME:
                self.service_id = svc["service_id"]
                break
        if not self.service_id:
            raise RuntimeError(f"DKB service {DKB_SERVICE_NAME!r} not found in API")

        # Create subscription
        sub_payload = {
            "name": self.sub_name,
            "config": {},
            "cron": "0 0 * * *",
            "access_level": "PRIVATE",
        }
        sub_resp = _api("POST", f"/subscriptions/{PLUGIN_ID}", sub_payload)
        self.sub_id = sub_resp["id"]

        # Create datastore linked to the sub
        ds_payload = {
            "name": self.ds_name,
            "api_url": "http://fake-dkb/api",
            "api_key": "test-key",
            "ds_extra_params": {},
            "subscription_ids": [self.sub_id],
        }
        ds_resp = _api("POST", f"/dkb_services/{self.service_id}/datastores", ds_payload)
        self.ds_id = ds_resp["datastore_id"]

    def teardown(self):
        """Delete datastore first, then subscription (FK-safe order)."""
        if self.ds_id:
            try:
                _api("DELETE", f"/dkb_datastores/{self.ds_id}")
            except Exception:
                pass
            self.ds_id = None
        if self.sub_id:
            try:
                _api("DELETE", f"/subscriptions/{self.sub_id}")
            except Exception:
                pass
            self.sub_id = None
        _rm_output_dir(self.sub_name)

    @property
    def output_dir(self) -> str:
        return os.path.join("/output", PLUGIN_ID, self.sub_name)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_full_pipeline() -> List[str]:
    """FULL trigger: upstream runs, then downstream recon runs."""
    errors: List[str] = []
    fix = DKBE2EFixture("full")
    fix.setup()
    try:
        _reset_calls()

        # Place an extra file manually (to test it gets picked up by recon)
        _write_output_file(fix.sub_name, "manual.md", "Manually placed file.\n")

        # Trigger FULL
        _api("POST", f"/subscriptions/{fix.sub_id}/trigger")

        # Wait for datastore_subscription → ENABLED
        deadline = time.time() + MAX_WAIT
        last_status = None
        while time.time() < deadline:
            ds_detail = _api("GET", f"/dkb_datastores/{fix.ds_id}")
            subs_detail = ds_detail.get("subscriptions", [])
            if subs_detail:
                last_status = subs_detail[0].get("status", "")
                if last_status == "ENABLED":
                    break
            time.sleep(POLL_INTERVAL)
        else:
            errors.append(
                f"Full pipeline: ds_sub status did not reach ENABLED "
                f"(last={last_status}) within {MAX_WAIT}s"
            )
            return errors

        # --- Verify DKB service calls ---
        calls = _read_calls()
        call_names = [c[0] for c in calls]
        if "add_datastore" not in call_names:
            errors.append("Full pipeline: add_datastore was never called")
        add_files = [c for c in calls if c[0] == "add_datafile"]
        found_paths = [c[1].replace("\\", "/") for c in add_files]
        if not any("manual.md" in p for p in found_paths):
            errors.append(f"Full pipeline: add_datafile for manual.md not found in {found_paths}")
        if not any("hello.md" in p for p in found_paths):
            errors.append(f"Full pipeline: add_datafile for hello.md not found in {found_paths}")
        if not any("world.md" in p for p in found_paths):
            errors.append(f"Full pipeline: add_datafile for world.md not found in {found_paths}")

    finally:
        fix.teardown()
    return errors


def test_dkb_only_recon() -> List[str]:
    """DKB_ONLY trigger: upstream skipped, downstream recon runs."""
    errors: List[str] = []
    fix = DKBE2EFixture("dkbonly")
    fix.setup()
    try:
        _reset_calls()

        # Place a file manually — the plugin won't run so this is the only file
        _write_output_file(fix.sub_name, "only_file.md", "DKB-only test file.\n")

        # Trigger DKB_ONLY
        _api("POST", f"/dkb_datastores/{fix.ds_id}/update")

        # Wait for datastore_subscription → ENABLED
        deadline = time.time() + MAX_WAIT
        last_status = None
        while time.time() < deadline:
            ds_detail = _api("GET", f"/dkb_datastores/{fix.ds_id}")
            subs_detail = ds_detail.get("subscriptions", [])
            if subs_detail:
                last_status = subs_detail[0].get("status", "")
                if last_status == "ENABLED":
                    break
            time.sleep(POLL_INTERVAL)
        else:
            errors.append(
                f"DKB-only: ds_sub did not reach ENABLED (last={last_status})"
            )
            return errors

        # --- Verify DKB service calls ---
        calls = _read_calls()
        add_files = [c for c in calls if c[0] == "add_datafile"]
        found_paths = [c[1].replace("\\", "/") for c in add_files]
        if not any("only_file.md" in p for p in found_paths):
            errors.append(f"DKB-only: add_datafile for only_file.md not found in {found_paths}")

        # --- Verify NO upstream ran (plugin output files should NOT exist) ---
        hello_path = os.path.join(fix.output_dir, "hello.md")
        world_path = os.path.join(fix.output_dir, "world.md")
        if os.path.isfile(hello_path):
            errors.append(f"DKB-only: hello.md exists (upstream should not have run)")
        if os.path.isfile(world_path):
            errors.append(f"DKB-only: world.md exists (upstream should not have run)")

    finally:
        fix.teardown()
    return errors


def leak_check() -> List[str]:
    """After cleanup, verify no test records remain. Fail if leaked, then clean."""
    import subprocess as _sp
    errors: List[str] = []
    try:
        all_ds = _api("GET", "/dkb_datastores")
        for ds in all_ds:
            if ds["name"].startswith("e2e-"):
                errors.append(f"Leaked datastore: {ds['name']} ({ds['datastore_id']})")
                try:
                    _api("DELETE", f"/dkb_datastores/{ds['datastore_id']}")
                except Exception:
                    pass

        all_subs = _api("GET", "/subscriptions")
        for sub in all_subs:
            n = sub.get("name", "")
            if n.startswith("e2e-"):
                errors.append(f"Leaked subscription: {n} ({sub['id']})")
                try:
                    _api("DELETE", f"/subscriptions/{sub['id']}")
                except Exception:
                    pass

        _sp.run(["rm", "-rf", "/output/testDKBWriterPlugin"], capture_output=True)

    except Exception as exc:
        errors.append(f"Leak-check error: {exc}")
    return errors


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    all_errors: List[str] = []

    # Pre-test: clean up any leftover state
    all_errors.extend(leak_check())

    all_errors.extend(test_full_pipeline())
    all_errors.extend(test_dkb_only_recon())

    # Post-test leak check
    all_errors.extend(leak_check())

    if all_errors:
        print(f"FAILED ({len(all_errors)} error(s))")
        for e in all_errors:
            print(f"  - {e}")
        return 1
    else:
        print("OK (all DKB e2e tests passed)")
        return 0


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "confirm":
        sys.exit(main())
    else:
        print("Usage: python /src/testing/test_dkb_e2e.py confirm")
        sys.exit(1)
