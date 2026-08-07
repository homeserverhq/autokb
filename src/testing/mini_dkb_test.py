"""Temp test runner — ONLY runs DKB/Sink e2e + unit tests for fast debugging."""
import json, os, sys, time, traceback
sys.path.insert(0, "/src")
import requests

MANAGER_URL = "http://localhost:80"
BACKEND_KEY = os.environ.get("BACKEND_API_KEY", "sZdx8RLMFOBBnVyINfjvlQXrSHMg0Wwy")
ADMIN_USER = "admin"
ADMIN_PASS = os.environ.get("ADMIN_PASSWORD", "23r23weqc23eWR$%$W$%")
TEST_PLUGIN_DIR = "/src/testing/plugins"
TEST_SINK_DIR = "/src/testing/sinks"
SINK_DIR = "/src/sinks"
SINK_CALLS_FILE = "/output/.sink_e2e_calls.json"

def log(msg): print(f"[mini] {msg}", flush=True)

def hdrs(): return {"X-Api-Key": BACKEND_KEY}

def api_get(p):
    r = requests.get(f"{MANAGER_URL}{p}", headers=hdrs(), timeout=30)
    r.raise_for_status(); return r.json() if r.content else None

def api_post(p, body=None):
    r = requests.post(f"{MANAGER_URL}{p}", headers={**hdrs(), "Content-Type": "application/json"},
                      data=json.dumps(body or {}), timeout=30)
    r.raise_for_status(); return r.json() if r.content else None

def api_del(p):
    r = requests.delete(f"{MANAGER_URL}{p}", headers=hdrs(), timeout=30)
    if r.status_code not in (200,204,404): raise RuntimeError(f"DELETE {p}: {r.status_code}")

def _sync_sinks():
    if not os.path.isdir(TEST_SINK_DIR): return
    for fname in sorted(os.listdir(TEST_SINK_DIR)):
        if fname.endswith(".py") and not fname.startswith("__"):
            import shutil
            shutil.copy(os.path.join(TEST_SINK_DIR, fname), os.path.join(SINK_DIR, fname))
    log("Synced test sinks")

def _reset_calls():
    try: os.remove(SINK_CALLS_FILE)
    except: pass

def _read_calls():
    if not os.path.isfile(SINK_CALLS_FILE): return []
    return [json.loads(l) for l in open(SINK_CALLS_FILE).read().strip().split("\n") if l]

def _write_output(sub_name, name, content):
    path = os.path.join("/output", "testSinkWriterPlugin", sub_name, name)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f: f.write(content)

def _delete_sub(sid):
    api_del(f"/api/subscriptions/{sid}")

def wait_sub_status(sid, target, timeout=120):
    deadline = time.time() + timeout
    while time.time() < deadline:
        s = api_get(f"/api/subscriptions/{sid}")
        if s.get("status") == target: return True
        time.sleep(2)
    return False

def wait_target_status(tid, target="ENABLED", timeout=120):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            t = api_get(f"/api/targets/{tid}")
            subs = t.get("subscriptions", [])
            if subs: last = subs[0].get("status", "")
            if last == target: return True
        except: pass
        time.sleep(2)
    log(f"  last target status: {last}")
    return False

# ----------------------------------------------------------------
log("=== DKB/Sink mini test runner ===")

# Sync test sinks
_sync_sinks()

# Wait for test sink to appear
log("Waiting for 'test' sink in /api/sinks...")
for _ in range(15):
    svcs = api_get("/api/sinks")
    if any(s.get("name") == "test" for s in svcs):
        log("test sink loaded OK")
        break
    time.sleep(2)
else:
    log("FAIL: test sink did not appear"); sys.exit(1)

# ----------------------------------------------------------------
# TEST 1: Full pipeline
log("\n--- TEST 1: Sink full pipeline ---")
_reset_calls()
uid = str(time.time()).replace(".", "")[-8:]
sub_name = f"mini-full-{uid}"
t_name = f"mini-full-ds-{uid}"
sub_id = None; t_id = None

try:
    sub = api_post("/api/subscriptions/testSinkWriterPlugin", {
        "name": sub_name, "config": {}, "access_level": "PRIVATE", "cron": "0 0 * * *"})
    sub_id = sub["id"]
    log(f"  created sub {sub_id}")

    svcs = api_get("/api/sinks")
    svc_id = next(s["service_id"] for s in svcs if s.get("name") == "test")

    t = api_post(f"/api/sinks/{svc_id}/targets", {
        "name": t_name, "api_url": "http://fake/api", "api_key": "test",
        "target_extra_params": {}, "subscription_ids": [sub_id]})
    t_id = t["target_id"]
    log(f"  created target {t_id}")

    _write_output(sub_name, "manual.md", "Manually placed.\n")

    # Trigger FULL
    requests.post(f"{MANAGER_URL}/api/subscriptions/{sub_id}/trigger",
                  headers=hdrs(), timeout=10)
    log("  triggered FULL, waiting for sub ENABLED...")

    if not wait_sub_status(sub_id, "ENABLED", timeout=120):
        log(f"  FAIL: sub did not reach ENABLED")
        # Check last status
        s = api_get(f"/api/subscriptions/{sub_id}")
        log(f"  sub status: {s.get('status')}, last_msg: {s.get('last_message','')[:200]}")
        raise SystemExit(1)

    log("  sub ENABLED, waiting for target ENABLED...")
    if not wait_target_status(t_id, "ENABLED", timeout=120):
        raise SystemExit(1)

    calls = _read_calls()
    add_files = [c for c in calls if c[0] == "add_datafile"]
    found = " ".join(c[1] for c in add_files)
    log(f"  add_datafile calls: {len(add_files)}")
    for want in ["manual.md", "hello.md", "world.md"]:
        if want not in found:
            log(f"  FAIL: {want} not in add_datafile calls"); raise SystemExit(1)
    log("  PASS: all 3 files added")

except Exception as e:
    log(f"  FAIL: {e}")
    traceback.print_exc()
finally:
    if t_id:
        try: requests.delete(f"{MANAGER_URL}/api/targets/{t_id}", headers=hdrs(), timeout=10)
        except: pass
    if sub_id:
        _delete_sub(sub_id)

# ----------------------------------------------------------------
# TEST 2: Sink-only recon
log("\n--- TEST 2: Sink-only recon ---")
_reset_calls()
uid = str(time.time()).replace(".", "")[-8:]
sub_name = f"mini-only-{uid}"
t_name = f"mini-only-ds-{uid}"
sub_id = None; t_id = None

try:
    sub = api_post("/api/subscriptions/testSinkWriterPlugin", {
        "name": sub_name, "config": {}, "access_level": "PRIVATE", "cron": "0 0 * * *"})
    sub_id = sub["id"]
    log(f"  created sub {sub_id}")

    svcs = api_get("/api/sinks")
    svc_id = next(s["service_id"] for s in svcs if s.get("name") == "test")

    t = api_post(f"/api/sinks/{svc_id}/targets", {
        "name": t_name, "api_url": "http://fake/api", "api_key": "test",
        "target_extra_params": {}, "subscription_ids": [sub_id]})
    t_id = t["target_id"]
    log(f"  created target {t_id}")

    # Place file directly (no plugin run)
    _write_output(sub_name, "only_file.md", "Sink-only test.\n")

    # Trigger SINK_ONLY via target update
    api_post(f"/api/targets/{t_id}/update", {})
    log("  triggered SINK_ONLY, waiting for target ENABLED...")

    if not wait_target_status(t_id, "ENABLED", timeout=120):
        log(f"  FAIL: target did not reach ENABLED")
        raise SystemExit(1)

    calls = _read_calls()
    add_files = [c for c in calls if c[0] == "add_datafile"]
    found = " ".join(c[1] for c in add_files)
    log(f"  add_datafile calls: {len(add_files)}")
    if "only_file.md" not in found:
        log(f"  FAIL: only_file.md not added"); raise SystemExit(1)

    # Verify upstream did NOT run
    hello = f"/output/testSinkWriterPlugin/{sub_name}/hello.md"
    world = f"/output/testSinkWriterPlugin/{sub_name}/world.md"
    if os.path.isfile(hello): log(f"  FAIL: hello.md exists (upstream ran)"); raise SystemExit(1)
    log("  PASS: sink-only recon OK, upstream skipped")

except Exception as e:
    log(f"  FAIL: {e}")
    traceback.print_exc()
finally:
    if t_id:
        try: requests.delete(f"{MANAGER_URL}/api/targets/{t_id}", headers=hdrs(), timeout=10)
        except: pass
    if sub_id:
        _delete_sub(sub_id)

# ----------------------------------------------------------------
# TEST 3-6: unit tests (quick DB-level)
log("\n--- TEST 3: Unit — DB CRUD ---")
from utils.database import DatabaseManager
import uuid
db = DatabaseManager(os.environ.get("DATABASE_URL", "postgresql://autokb:autokb@autokb-db:5432/autokb"), component="mini_test")

try:
    svc = db.upsert_sink("MiniSvc", "desc")
    assert svc.id, "svc id None"
    fetched = db.get_sink(svc.id)
    assert fetched.name == "MiniSvc", f"wrong name: {fetched.name}"
    log("  PASS: sink CRUD")
except Exception as e:
    log(f"  FAIL: {e}"); raise

from utils.sink_registry import SinkRegistry
log("\n--- TEST 4: Unit — Registry ---")
reg = SinkRegistry(sinks_dir="/src/sinks", component="mini_reg")
reg.reload_all()
rec = reg.get("test")
assert rec is not None, "test sink not loaded"
assert rec.service_name == "test", f"wrong name: {rec.service_name}"
log("  PASS: registry load")

log("\n--- TEST 5: Unit — BaseSink instantiation ---")
from utils.sink_base import BaseSink, compute_file_hash
svc = db.upsert_sink("MiniSvc", "desc")
t = db.create_target(svc.id, "MiniTarget", "http://x.com", "key", {})
inst = rec.cls(t, db)
assert isinstance(inst, BaseSink), "not a BaseSink instance"
assert inst.target_id == t.id
log("  PASS: BaseSink instantiation")

log("\n--- TEST 6: Unit — add_datafile via base ---")
df_path = f"/tmp/mini_df_test_{uid}.md"
with open(df_path, "w") as f: f.write("test content")
from utils.database import Subscription
sub_id = str(uuid.uuid4())
with db.get_session() as s:
    from utils.database import Subscription as Sub, PluginRegistryState
    p = s.query(PluginRegistryState).filter(PluginRegistryState.plugin_id == "test_plugin").first()
    if not p:
        s.add(PluginRegistryState(plugin_id="test_plugin", schema_hash="abc", last_loaded=__import__("datetime").datetime.now()))
    s.add(Sub(id=sub_id, plugin_id="test_plugin", name=f"mini_sub_{uid}", config={}, status="ENABLED", access_level="PRIVATE", sub_type="SCHEDULED", cron="0 0 * * *"))
inst.base_add_datafile(sub_id, df_path)
tdf = db.list_datafiles_for_target(t.id)
assert len(tdf) == 1, f"expected 1 tdf, got {len(tdf)}"
db.delete_target_row(t.id)
db.delete_sink(svc.id)
db.delete_subscription_row(sub_id)
db.delete_datafile(db.get_datafile_by_path(df_path).id)
os.remove(df_path)
log("  PASS: add_datafile via base")

log("\n=== ALL MINI TESTS PASSED ===")
