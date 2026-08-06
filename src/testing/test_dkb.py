"""Unit and integration tests for the DKB (Downstream Knowledge Base) system.

Usage::

    python /src/testing/test_dkb.py confirm

Requires a running DB (postgres), Redis, and the autokb source tree
mounted at /src. Uses a separate database 'autokb_test' if available,
else falls back to the main database.
"""

import json
import os
import shutil
import sys
import tempfile
import time
import unittest
from unittest.mock import MagicMock, patch

# Ensure /src is on sys.path
_SRC = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from utils.constants import (
    OPERATION_FULL, OPERATION_DKB_ONLY,
    STATE_ENABLED, STATE_ENQUEUED, STATE_ERROR, STATE_DISABLED,
)
from utils.database import (
    AKBDatafile, DKBService, DKBDatastore,
    DatabaseManager, DatastoreDatafile, DatastoreSubscription,
    run_migrations,
)
from utils.dkb_service_base import BaseDKBService, compute_file_hash
from utils.dkb_registry import DKBRegistry
from utils.queue_utils import QueueManager, _encode_item, _decode_item, P_QUEUE_KEY, S_QUEUE_KEY
from utils.misc_utils import uuid7


DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://autokb:autokb@autokb-db:5432/autokb")
REDIS_URL = os.environ.get("REDIS_URL", "redis://autokb-redis:6379/0")
TEST_OUTPUT = "/tmp/dkb_test_output"


# ---- Helpers ----
def _db() -> DatabaseManager:
    """Create a fresh DatabaseManager for test use."""
    return DatabaseManager(DATABASE_URL, component="test_dkb")


def _queue() -> QueueManager:
    return QueueManager(REDIS_URL)


# ---- Mock DKB service for testing ----
_MOCK_REMOTE_ID = "remote-file-123"
_MOCK_DATASTORE_ID = "remote-ds-abc"


class MockDKBService(BaseDKBService):
    """Concrete DKB service for testing — records all calls."""

    metadata = {
        "name": "MockDKB",
        "description": "Test DKB service",
        "icon": "mock.png",
    }

    def __init__(self, datastore_row, db):
        super().__init__(datastore_row, db)
        self.calls = []

    def add_datafile(self, path: str) -> str:
        self.calls.append(("add_datafile", path, self.remote_datastore_id))
        return _MOCK_REMOTE_ID

    def update_datafile(self, remote_datafile_id: str, path: str) -> None:
        self.calls.append(("update_datafile", remote_datafile_id, path))

    def remove_datafile(self, remote_datafile_id: str) -> None:
        self.calls.append(("remove_datafile", remote_datafile_id))

    def add_datastore(self) -> str:
        self.calls.append(("add_datastore",))
        return _MOCK_DATASTORE_ID

    def remove_datastore(self) -> None:
        self.calls.append(("remove_datastore",))

    def clear_datastore(self) -> None:
        self.calls.append(("clear_datastore",))


# ---- Test Suite ----
class TestDKB(unittest.TestCase):
    """Tests for the DKB subsystem."""

    @classmethod
    def setUpClass(cls):
        # Ensure DB schema is current
        run_migrations(DATABASE_URL)

    def setUp(self):
        self.db = _db()
        self.queue = _queue()
        # Purge Redis test queues
        self.queue.client.delete(P_QUEUE_KEY)
        self.queue.client.delete(S_QUEUE_KEY)
        # Create a test subscription and DKB service
        self._cleanup_test_data()
        self.sub, self.ds, self.ds_link = self._create_test_fixtures()
        # Ensure output dir exists
        os.makedirs(TEST_OUTPUT, exist_ok=True)

    def tearDown(self):
        self._cleanup_test_data()
        self.db.dispose()
        if os.path.isdir(TEST_OUTPUT):
            shutil.rmtree(TEST_OUTPUT)

    def _cleanup_test_data(self):
        """Remove any rows we might have created."""
        with self.db.get_session() as s:
            for tbl in (DatastoreDatafile, AKBDatafile, DatastoreSubscription,
                        DKBDatastore, DKBService):
                s.query(tbl).delete()

    def _create_test_fixtures(self):
        """Create a DKB service, datastore, and a subscription."""
        # Ensure a subscription exists
        with self.db.get_session() as s:
            from utils.database import Subscription, PluginRegistryState
            # Ensure test plugin state
            test_plugin = s.query(PluginRegistryState).filter(
                PluginRegistryState.plugin_id == "test_plugin"
            ).first()
            if not test_plugin:
                s.add(PluginRegistryState(
                    plugin_id="test_plugin", schema_hash="abc123",
                    last_loaded=__import__("datetime").datetime.now(
                        __import__("datetime").timezone.utc
                    ),
                ))
            sub = s.query(Subscription).filter(
                Subscription.plugin_id == "test_plugin",
                Subscription.name == "test_sub",
            ).first()
            if not sub:
                sub = Subscription(
                    id=str(uuid7()), plugin_id="test_plugin", name="test_sub",
                    config={}, status=STATE_ENABLED, access_level="PRIVATE",
                    sub_type="SCHEDULED", cron="0 0 * * *",
                )
                s.add(sub)
                s.flush()
            sub_id = sub.id

        # Create DKB service
        svc = self.db.upsert_dkb_service("TestService", "Test Description")
        # Create datastore
        ds = self.db.create_datastore(svc.id, "TestDS", "https://example.com", "test-key", {})
        # Link subscription
        self.db.link_datastore_subscriptions(ds.id, [sub_id], status=STATE_ENABLED)
        ds_link = self.db.list_datastore_subscriptions(ds.id)[0]
        return sub_id, ds, ds_link

    # ---- 1. Database CRUD tests ----

    def test_dkb_service_crud(self):
        svc = self.db.upsert_dkb_service("MyService", "desc")
        self.assertIsNotNone(svc.id)
        fetched = self.db.get_dkb_service(svc.id)
        self.assertEqual(fetched.name, "MyService")
        svc2 = self.db.upsert_dkb_service("MyService", "updated")
        self.assertEqual(svc2.id, svc.id)  # upsert returns existing
        services = self.db.list_dkb_services()
        self.assertIn(svc.id, [s.id for s in services])

    def test_datastore_crud(self):
        svc = self.db.upsert_dkb_service("Svc", "")
        ds = self.db.create_datastore(
            svc.id, "MyDS", "https://example.com/api", "secret_key",
            {"extra": "value"},
        )
        self.assertIsNotNone(ds.id)
        fetched = self.db.get_datastore(ds.id)
        self.assertEqual(fetched.name, "MyDS")
        self.assertEqual(fetched.api_url, "https://example.com/api")
        # api_key should be encrypted
        self.assertNotEqual(fetched.api_key, "secret_key")
        # decrypt check
        decrypted = self.db.decrypt_datastore_api_key(fetched)
        self.assertEqual(decrypted, "secret_key")
        # Update
        updated = self.db.update_datastore(ds.id, name="MyDS-Updated")
        self.assertEqual(updated.name, "MyDS-Updated")
        # Set remote_id
        self.db.set_datastore_remote_id(ds.id, "remote-xyz")
        fetched2 = self.db.get_datastore(ds.id)
        self.assertEqual(fetched2.remote_datastore_id, "remote-xyz")

    def test_subscription_link_crud(self):
        svc = self.db.upsert_dkb_service("Svc", "")
        ds = self.db.create_datastore(svc.id, "DS", "url", "key", {})
        self.db.link_datastore_subscriptions(ds.id, [self.sub], status="ENABLED")
        links = self.db.list_datastore_subscriptions(ds.id)
        self.assertEqual(len(links), 1)
        self.assertEqual(links[0].subscription_id, self.sub)
        self.assertEqual(links[0].status, "ENABLED")

        # List for subscription
        subs_for = self.db.list_datastores_for_subscription(self.sub)
        self.assertGreaterEqual(len(subs_for), 1)

        # Set status
        self.db.set_datastore_subscription_status(ds.id, self.sub, "ERROR", message="oops")
        links2 = self.db.list_datastore_subscriptions(ds.id)
        self.assertEqual(links2[0].status, "ERROR")
        self.assertEqual(links2[0].last_message, "oops")

        # Delete
        self.db.delete_datastore_subscription(ds.id, self.sub)
        links3 = self.db.list_datastore_subscriptions(ds.id)
        self.assertEqual(len(links3), 0)

    def test_datafile_crud(self):
        # Create a test file
        test_path = os.path.join(TEST_OUTPUT, "test_file.md")
        with open(test_path, "w") as f:
            f.write("# Hello\nWorld\n")
        size = os.path.getsize(test_path)
        mtime = os.path.getmtime(test_path)
        h = compute_file_hash(test_path)

        df = self.db.get_or_create_datafile(self.sub, test_path, size, mtime, h)
        self.assertIsNotNone(df.id)
        self.assertEqual(df.path, test_path)
        self.assertEqual(df.size, size)

        # get_or_create should return existing
        df2 = self.db.get_or_create_datafile(self.sub, test_path, size, mtime, h)
        self.assertEqual(df2.id, df.id)

        # Lookup by path
        df3 = self.db.get_datafile_by_path(test_path)
        self.assertIsNotNone(df3)

        # Update stats
        new_size = 100
        new_mtime = 1000.0
        new_hash = "newhash"
        self.db.update_datafile_stats(df.id, new_size, new_mtime, new_hash)
        df4 = self.db.get_datafile(df.id)
        self.assertEqual(df4.size, new_size)

        # List for subscription
        files = self.db.list_datafiles_for_subscription(self.sub)
        self.assertIn(df.id, [f.id for f in files])

        # Delete
        self.db.delete_datafile(df.id)
        df5 = self.db.get_datafile(df.id)
        self.assertIsNone(df5)

    def test_datastore_datafile_crud(self):
        # Create a datafile first
        test_path = os.path.join(TEST_OUTPUT, "df_test.md")
        with open(test_path, "w") as f:
            f.write("data")
        h = compute_file_hash(test_path)
        df = self.db.get_or_create_datafile(
            self.sub, test_path, os.path.getsize(test_path),
            os.path.getmtime(test_path), h,
        )

        # Insert
        self.db.insert_datastore_datafile(
            self.ds.id, df.id, "remote-001", h,
        )
        ds_df = self.db.get_datastore_datafile(self.ds.id, df.id)
        self.assertIsNotNone(ds_df)
        self.assertEqual(ds_df.remote_datafile_id, "remote-001")

        # Update hash
        new_hash = "newhash456"
        self.db.update_datastore_datafile_hash(self.ds.id, df.id, new_hash)
        ds_df2 = self.db.get_datastore_datafile(self.ds.id, df.id)
        self.assertEqual(ds_df2.hash, new_hash)

        # List for datastore
        items = self.db.list_datafiles_for_datastore(self.ds.id)
        self.assertEqual(len(items), 1)

        # Delete
        self.db.delete_datastore_datafile(self.ds.id, df.id)
        ds_df3 = self.db.get_datastore_datafile(self.ds.id, df.id)
        self.assertIsNone(ds_df3)

    # ---- 2. Queue JSON tests ----

    def test_queue_json_encoding(self):
        encoded = _encode_item("sub-123", OPERATION_FULL)
        parsed = _decode_item(encoded)
        self.assertEqual(parsed["sub_id"], "sub-123")
        self.assertEqual(parsed["operation"], OPERATION_FULL)

        encoded2 = _encode_item("sub-456", OPERATION_DKB_ONLY)
        parsed2 = _decode_item(encoded2)
        self.assertEqual(parsed2["sub_id"], "sub-456")
        self.assertEqual(parsed2["operation"], OPERATION_DKB_ONLY)

    def test_queue_push_pop(self):
        q = self.queue
        q.push_primary("test-1", OPERATION_FULL)
        q.push_primary("test-2", OPERATION_DKB_ONLY)
        q.push_primary("test-1", OPERATION_DKB_ONLY)

        self.assertTrue(q.any_full_for("test-1"))
        self.assertFalse(q.any_full_for("test-2"))

        item = q.pop_primary(timeout=1)
        self.assertIsNotNone(item)
        self.assertEqual(item["sub_id"], "test-1")

        # drain_all
        n = q.drain_all("test-1")
        self.assertGreaterEqual(n, 1)
        self.assertFalse(q.has_in_queue("test-1"))

        # remaining: test-2
        self.assertTrue(q.has_in_queue("test-2"))

    def test_queue_drain_operations(self):
        q = self.queue
        q.push_primary("sub-1", OPERATION_FULL)
        q.push_primary("sub-1", OPERATION_DKB_ONLY)
        q.push_secondary("sub-1", OPERATION_DKB_ONLY)

        n = q.drain_all("sub-1")
        self.assertEqual(n, 3)
        self.assertEqual(q.queue_depth(P_QUEUE_KEY), 0)
        self.assertEqual(q.queue_depth(S_QUEUE_KEY), 0)

    # ---- 3. DKB registry tests ----

    def test_dkb_registry_load(self):
        reg = DKBRegistry(dkbs_dir="/src/dkbservices", component="test_dkb_reg")
        reg.reload_all()
        records = reg.list_records()
        names = [r.service_name for r in records]
        self.assertIn("OpenWebUI", names)
        self.assertIn("Cognee", names)

    # ---- 4. Service base class tests ----

    def test_compute_file_hash(self):
        test_path = os.path.join(TEST_OUTPUT, "hash_test.txt")
        content = b"Hello World! " * 1000
        with open(test_path, "wb") as f:
            f.write(content)
        h = compute_file_hash(test_path)
        import hashlib
        expected = hashlib.sha256(content).hexdigest()
        self.assertEqual(h, expected)

    def test_base_add_datafile(self):
        ds_row = self.db.get_datastore(self.ds.id)
        ds_row.api_key = self.db.decrypt_datastore_api_key(ds_row)
        svc = MockDKBService(ds_row, self.db)
        svc.remote_datastore_id = "mock-remote-id"

        test_path = os.path.join(TEST_OUTPUT, "base_add.md")
        with open(test_path, "w") as f:
            f.write("test content")
        svc.base_add_datafile(self.sub, test_path)

        # Should have created akb_datafile
        df = self.db.get_datafile_by_path(test_path)
        self.assertIsNotNone(df)
        # Should have called add_datafile
        self.assertIn(("add_datafile", test_path, "mock-remote-id"), svc.calls)
        # Should have datastore_datafile
        ds_df = self.db.get_datastore_datafile(self.ds.id, df.id)
        self.assertIsNotNone(ds_df)

    def test_base_update_datafile(self):
        ds_row = self.db.get_datastore(self.ds.id)
        ds_row.api_key = self.db.decrypt_datastore_api_key(ds_row)
        svc = MockDKBService(ds_row, self.db)
        svc.remote_datastore_id = "mock-remote-id"

        test_path = os.path.join(TEST_OUTPUT, "base_update.md")
        with open(test_path, "w") as f:
            f.write("original")
        h = compute_file_hash(test_path)
        df = self.db.get_or_create_datafile(
            self.sub, test_path, os.path.getsize(test_path),
            os.path.getmtime(test_path), h,
        )
        self.db.insert_datastore_datafile(self.ds.id, df.id, "remote-old", h)

        new_hash = "newhash123"
        svc.base_update_datafile(df.id, new_hash)
        self.assertIn(("update_datafile", "remote-old", test_path), svc.calls)

        ds_df = self.db.get_datastore_datafile(self.ds.id, df.id)
        self.assertEqual(ds_df.hash, new_hash)

    def test_base_remove_datafile(self):
        ds_row = self.db.get_datastore(self.ds.id)
        ds_row.api_key = self.db.decrypt_datastore_api_key(ds_row)
        svc = MockDKBService(ds_row, self.db)
        svc.remote_datastore_id = "mock-remote-id"

        test_path = os.path.join(TEST_OUTPUT, "base_remove.md")
        with open(test_path, "w") as f:
            f.write("remove me")
        h = compute_file_hash(test_path)
        df = self.db.get_or_create_datafile(
            self.sub, test_path, os.path.getsize(test_path),
            os.path.getmtime(test_path), h,
        )
        self.db.insert_datastore_datafile(self.ds.id, df.id, "remote-del", h)

        svc.base_remove_datafile(df.id)
        self.assertIn(("remove_datafile", "remote-del"), svc.calls)
        ds_df = self.db.get_datastore_datafile(self.ds.id, df.id)
        self.assertIsNone(ds_df)

    def test_base_add_datastore(self):
        ds_row = self.db.get_datastore(self.ds.id)
        ds_row.api_key = self.db.decrypt_datastore_api_key(ds_row)
        self.assertIsNone(ds_row.remote_datastore_id)
        svc = MockDKBService(ds_row, self.db)
        svc.base_add_datastore()
        self.assertIn(("add_datastore",), svc.calls)
        refreshed = self.db.get_datastore(self.ds.id)
        self.assertEqual(refreshed.remote_datastore_id, _MOCK_DATASTORE_ID)

    # ---- 5. Recon engine tests ----

    def _write_output_file(self, name: str, content: str) -> str:
        path = os.path.join(TEST_OUTPUT, "test_plugin", "test_sub", name)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            f.write(content)
        return path

    @patch("worker.dkb_recon._get_service")
    def test_reconcile_adds_new_file(self, mock_get_service):
        from worker.dkb_recon import reconcile_subscription_datastores

        # Set up mock service
        mock_svc = MagicMock()
        mock_svc.remote_datastore_id = "mock-remote"
        mock_svc.name = "TestSvc"
        mock_svc.base_add_datafile = MagicMock()
        mock_svc.base_update_datafile = MagicMock()
        mock_svc.base_remove_datafile = MagicMock()
        mock_get_service.return_value = mock_svc

        # Write a file to the sub's output dir
        self._write_output_file("article.md", "# Test Article\n\nContent here.")

        # Mock the Subscription object
        sub = self.db.get_subscription(self.sub)

        # Run recon
        reconcile_subscription_datastores(
            sub, self.db, MagicMock(), MagicMock(),
        )

        # base_add_datafile should have been called
        self.assertTrue(mock_svc.base_add_datafile.called)

    @patch("worker.dkb_recon._get_service")
    def test_reconcile_removes_deleted_file(self, mock_get_service):
        from worker.dkb_recon import reconcile_subscription_datastores

        # Create a datafile row for a file that no longer exists on disk
        test_path = os.path.join(TEST_OUTPUT, "test_plugin", "test_sub", "old.md")
        os.makedirs(os.path.dirname(test_path), exist_ok=True)
        with open(test_path, "w") as f:
            f.write("old content")
        h = compute_file_hash(test_path)
        os.remove(test_path)

        df = self.db.get_or_create_datafile(
            self.sub, test_path, 100, 1000.0, h,
        )
        self.db.insert_datastore_datafile(self.ds.id, df.id, "remote-old", h)

        mock_svc = MagicMock()
        mock_svc.remote_datastore_id = "mock-remote"
        mock_svc.name = "TestSvc"
        mock_svc.base_add_datafile = MagicMock()
        mock_svc.base_remove_datafile = MagicMock()
        mock_get_service.return_value = mock_svc

        sub = self.db.get_subscription(self.sub)
        reconcile_subscription_datastores(
            sub, self.db, MagicMock(), MagicMock(),
        )

        # base_remove_datafile should have been called
        self.assertTrue(mock_svc.base_remove_datafile.called)

    @patch("worker.dkb_recon._get_service")
    def test_reconcile_skips_disabled_ds(self, mock_get_service):
        from worker.dkb_recon import reconcile_subscription_datastores

        # Set the datastore_subscription to DISABLED
        self.db.set_datastore_subscription_status(self.ds.id, self.sub, STATE_DISABLED)

        self._write_output_file("enabled_only.md", "should be skipped")

        mock_svc = MagicMock()
        mock_get_service.return_value = mock_svc

        sub = self.db.get_subscription(self.sub)
        reconcile_subscription_datastores(
            sub, self.db, MagicMock(), MagicMock(),
        )

        mock_svc.base_add_datafile.assert_not_called()
        mock_svc.base_remove_datafile.assert_not_called()

    @patch("worker.dkb_recon._get_service")
    def test_reconcile_sets_error_on_failure(self, mock_get_service):
        from worker.dkb_recon import reconcile_subscription_datastores

        mock_svc = MagicMock()
        mock_svc.remote_datastore_id = "mock-remote"
        mock_svc.name = "TestSvc"
        mock_svc.base_add_datafile = MagicMock()
        mock_svc.base_add_datafile.side_effect = RuntimeError("API failure")
        mock_svc.base_add_datastore = MagicMock()
        mock_get_service.return_value = mock_svc

        self._write_output_file("error_test.md", "will fail")

        sub = self.db.get_subscription(self.sub)
        reconcile_subscription_datastores(
            sub, self.db, MagicMock(), MagicMock(),
        )

        # Check that the datastore_subscription is in ERROR state
        links = self.db.list_datastore_subscriptions(self.ds.id)
        self.assertEqual(links[0].status, STATE_ERROR)
        self.assertIn("failure", links[0].last_message)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "confirm":
        sys.argv.pop(1)
        unittest.main()
    else:
        print("Usage: python /src/testing/test_dkb.py confirm")
        sys.exit(1)
