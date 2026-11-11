"""OpenWebUI duplicate-content regression tests.

Runnable directly:
    python /src/testing/unit_tests/test_openwebui_duplicate.py
"""

import os
import sys
import types

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, SRC)

try:
    import requests  # noqa: F401
except ImportError:
    requests_stub = types.ModuleType("requests")

    class RequestException(Exception):
        pass

    class HTTPError(RequestException):
        pass

    class Response:
        status_code = 200
        text = ""
        headers = {}

        def json(self):
            return {}

    requests_stub.RequestException = RequestException
    requests_stub.HTTPError = HTTPError
    requests_stub.Response = Response
    requests_stub.get = lambda *args, **kwargs: None
    requests_stub.post = lambda *args, **kwargs: None
    requests_stub.delete = lambda *args, **kwargs: None
    sys.modules["requests"] = requests_stub

from builtin_sinks.openWebUISink import OpenWebUISink
from utils.misc_utils import SinkDuplicateContent


class _FakeDB:
    def __init__(self, refs=0):
        self.refs = refs
        self.deleted = []

    def count_remote_datafile_references(self, remote_id, **kwargs):
        return self.refs


class _Target:
    id = "target-1"
    service_id = "svc-1"
    name = "DevTarget"
    api_url = "http://openwebui"
    api_key = "key"
    remote_target_id = "kb-1"
    target_extra_params = {}
    include_path_in_filename = False
    access_level = "PRIVATE"
    schedule_start = None
    schedule_end = None


def _sink(db=None, refs=0):
    sink = OpenWebUISink(_Target(), db or _FakeDB(refs))
    sink._log = types.SimpleNamespace(
        info=lambda *a, **k: None,
        warning=lambda *a, **k: None,
        error=lambda *a, **k: None,
    )
    return sink


def test_find_owner_matches_hash():
    sink = _sink()
    sink._content_hash = lambda file_id: "hash-1"
    sink._kb_files = lambda kb: [
        {"id": "new-file", "hash": "hash-1"},
        {"id": "owner-file", "hash": "hash-1"},
    ]
    owner, content_hash = sink._find_duplicate_owner("new-file")
    assert owner == "owner-file"
    assert content_hash == "hash-1"


def test_orphan_duplicate_is_typed_and_not_deleted():
    sink = _sink()
    sink._upload_file = lambda path: "new-file"
    sink._link_to_kb = lambda kb, fid: (_ for _ in ()).throw(
        RuntimeError("openWebUI link file to KB failed: HTTP 400: Duplicate content detected")
    )
    sink._find_duplicate_owner = lambda fid: (None, "orphan-hash")
    sink._delete_file = lambda fid: (_ for _ in ()).throw(AssertionError("must not delete"))
    try:
        sink.add_datafile("/tmp/doc.md")
    except SinkDuplicateContent as exc:
        assert exc.reason == "orphan_vectors"
        assert exc.content_hash == "orphan-hash"
        return
    raise AssertionError("expected SinkDuplicateContent")


def test_duplicate_owner_is_reused_without_delete():
    sink = _sink()
    sink._upload_file = lambda path: "new-file"
    sink._link_to_kb = lambda kb, fid: (_ for _ in ()).throw(
        RuntimeError("openWebUI link file to KB failed: HTTP 400: Duplicate content detected")
    )
    sink._find_duplicate_owner = lambda fid: ("owner-file", "hash-1")
    sink._delete_file = lambda fid: (_ for _ in ()).throw(AssertionError("must not delete"))
    assert sink.add_datafile("/tmp/doc.md") == "owner-file"


def test_same_hash_update_does_not_delete_old_version():
    sink = _sink(refs=0)
    sink._upload_file = lambda path: "new-file"
    sink._link_to_kb = lambda kb, fid: None
    deleted = []
    sink._delete_file = lambda fid: deleted.append(fid)
    result = sink.update_datafile(
        "old-file", "/tmp/doc.md",
        previous_hash="same-hash", new_hash="same-hash", datafile_id="df-1",
    )
    assert result == "new-file"
    assert deleted == [], "same-hash update must not delete the old remote file"


def test_changed_hash_update_deletes_old_version():
    sink = _sink(refs=0)
    sink._upload_file = lambda path: "new-file"
    sink._link_to_kb = lambda kb, fid: None
    deleted = []
    sink._delete_file = lambda fid: deleted.append(fid)
    result = sink.update_datafile(
        "old-file", "/tmp/doc.md",
        previous_hash="old-hash", new_hash="new-hash", datafile_id="df-1",
    )
    assert result == "new-file"
    assert deleted == ["old-file"]


def test_shared_remote_id_update_does_not_delete_old_version():
    sink = _sink(refs=2)
    sink._upload_file = lambda path: "new-file"
    sink._link_to_kb = lambda kb, fid: None
    deleted = []
    sink._delete_file = lambda fid: deleted.append(fid)
    result = sink.update_datafile(
        "old-file", "/tmp/doc.md",
        previous_hash="old-hash", new_hash="new-hash", datafile_id="df-1",
    )
    assert result == "new-file"
    assert deleted == [], "shared remote id must not be deleted"


def main():
    for fn in (
        test_find_owner_matches_hash,
        test_orphan_duplicate_is_typed_and_not_deleted,
        test_duplicate_owner_is_reused_without_delete,
        test_same_hash_update_does_not_delete_old_version,
        test_changed_hash_update_deletes_old_version,
        test_shared_remote_id_update_does_not_delete_old_version,
    ):
        fn()
        print(f"  ok: {fn.__name__}")
    print("test_openwebui_duplicate.py: ALL PASSED")


if __name__ == "__main__":
    main()
