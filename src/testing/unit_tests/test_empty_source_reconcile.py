"""Regression tests for empty-source reconciliation.

Runnable directly: ``python /src/testing/unit_tests/test_empty_source_reconcile.py``.
"""

import hashlib
import json
import os
import sys
import tempfile
import types
from pathlib import Path

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, SRC)

# The unit-test CI job only installs structlog + cryptography. These plugins
# import requests/tiktoken at module scope but do not use them in these tests.
try:
    import requests  # noqa: F401
except ImportError:
    requests_stub = types.ModuleType("requests")

    class RequestException(Exception):
        pass

    class HTTPError(RequestException):
        pass

    requests_stub.RequestException = RequestException
    requests_stub.HTTPError = HTTPError
    requests_stub.get = lambda *args, **kwargs: None
    sys.modules["requests"] = requests_stub

try:
    import tiktoken  # noqa: F401
except ImportError:
    tiktoken_stub = types.ModuleType("tiktoken")

    class _Encoding:
        def encode(self, _text):
            return []

    tiktoken_stub.get_encoding = lambda _name: _Encoding()
    sys.modules["tiktoken"] = tiktoken_stub

from utils.plugin_loading import find_plugin_subclass, load_plugin_module


def _load_plugin(filename):
    path = os.path.join(SRC, "builtin_plugins", filename)
    module = load_plugin_module(path)
    cls = find_plugin_subclass(module)
    if cls is None:
        raise AssertionError(f"no plugin subclass found in {filename}")
    return module, cls


def test_paperless_empty_filter_removes_output():
    _, cls = _load_plugin("ePaperlessDoclingPlugin.py")
    with tempfile.TemporaryDirectory() as td:
        plugin = cls()
        plugin._subscription_name = "unit-sub"
        plugin.get_destination_path = lambda: td
        plugin._query_documents = lambda *args, **kwargs: []

        stale = Path(td) / "32.cf2d32af4c6dee9e.md"
        stale.write_text("stale", encoding="utf-8")

        progress = []
        plugin._run({}, lambda pct, message=None: progress.append((pct, message)))

        assert not stale.exists(), "stale Paperless output was not removed"
        assert progress[-1][0] == 100


def test_crawl_empty_results_removes_output():
    _, cls = _load_plugin("crawl4AIWebScraperPlugin.py")
    url = "https://example.com"
    uh = hashlib.sha256(url.encode()).hexdigest()[:16]

    with tempfile.TemporaryDirectory() as td:
        plugin = cls()
        plugin._subscription_name = "unit-sub"
        plugin.get_destination_path = lambda: td
        plugin._submit_job = lambda *args, **kwargs: "task"
        plugin._poll_job = lambda *args, **kwargs: []

        stale = Path(td) / f"{uh}.{'a' * 16}.md"
        stale.write_text("stale", encoding="utf-8")

        plugin.getData({"url": url}, lambda pct, message=None: None)

        assert not stale.exists(), "stale crawl output was not removed"


def test_crawl_all_failed_keeps_output():
    _, cls = _load_plugin("crawl4AIWebScraperPlugin.py")
    url = "https://example.com"
    uh = hashlib.sha256(url.encode()).hexdigest()[:16]

    with tempfile.TemporaryDirectory() as td:
        plugin = cls()
        plugin._subscription_name = "unit-sub"
        plugin.get_destination_path = lambda: td
        plugin._submit_job = lambda *args, **kwargs: "task"
        plugin._poll_job = lambda *args, **kwargs: [
            {"success": False, "url": url, "error_message": "boom"}
        ]

        stale = Path(td) / f"{uh}.{'a' * 16}.md"
        stale.write_text("stale", encoding="utf-8")

        plugin.getData({"url": url}, lambda pct, message=None: None)

        assert stale.exists(), "all-failed crawl must not delete existing output"


def test_crawl_poll_rejects_malformed_results():
    module, cls = _load_plugin("crawl4AIWebScraperPlugin.py")

    class _Resp:
        status_code = 200

        def raise_for_status(self):
            return None

        def json(self):
            return {"status": "completed", "result": "{}"}

    old_get = module.requests.get
    module.requests.get = lambda *args, **kwargs: _Resp()
    try:
        plugin = cls()
        try:
            plugin._poll_job("task", lambda *args, **kwargs: None)
        except RuntimeError:
            return
        raise AssertionError("expected RuntimeError for malformed results payload")
    finally:
        module.requests.get = old_get


def test_crawl_poll_accepts_empty_results():
    module, cls = _load_plugin("crawl4AIWebScraperPlugin.py")

    class _Resp:
        status_code = 200

        def raise_for_status(self):
            return None

        def json(self):
            return {"status": "completed", "result": json.dumps({"results": []})}

    old_get = module.requests.get
    module.requests.get = lambda *args, **kwargs: _Resp()
    try:
        plugin = cls()
        assert plugin._poll_job("task", lambda *args, **kwargs: None) == []
    finally:
        module.requests.get = old_get


def test_fastcrw_empty_results_removes_output():
    _, cls = _load_plugin("fastCRWWebScraperPlugin.py")
    url = "https://example.com"
    uh = hashlib.sha256(url.encode()).hexdigest()[:16]

    with tempfile.TemporaryDirectory() as td:
        plugin = cls()
        plugin._subscription_name = "unit-sub"
        plugin.get_destination_path = lambda: td
        plugin._submit_job = lambda *args, **kwargs: "job"
        plugin._poll_job = lambda *args, **kwargs: []

        stale = Path(td) / f"{uh}.{'a' * 16}.md"
        stale.write_text("stale", encoding="utf-8")

        plugin.getData({"url": url}, lambda pct, message=None: None)

        assert not stale.exists(), "stale fastCRW output was not removed"


def test_fastcrw_all_blocked_keeps_output():
    _, cls = _load_plugin("fastCRWWebScraperPlugin.py")
    url = "https://example.com"
    uh = hashlib.sha256(url.encode()).hexdigest()[:16]

    with tempfile.TemporaryDirectory() as td:
        plugin = cls()
        plugin._subscription_name = "unit-sub"
        plugin.get_destination_path = lambda: td
        plugin._submit_job = lambda *args, **kwargs: "job"
        plugin._poll_job = lambda *args, **kwargs: [
            {
                "metadata": {"sourceURL": url},
                "block": {"vendor": "cloudflare", "reason": "challenge"},
                "markdown": "wall",
            }
        ]

        stale = Path(td) / f"{uh}.{'a' * 16}.md"
        stale.write_text("stale", encoding="utf-8")

        plugin.getData({"url": url}, lambda pct, message=None: None)

        assert stale.exists(), (
            "all-blocked fastCRW crawl must not delete existing output"
        )


def test_fastcrw_flat_scrapedata_parses_and_writes():
    _, cls = _load_plugin("fastCRWWebScraperPlugin.py")
    url = "https://example.com/page"
    bad_url = "https://example.com/bad"
    uh = hashlib.sha256(url.encode()).hexdigest()[:16]

    with tempfile.TemporaryDirectory() as td:
        plugin = cls()
        plugin._subscription_name = "unit-sub"
        plugin.get_destination_path = lambda: td

        def _move(path):
            target = os.path.join(td, os.path.basename(path))
            os.replace(path, target)
            return target

        plugin.move_to_destination = _move
        plugin._submit_job = lambda *args, **kwargs: "job"
        plugin._poll_job = lambda *args, **kwargs: [
            {"markdown": "# Hello", "metadata": {"sourceURL": url}},
            {
                "markdown": "wall",
                "metadata": {"sourceURL": bad_url},
                "block": {"vendor": "cloudflare", "reason": "challenge"},
            },
        ]

        plugin.getData({"url": url}, lambda pct, message=None: None)

        files = sorted(os.listdir(td))
        assert len(files) == 1, f"expected one output file, got {files}"
        assert files[0].startswith(f"{uh}."), files[0]
        assert files[0].endswith(".md"), files[0]
        assert (
            Path(td, files[0])
            .read_text(encoding="utf-8")
            .startswith(f"Original URL: {url}")
        )


def test_fastcrw_poll_rejects_malformed_data():
    module, cls = _load_plugin("fastCRWWebScraperPlugin.py")

    class _Resp:
        status_code = 200

        def raise_for_status(self):
            return None

        def json(self):
            return {"success": True, "status": "completed", "data": None}

    old_get = module.requests.get
    module.requests.get = lambda *args, **kwargs: _Resp()
    try:
        plugin = cls()
        try:
            plugin._poll_job("job", lambda *args, **kwargs: None)
        except RuntimeError:
            return
        raise AssertionError("expected RuntimeError for malformed result data")
    finally:
        module.requests.get = old_get


def test_fastcrw_poll_accepts_empty_data():
    module, cls = _load_plugin("fastCRWWebScraperPlugin.py")

    class _Resp:
        status_code = 200

        def raise_for_status(self):
            return None

        def json(self):
            return {"success": True, "status": "completed", "data": []}

    old_get = module.requests.get
    module.requests.get = lambda *args, **kwargs: _Resp()
    try:
        plugin = cls()
        assert plugin._poll_job("job", lambda *args, **kwargs: None) == []
    finally:
        module.requests.get = old_get


def test_fastcrw_poll_rejects_cancelled_job():
    module, cls = _load_plugin("fastCRWWebScraperPlugin.py")

    class _Resp:
        status_code = 200

        def raise_for_status(self):
            return None

        def json(self):
            return {"success": True, "status": "cancelled", "error": "stopped"}

    old_get = module.requests.get
    module.requests.get = lambda *args, **kwargs: _Resp()
    try:
        plugin = cls()
        try:
            plugin._poll_job("job", lambda *args, **kwargs: None)
        except RuntimeError:
            return
        raise AssertionError("expected RuntimeError for cancelled crawl job")
    finally:
        module.requests.get = old_get


def test_fastcrw_api_key_config_overrides_env():
    module, _ = _load_plugin("fastCRWWebScraperPlugin.py")
    old_key = os.environ.get("FASTCRW_API_KEY")
    try:
        os.environ["FASTCRW_API_KEY"] = "env-key"
        assert module._resolve_api_key({}) == "env-key"
        assert module._resolve_api_key({"api_key": "sub-key"}) == "sub-key"
        assert module._api_headers("sub-key") == {"Authorization": "Bearer sub-key"}
        assert module._api_headers("") == {}
    finally:
        if old_key is None:
            os.environ.pop("FASTCRW_API_KEY", None)
        else:
            os.environ["FASTCRW_API_KEY"] = old_key


def test_fastcrw_submit_sends_bearer_header():
    module, cls = _load_plugin("fastCRWWebScraperPlugin.py")
    captured = {}

    class _Resp:
        status_code = 200

        def raise_for_status(self):
            return None

        def json(self):
            return {"success": True, "id": "job-1"}

    def _post(url, json=None, headers=None, timeout=None):
        captured["url"] = url
        captured["json"] = json
        captured["headers"] = headers
        return _Resp()

    old_post = module.requests.post
    module.requests.post = _post
    try:
        plugin = cls()
        assert (
            plugin._submit_job("https://example.com", 10, 1000, "secret-key") == "job-1"
        )
        assert captured["headers"] == {"Authorization": "Bearer secret-key"}
        assert captured["json"]["maxDepth"] == 10
        assert captured["json"]["maxPages"] == 1000
        assert captured["json"]["formats"] == ["markdown"]
    finally:
        module.requests.post = old_post


def main():
    for fn in (
        test_paperless_empty_filter_removes_output,
        test_crawl_empty_results_removes_output,
        test_crawl_all_failed_keeps_output,
        test_crawl_poll_rejects_malformed_results,
        test_crawl_poll_accepts_empty_results,
        test_fastcrw_empty_results_removes_output,
        test_fastcrw_all_blocked_keeps_output,
        test_fastcrw_flat_scrapedata_parses_and_writes,
        test_fastcrw_poll_rejects_malformed_data,
        test_fastcrw_poll_accepts_empty_data,
        test_fastcrw_poll_rejects_cancelled_job,
        test_fastcrw_api_key_config_overrides_env,
        test_fastcrw_submit_sends_bearer_header,
    ):
        fn()
        print(f"  ok: {fn.__name__}")
    print("test_empty_source_reconcile.py: ALL PASSED")


if __name__ == "__main__":
    main()
