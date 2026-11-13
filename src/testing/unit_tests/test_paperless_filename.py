"""Unit tests for Paperless output filenames and reconciliation."""

import os
import sys
import tempfile
import types

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
    requests_stub.post = lambda *args, **kwargs: None
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

CHECKSUM = "cf2d32af4c6dee9e" + ("a" * 48)
HASH = CHECKSUM[:16]


def _load_plugin():
    path = os.path.join(
        SRC, "builtin_plugins", "ePaperlessDoclingPlugin.py"
    )
    module = load_plugin_module(path)
    cls = find_plugin_subclass(module)
    if cls is None:
        raise AssertionError("no plugin subclass found")
    return cls


def _doc(doc_id=32, name="myfile.pdf", checksum=CHECKSUM):
    return {
        "id": doc_id,
        "original_file_name": name,
        "title": name,
        "versions": [{"checksum": checksum}],
    }


def _make_plugin(cls, td, docs, chunks=None):
    plugin = cls()
    plugin._subscription_name = "unit-sub"
    plugin.get_destination_path = lambda: td
    plugin._query_documents = lambda *args, **kwargs: docs
    plugin._fetch_content = lambda *args, **kwargs: "body"

    if chunks is None:
        chunks = [{"chunk_index": 0, "text": "body"}]
    plugin._chunks_from_content = lambda *args, **kwargs: list(chunks)

    def _move(path):
        target = os.path.join(td, os.path.basename(path))
        os.replace(path, target)
        return target

    plugin.move_to_destination = _move
    return plugin


def _run(plugin, config=None):
    plugin._run(config or {}, lambda pct, message=None: None)


def _files(td):
    return set(os.listdir(td))


def test_output_includes_original_name_doc_id_and_hash():
    cls = _load_plugin()
    with tempfile.TemporaryDirectory() as td:
        plugin = _make_plugin(cls, td, [_doc()])
        _run(plugin)
        assert _files(td) == {f"myfile.32.{HASH}.md"}


def test_chunked_output_uses_hash_then_chunk_id():
    cls = _load_plugin()
    chunks = [
        {"chunk_index": 0, "text": "one"},
        {"chunk_index": 1, "text": "two"},
    ]
    with tempfile.TemporaryDirectory() as td:
        plugin = _make_plugin(cls, td, [_doc()], chunks=chunks)
        _run(plugin, {"chunking_enabled": True})
        assert _files(td) == {
            f"myfile.32.{HASH}-000.md",
            f"myfile.32.{HASH}-001.md",
        }


def test_raw_pdf_uses_original_extension():
    cls = _load_plugin()
    with tempfile.TemporaryDirectory() as td:
        plugin = _make_plugin(cls, td, [_doc()])
        plugin._download_document = lambda *args, **kwargs: b"%PDF-1.4"
        _run(plugin, {"processing_mode": "Raw PDF"})
        assert _files(td) == {f"myfile.32.{HASH}.pdf"}


def test_legacy_doc_id_name_is_replaced():
    cls = _load_plugin()
    with tempfile.TemporaryDirectory() as td:
        legacy = os.path.join(td, f"32.{HASH}.md")
        with open(legacy, "w", encoding="utf-8") as f:
            f.write("legacy")

        plugin = _make_plugin(cls, td, [_doc()])
        _run(plugin)

        assert not os.path.exists(legacy)
        assert _files(td) == {f"myfile.32.{HASH}.md"}


def test_filename_change_repulls_and_removes_old_name():
    cls = _load_plugin()
    with tempfile.TemporaryDirectory() as td:
        old = os.path.join(td, f"oldname.32.{HASH}.md")
        with open(old, "w", encoding="utf-8") as f:
            f.write("old")

        plugin = _make_plugin(cls, td, [_doc(name="newname.pdf")])
        _run(plugin)

        assert not os.path.exists(old)
        assert _files(td) == {f"newname.32.{HASH}.md"}


def test_matching_file_is_skipped_without_fetching():
    cls = _load_plugin()
    with tempfile.TemporaryDirectory() as td:
        name = f"myfile.32.{HASH}.md"
        with open(os.path.join(td, name), "w", encoding="utf-8") as f:
            f.write("already current")

        plugin = _make_plugin(cls, td, [_doc()])

        def _unexpected(*args, **kwargs):
            raise AssertionError("matching output should not be re-fetched")

        plugin._fetch_content = _unexpected
        plugin._download_document = _unexpected
        plugin._chunks_from_content = _unexpected
        _run(plugin)

        assert _files(td) == {name}
        with open(os.path.join(td, name), encoding="utf-8") as f:
            assert f.read() == "already current"


def test_duplicate_names_with_same_checksum_get_distinct_files():
    cls = _load_plugin()
    docs = [_doc(doc_id=32), _doc(doc_id=33)]
    with tempfile.TemporaryDirectory() as td:
        plugin = _make_plugin(cls, td, docs)
        _run(plugin)
        assert _files(td) == {
            f"myfile.32.{HASH}.md",
            f"myfile.33.{HASH}.md",
        }


def test_parser_handles_dots_in_original_name_and_legacy_names():
    cls = _load_plugin()
    assert cls._parse_disk_name(
        f"my.file.32.{HASH}-001.md"
    ) == ("my.file", 32, HASH, True, "md")
    assert cls._parse_disk_name(f"32.{HASH}.md") == (
        None,
        32,
        HASH,
        False,
        "md",
    )


def test_long_original_name_is_truncated_before_fixed_suffix():
    cls = _load_plugin()
    long_name = ("a" * 300) + ".pdf"
    with tempfile.TemporaryDirectory() as td:
        plugin = _make_plugin(cls, td, [_doc(name=long_name)])
        _run(plugin)
        files = _files(td)
        assert len(files) == 1
        name = files.pop()
        assert len(name) <= 240
        assert name.endswith(f".32.{HASH}.md")


def test_original_name_is_sanitized():
    cls = _load_plugin()
    with tempfile.TemporaryDirectory() as td:
        plugin = _make_plugin(cls, td, [_doc(name="My File (1).pdf")])
        _run(plugin)
        assert _files(td) == {f"MyFile1.32.{HASH}.md"}


def main():
    for fn in (
        test_output_includes_original_name_doc_id_and_hash,
        test_chunked_output_uses_hash_then_chunk_id,
        test_raw_pdf_uses_original_extension,
        test_legacy_doc_id_name_is_replaced,
        test_filename_change_repulls_and_removes_old_name,
        test_matching_file_is_skipped_without_fetching,
        test_duplicate_names_with_same_checksum_get_distinct_files,
        test_parser_handles_dots_in_original_name_and_legacy_names,
        test_long_original_name_is_truncated_before_fixed_suffix,
        test_original_name_is_sanitized,
    ):
        fn()
        print(f"  ok: {fn.__name__}")
    print("test_paperless_filename.py: ALL PASSED")


if __name__ == "__main__":
    main()
