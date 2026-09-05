"""Shared plugin-loader checks (R22).

Runnable directly: ``python /src/testing/unit_tests/test_plugin_loading.py``.
"""

import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from utils.plugin_loading import find_plugin_subclass, load_plugin_class, load_plugin_module

_PLUGIN_SRC = '''\
from utils.plugin_base import BaseSubscription


class SamplePlugin(BaseSubscription):
    metadata = {
        "name": "SamplePlugin",
        "description": "unit-test fixture",
        "sub_type": "SCHEDULED",
    }

    def get_schema(self):
        return {"type": "object", "properties": {"x": {"type": "string"}}}

    def getData(self, config, progress_callback):
        progress_callback(100)
'''


def test_load_plugin_class():
    with tempfile.NamedTemporaryFile(suffix=".py", mode="w", delete=False) as f:
        f.write(_PLUGIN_SRC)
        path = f.name
    try:
        cls = load_plugin_class(path)
        assert cls.__name__ == "SamplePlugin"
        got = cls()
        assert got.metadata["sub_type"] == "SCHEDULED"
        # find_plugin_subclass on a loaded module
        module = load_plugin_module(path, name="sample_plugin_mod")
        found = find_plugin_subclass(module)
        assert found is not None and found.__name__ == "SamplePlugin"
    finally:
        os.unlink(path)


def test_missing_subclass_raises():
    with tempfile.NamedTemporaryFile(suffix=".py", mode="w", delete=False) as f:
        f.write("# no subclass here\nvalue = 1\n")
        path = f.name
    try:
        try:
            load_plugin_class(path)
        except RuntimeError:
            return
        raise AssertionError("expected RuntimeError for plugin without a subclass")
    finally:
        os.unlink(path)


def main():
    for fn in (test_load_plugin_class, test_missing_subclass_raises):
        fn()
        print(f"  ok: {fn.__name__}")
    print("test_plugin_loading.py: ALL PASSED")


if __name__ == "__main__":
    main()