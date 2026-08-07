"""Test Sink service for end-to-end testing.

Records every abstract method call to a JSON-lines file at
/output/.sink_e2e_calls.json so the test harness can read it after recon.
"""

import json
import os

from utils.sink_base import BaseSink


_CALLS_FILE = "/output/.sink_e2e_calls.json"


def _record(method: str, *args):
    os.makedirs(os.path.dirname(_CALLS_FILE), exist_ok=True)
    with open(_CALLS_FILE, "a") as f:
        f.write(json.dumps([method] + list(args), separators=(",", ":")) + "\n")


class TestSink(BaseSink):
    metadata = {
        "name": "testSink",
        "display_name": "testSink",
        "description": "End-to-end test Sink service",
        "icon": "default_icon.png",
    }

    def _file_name(self, path: str) -> str:
        return os.path.basename(path)

    def add_datafile(self, path: str) -> str:
        rid = self._file_name(path)
        _record("add_datafile", path, rid)
        return rid

    def update_datafile(self, remote_datafile_id: str, path: str) -> str:
        new_rid = self._file_name(path)
        _record("update_datafile", remote_datafile_id, path, new_rid)
        return new_rid

    def remove_datafile(self, remote_datafile_id: str) -> None:
        _record("remove_datafile", remote_datafile_id)

    def add_target(self) -> str:
        rid = "test-remote-target-id"
        _record("add_target", rid)
        return rid

    def remove_target(self) -> None:
        _record("remove_target")

    def clear_target(self) -> None:
        _record("clear_target")


__all__ = ["TestSink"]
