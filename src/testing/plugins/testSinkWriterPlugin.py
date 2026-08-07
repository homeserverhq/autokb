"""Test plugin for Sink end-to-end tests.

Writes two files to the output directory with known content.
"""

import os
from typing import Any, Callable, Dict

from utils.misc_utils import SubscriptionCancelledError
from utils.plugin_base import BaseSubscription


class TestSinkWriterPlugin(BaseSubscription):
    metadata = {
        "name": "testSinkWriterPlugin",
        "icon": "default_icon.png",
        "description": "Writes test files for Sink e2e tests.",
        "sub_type": "SCHEDULED",
    }

    required_config_keys = []

    def get_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {},
            "required": [],
        }

    def getData(self, config: Dict[str, Any], progress_callback: Callable[[int], None]) -> None:
        tmp1 = f"/tmp/{self._subscription_id}_hello.md"
        tmp2 = f"/tmp/{self._subscription_id}_world.md"
        with open(tmp1, "w") as f:
            f.write("# Hello\n\nContent from testSinkWriterPlugin.\n")
        progress_callback(50)
        with open(tmp2, "w") as f:
            f.write("# World\n\nMore test content.\n")
        progress_callback(100)
        self.move_to_destination(tmp1)
        self.move_to_destination(tmp2)


__all__ = ["TestSinkWriterPlugin"]
