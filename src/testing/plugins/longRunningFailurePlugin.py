"""Test plugin 5: Long running then RuntimeError."""

import time

from utils.plugin_base import BaseSubscription
from utils.constants import HEARTBEAT_TIMEOUT


class longRunningFailurePlugin(BaseSubscription):
    metadata = {
        "name": "longRunningFailurePlugin",
        "display_name": "longRunningFailurePlugin",
        "icon": "default_icon.png",
        "description": "Long running failure plugin (Test 5)",
        "sub_type": "SCHEDULED",
    }
    DEFAULT_ACCESS_LEVEL = "PRIVATE"

    def get_schema(self):
        return {
            "type": "object",
            "properties": {
                "fail_at": {"type": "integer", "minimum": 1, "maximum": 100},
            },
        }

    def getData(self, config, progress_callback):
        sleep_total = HEARTBEAT_TIMEOUT
        interval = HEARTBEAT_TIMEOUT / 10
        iterations = max(int(sleep_total / interval), 1)
        for i in range(iterations):
            time.sleep(interval)
            progress_callback(int(100 * (i + 1) / iterations))
        raise RuntimeError("Long running plugin failed at the end of its run")
