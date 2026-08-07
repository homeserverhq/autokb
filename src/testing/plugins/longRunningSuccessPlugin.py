"""Test plugin 4: Long running with regular heartbeats → success."""

import os
import time

from utils.plugin_base import BaseSubscription
from utils.constants import HEARTBEAT_TIMEOUT


class longRunningSuccessPlugin(BaseSubscription):
    metadata = {
        "name": "longRunningSuccessPlugin",
            "display_name": "longRunningSuccessPlugin",
        "icon": "default_icon.png",
        "description": "Long running success plugin (Test 4)",
        "sub_type": "SCHEDULED",
    }
    DEFAULT_ACCESS_LEVEL = "PRIVATE"

    def get_schema(self):
        return {
            "type": "object",
            "properties": {
                "name": {"type": "string", "minLength": 1},
            },
            "required": ["name"],
        }

    def getData(self, config, progress_callback):
        # Total runtime ~ HEARTBEAT_TIMEOUT * 2 (very fast in test mode)
        sleep_total = HEARTBEAT_TIMEOUT * 2
        interval = HEARTBEAT_TIMEOUT / 10
        iterations = max(int(sleep_total / interval), 1)
        for i in range(iterations):
            time.sleep(interval)
            progress_callback(int(100 * (i + 1) / iterations))
        # Write output file
        tmp = "/tmp/longRunningSuccess_output.txt"
        with open(tmp, "w") as f:
            f.write(f"(EDIT TEST)Completed long running success for {config.get('name', '')}\n")
        self.move_to_destination(tmp)
        progress_callback(100)
