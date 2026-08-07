"""Test plugin 3: No heartbeat → ERROR after HEARTBEAT_TIMEOUT."""

import time

from utils.plugin_base import BaseSubscription
from utils.constants import HEARTBEAT_TIMEOUT


class noHeartbeatPlugin(BaseSubscription):
    metadata = {
        "name": "noHeartbeatPlugin",
            "display_name": "noHeartbeatPlugin",
        "icon": "default_icon.png",
        "description": "Heartbeat timeout plugin (Test 3)",
        "sub_type": "SCHEDULED",
    }
    DEFAULT_ACCESS_LEVEL = "PRIVATE"

    def get_schema(self):
        return {
            "type": "object",
            "properties": {"label": {"type": "string"}},
        }

    def getData(self, config, progress_callback):
        # Sleep longer than HEARTBEAT_TIMEOUT, never calling progress_callback
        time.sleep(HEARTBEAT_TIMEOUT * 2)
        # If we ever get here, write a file and finish
        tmp = "/tmp/noHeartbeat_output.txt"
        with open(tmp, "w") as f:
            f.write("should not be reached\n")
        self.move_to_destination(tmp)
