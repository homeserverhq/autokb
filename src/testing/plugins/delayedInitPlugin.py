"""Test plugin 12: Delayed init — sleeps before first heartbeat."""

import time

from utils.plugin_base import BaseSubscription
from utils.constants import HEARTBEAT_TIMEOUT


class delayedInitPlugin(BaseSubscription):
    metadata = {
        "name": "delayedInitPlugin",
        "display_name": "delayedInitPlugin",
        "icon": "default_icon.png",
        "description": "Delayed init plugin (Test 12)",
        "sub_type": "SCHEDULED",
    }
    DEFAULT_ACCESS_LEVEL = "PRIVATE"

    def get_schema(self):
        return {"type": "object", "properties": {"label": {"type": "string"}}}

    def getData(self, config, progress_callback):
        # Sleep BEFORE the first user-controlled progress_callback; the
        # wrapper's auto progress_callback(0) covers this period.
        time.sleep(HEARTBEAT_TIMEOUT * 0.4)
        progress_callback(50)
        tmp = "/tmp/delayedInit_output.txt"
        with open(tmp, "w") as f:
            f.write("delayed init completed\n")
        self.move_to_destination(tmp)
        progress_callback(100)
