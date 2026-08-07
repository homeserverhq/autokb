"""Test plugin 7: Cancellation — exits gracefully when DISABLED.

This plugin relies on the worker's behaviour: when the user sets the
subscription to DISABLED while IN_PROGRESS, the worker's child process
sees the status flip in the next progress_callback() invocation and
raises ``SubscriptionCancelledError`` → exit code 0 → no EventLog entry.

For E2E testing, we set the status to DISABLED programmatically via
the test runner, so we don't need a separate trigger.
"""

import time

from utils.plugin_base import BaseSubscription
from utils.constants import HEARTBEAT_TIMEOUT


class cancellationPlugin(BaseSubscription):
    metadata = {
        "name": "cancellationPlugin",
        "display_name": "cancellationPlugin",
        "icon": "cancellationPlugin.png",
        "description": "Graceful cancellation plugin (Test 7)",
        "sub_type": "SCHEDULED",
    }
    DEFAULT_ACCESS_LEVEL = "PRIVATE"

    def get_schema(self):
        return {
            "type": "object",
            "properties": {"iterations": {"type": "integer", "minimum": 1, "maximum": 10000}},
        }

    def getData(self, config, progress_callback):
        iterations = int(config.get("iterations", 50))
        interval = HEARTBEAT_TIMEOUT / 30
        for i in range(iterations):
            time.sleep(interval)
            progress_callback(int(100 * (i + 1) / iterations))
        tmp = "/tmp/cancellation_output.txt"
        with open(tmp, "w") as f:
            f.write("Completed all iterations\n")
        self.move_to_destination(tmp)
        progress_callback(100)
