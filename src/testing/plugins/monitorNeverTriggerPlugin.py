"""Test plugin 15: Monitor never triggers — relies on fallback cron."""

import os

from utils.plugin_base import BaseSubscription


class monitorNeverTriggerPlugin(BaseSubscription):
    metadata = {
        "name": "monitorNeverTriggerPlugin",
        "display_name": "monitorNeverTriggerPlugin",
        "description": "Monitor never triggers — cron fallback (Test 15)",
        "sub_type": "EVENT_BASED",
    }
    DEFAULT_ACCESS_LEVEL = "PRIVATE"

    def get_schema(self):
        return {"type": "object", "properties": {"marker": {"type": "string"}}}

    async def monitor(self, config, cancel_token):
        return False  # never trigger

    def getData(self, config, progress_callback):
        progress_callback(50)
        tmp = "/tmp/monitorNeverTrigger_output.txt"
        with open(tmp, "w") as f:
            f.write("Fallback cron run\n")
        self.move_to_destination(tmp)
        progress_callback(100)
