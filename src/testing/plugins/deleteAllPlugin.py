"""Test plugin 25: Delete-all — used by test_delete_subscription_and_plugin."""

import os
import time

from utils.plugin_base import BaseSubscription


class deleteAllPlugin(BaseSubscription):
    metadata = {
        "name": "deleteAllPlugin",
        "display_name": "deleteAllPlugin",
        "icon": "default_icon.png",
        "description": "Delete-all test plugin (Test 25)",
        "sub_type": "SCHEDULED",
    }
    DEFAULT_ACCESS_LEVEL = "PUBLIC"

    def get_schema(self):
        return {
            "type": "object",
            "properties": {
                "label": {"type": "string", "minLength": 1, "maxLength": 100},
            },
            "required": ["label"],
        }

    def getData(self, config, progress_callback):
        progress_callback(25)
        tmp = "/tmp/deleteAllPlugin_output.txt"
        with open(tmp, "w") as f:
            f.write(f"deleteAllPlugin output — label={config.get('label', '')}\n")
            time.sleep(0.05)
            f.flush()
        progress_callback(50)
        time.sleep(0.05)
        progress_callback(75)
        time.sleep(0.05)
        self.move_to_destination(tmp)
        progress_callback(100)
