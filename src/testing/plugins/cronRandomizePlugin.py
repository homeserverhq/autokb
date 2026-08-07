"""Test plugin 26: Cron randomization — verifies default cron strings are randomized at creation."""

import time

from utils.plugin_base import BaseSubscription


class cronRandomizePlugin(BaseSubscription):
    metadata = {
        "name": "cronRandomizePlugin",
        "display_name": "cronRandomizePlugin",
        "icon": "default_icon.png",
        "description": "Cron randomization test (Test 26)",
        "sub_type": "SCHEDULED",
    }
    DEFAULT_ACCESS_LEVEL = "PRIVATE"

    def get_schema(self):
        return {
            "type": "object",
            "properties": {
                "label": {"type": "string", "minLength": 1, "maxLength": 50},
            },
            "required": ["label"],
        }

    def getData(self, config, progress_callback):
        progress_callback(50)
        time.sleep(0.05)
        progress_callback(100)
