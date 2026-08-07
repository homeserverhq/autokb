"""Test plugin 1: Happy path — normal success."""

import os
import time

from utils.plugin_base import BaseSubscription


class happyPathPlugin(BaseSubscription):
    metadata = {
        "name": "happyPathPlugin",
            "display_name": "happyPathPlugin",
        "icon": "default_icon.png",
        "description": "Normal success path plugin (Test 1)",
        "sub_type": "SCHEDULED",
    }
    DEFAULT_ACCESS_LEVEL = "PUBLIC"

    def get_schema(self):
        return {
            "type": "object",
            "properties": {
                "title": {"type": "string", "minLength": 1, "maxLength": 100},
            },
            "required": ["title"],
        }

    def getData(self, config, progress_callback):
        progress_callback(25)
        # Write a small file
        tmp = "/tmp/happyPathPlugin_output.txt"
        with open(tmp, "w") as f:
            f.write(f"Hello from happyPathPlugin — title={config.get('title', '')}\n")
            time.sleep(0.05)
            f.flush()
        progress_callback(50)
        time.sleep(0.05)
        progress_callback(75)
        time.sleep(0.05)
        self.move_to_destination(tmp)
        progress_callback(100)
