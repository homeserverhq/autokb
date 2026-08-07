"""Test plugin with a maximum-length (32 char) name — exercises the
plugin grid layout when names are at the upper end of the allowed
length."""

import os
import time

from utils.plugin_base import BaseSubscription


class longNamePlugin32CharNameForUITes(BaseSubscription):
    metadata = {
        "name": "longNamePlugin32CharNameForUITes",
        "display_name": "longNamePlugin32CharNameForUITes",
        "icon": "default_icon.png",
        "description": "32-char name plugin (UI tile layout test)",
        "sub_type": "SCHEDULED",
    }
    DEFAULT_ACCESS_LEVEL = "PRIVATE"

    def get_schema(self):
        return {
            "type": "object",
            "properties": {
                "label": {"type": "string", "minLength": 1, "maxLength": 100},
            },
            "required": ["label"],
        }

    def getData(self, config, progress_callback):
        progress_callback(50)
        tmp = "/tmp/longNamePlugin32CharNameForUITes_output.txt"
        with open(tmp, "w") as f:
            f.write(f"label={config.get('label', '')}\n")
        time.sleep(0.05)
        self.move_to_destination(tmp)
        progress_callback(100)
