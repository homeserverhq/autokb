"""Test plugin 18: sys.exit(1) without Python exception."""

import sys

from utils.plugin_base import BaseSubscription


class nonZeroExitPlugin(BaseSubscription):
    metadata = {
        "name": "nonZeroExitPlugin",
        "icon": "default_icon.png",
        "description": "Bare sys.exit(1) plugin (Test 18)",
        "sub_type": "SCHEDULED",
    }
    DEFAULT_ACCESS_LEVEL = "PRIVATE"

    def get_schema(self):
        return {"type": "object", "properties": {"label": {"type": "string"}}}

    def getData(self, config, progress_callback):
        progress_callback(50)
        sys.exit(1)
