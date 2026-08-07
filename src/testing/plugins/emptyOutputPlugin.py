"""Test plugin 10: Empty output — no move_to_destination call."""

from utils.plugin_base import BaseSubscription


class emptyOutputPlugin(BaseSubscription):
    metadata = {
        "name": "emptyOutputPlugin",
        "display_name": "emptyOutputPlugin",
        "icon": "default_icon.png",
        "description": "Empty output plugin (Test 10)",
        "sub_type": "SCHEDULED",
    }
    DEFAULT_ACCESS_LEVEL = "PRIVATE"

    def get_schema(self):
        return {
            "type": "object",
            "properties": {"marker": {"type": "string"}},
        }

    def getData(self, config, progress_callback):
        progress_callback(50)
        # Intentionally do NOT write any files
        progress_callback(100)
