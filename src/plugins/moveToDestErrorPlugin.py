"""Test plugin 20: move_to_destination(".") → ValueError."""

from utils.plugin_base import BaseSubscription


class moveToDestErrorPlugin(BaseSubscription):
    metadata = {
        "name": "moveToDestErrorPlugin",
        "icon": "default_icon.png",
        "description": "Invalid output path plugin (Test 20)",
        "sub_type": "SCHEDULED",
    }
    DEFAULT_ACCESS_LEVEL = "PRIVATE"

    def get_schema(self):
        return {"type": "object", "properties": {"label": {"type": "string"}}}

    def getData(self, config, progress_callback):
        progress_callback(50)
        # sanitize_name(".") raises ValueError — "first/last char cannot be period"
        self.move_to_destination(".")
