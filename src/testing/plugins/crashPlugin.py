"""Test plugin 6: Crash — immediate exception."""

from utils.plugin_base import BaseSubscription


class crashPlugin(BaseSubscription):
    metadata = {
        "name": "crashPlugin",
        "display_name": "crashPlugin",
        "description": "Immediate crash plugin (Test 6)",
        "sub_type": "SCHEDULED",
    }

    def get_schema(self):
        return {
            "type": "object",
            "properties": {"reason": {"type": "string"}},
        }

    def getData(self, config, progress_callback):
        progress_callback(10)
        raise Exception("Something went wrong")
