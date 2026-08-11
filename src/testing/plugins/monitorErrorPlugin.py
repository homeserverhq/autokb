"""Test plugin 16: Monitor raises ConnectionError — retry loop."""

from utils.plugin_base import BaseSubscription


class monitorErrorPlugin(BaseSubscription):
    metadata = {
        "name": "monitorErrorPlugin",
        "display_name": "monitorErrorPlugin",
        "description": "Monitor exception retry loop (Test 16)",
        "sub_type": "EVENT_BASED",
    }

    def get_schema(self):
        return {"type": "object", "properties": {"label": {"type": "string"}}}

    async def monitor(self, config, cancel_token):
        raise ConnectionError("simulated monitor failure")

    def getData(self, config, progress_callback):
        progress_callback(100)
