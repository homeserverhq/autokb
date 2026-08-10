"""Test plugin: EVENT_BASED — fires immediately on enable, then every 42 seconds."""

import time

from utils.plugin_base import BaseSubscription


class eventOftenPlugin(BaseSubscription):
    metadata = {
        "name": "eventOftenPlugin",
        "display_name": "eventOftenPlugin",
        "description": "EVENT_BASED — fires on enable, then every 42s",
        "sub_type": "EVENT_BASED",
    }
    DEFAULT_ACCESS_LEVEL = "PRIVATE"

    FIRE_INTERVAL_SECONDS = 42

    def __init__(self):
        super().__init__()
        # Instance state — fresh per monitor task (scheduler calls
        # rec.cls() to create a new instance for each monitor lifetime).
        # _last_fire is None until the first call fires, then holds the
        # wall-clock time of the most recent fire.
        self._last_fire = None

    def get_schema(self):
        return {
            "type": "object",
            "properties": {
                "topic": {"type": "string", "minLength": 1},
            },
            "required": ["topic"],
        }

    async def monitor(self, config, cancel_token):
        now = time.time()
        if self._last_fire is None:
            # First call after enable: fire immediately.
            self._last_fire = now
            return True
        if now - self._last_fire >= self.FIRE_INTERVAL_SECONDS:
            self._last_fire = now
            return True
        return False

    def getData(self, config, progress_callback):
        progress_callback(50)
        tmp = "/tmp/eventOften_output.txt"
        with open(tmp, "w") as f:
            f.write(f"Event often triggered for topic: {config.get('topic', '')}\n")
        self.move_to_destination(tmp)
        progress_callback(100)
