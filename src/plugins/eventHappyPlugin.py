"""Test plugin 2: EVENT_BASED success — monitor fires exactly once on the 3rd call."""

from utils.plugin_base import BaseSubscription


class eventHappyPlugin(BaseSubscription):
    metadata = {
        "name": "eventHappyPlugin",
        "icon": "default_icon.png",
        "description": "EVENT_BASED success plugin (Test 2)",
        "sub_type": "EVENT_BASED",
    }
    DEFAULT_ACCESS_LEVEL = "PRIVATE"

    def __init__(self):
        super().__init__()
        # Instance state — the scheduler creates a fresh instance per
        # monitor task (via rec.cls()), so each monitor lifetime gets
        # its own counter and "fired" flag. This is per the spec:
        # "returns True on the 3rd invocation and False otherwise"
        # and "tests that the monitor triggers exactly one enqueue".
        self._calls = 0
        self._fired = False

    def get_schema(self):
        return {
            "type": "object",
            "properties": {
                "topic": {"type": "string", "minLength": 1},
            },
            "required": ["topic"],
        }

    async def monitor(self, config, cancel_token):
        if self._fired:
            return False
        self._calls += 1
        if self._calls >= 3:
            self._fired = True
            return True
        return False

    def getData(self, config, progress_callback):
        progress_callback(50)
        tmp = "/tmp/eventHappy_output.txt"
        with open(tmp, "w") as f:
            f.write(f"Event triggered for topic: {config.get('topic', '')}\n")
        self.move_to_destination(tmp)
        progress_callback(100)
