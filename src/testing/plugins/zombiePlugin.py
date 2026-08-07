"""Test plugin 19: Zombie — ignores cancellation, force-killed.

The plugin's getData sleeps for a long time and NEVER calls
``progress_callback``. The only heartbeat ever written is the initial
``progress_callback(0)`` invoked by the worker before getData runs.
When the test sets the subscription to DISABLED mid-execution, the
child ignores the cancellation, so the worker's watcher thread
force-terminates it at its next per-tick DB status check. The
user-initiated DISABLED status is preserved (NOT overwritten to ERROR);
the force-kill is recorded as an EventLog entry with exit_code=2.
"""

import time

from utils.plugin_base import BaseSubscription
from utils.constants import HEARTBEAT_TIMEOUT


class zombiePlugin(BaseSubscription):
    metadata = {
        "name": "zombiePlugin",
        "display_name": "zombiePlugin",
        "icon": "default_icon.png",
        "description": "Zombie — ignores cancellation, force-killed (Test 19)",
        "sub_type": "SCHEDULED",
    }
    DEFAULT_ACCESS_LEVEL = "PRIVATE"

    def get_schema(self):
        return {"type": "object", "properties": {"label": {"type": "string"}}}

    def getData(self, config, progress_callback):
        # Deliberately do NOT call progress_callback. The only heartbeat
        # ever sent is the initial one issued by the worker before
        # getData runs. After HEARTBEAT_TIMEOUT the watcher kills us.
        time.sleep(HEARTBEAT_TIMEOUT * 10)
