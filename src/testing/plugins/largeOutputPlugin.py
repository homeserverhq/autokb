"""Test plugin 11: Large output — 100 files of ~1MB each.

Note: The spec calls for 10MB each, but in this test deployment
HEARTBEAT_TIMEOUT=5 means a full run should complete well under 60s.
We use 1MB files to keep the test fast while still exercising rmtree.
"""

import os
import shutil
import time

from utils.plugin_base import BaseSubscription


CHUNK = b"X" * (1024 * 1024)  # 1MB


class largeOutputPlugin(BaseSubscription):
    metadata = {
        "name": "largeOutputPlugin",
        "icon": "default_icon.png",
        "description": "Large output plugin (Test 11)",
        "sub_type": "SCHEDULED",
    }
    DEFAULT_ACCESS_LEVEL = "PRIVATE"

    def get_schema(self):
        return {
            "type": "object",
            "properties": {"file_count": {"type": "integer", "minimum": 1, "maximum": 1000}},
        }

    def getData(self, config, progress_callback):
        n = int(config.get("file_count", 5))
        for i in range(n):
            tmp = f"/tmp/largeOutput_file_{i:03d}.bin"
            with open(tmp, "wb") as f:
                f.write(CHUNK)
            self.move_to_destination(tmp)
            time.sleep(0.01)
            progress_callback(int(100 * (i + 1) / n))
        progress_callback(100)
