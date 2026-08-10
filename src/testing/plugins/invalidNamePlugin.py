"""Test plugin 14: Invalid name — consecutive periods.

The plugin loader's sanitize_name rejects consecutive periods. The
metadata name "bad..name" sanitizes to "bad.name" but the filename
"invalidNamePlugin.py" maps to plugin_id "invalidNamePlugin", causing
a mismatch (the loader checks filename == sanitized(metadata.name)).

For the test runner, we want the plugin to be REJECTED at load time.
The simplest way is to make the metadata.name have characters that
fail sanitization rules — or to mismatch the filename.

Here we mismatch: filename is ``invalidNamePlugin.py`` but metadata.name
is ``bad..namePlugin``. After sanitize_name, the metadata plugin_id
becomes ``bad.namePlugin`` (collapsed to a single period), which does
NOT equal the filename stem ``invalidNamePlugin`` → loader rejects.
"""

from utils.plugin_base import BaseSubscription


class _BadNamePlugin(BaseSubscription):
    metadata = {
        "name": "bad..namePlugin",
        "display_name": "bad..namePlugin",
# consecutive periods → sanitize to "bad.namePlugin"
        "description": "Invalid name plugin (Test 14) — should be rejected at load",
        "sub_type": "SCHEDULED",
    }
    DEFAULT_ACCESS_LEVEL = "PRIVATE"

    def get_schema(self):
        return {"type": "object", "properties": {"x": {"type": "string"}}}

    def getData(self, config, progress_callback):
        progress_callback(100)
