"""Test plugin 9: Password field encryption."""

from utils.plugin_base import BaseSubscription


class passwordPlugin(BaseSubscription):
    metadata = {
        "name": "passwordPlugin",
        "display_name": "passwordPlugin",
        "description": "Password field encryption plugin (Test 9)",
        "sub_type": "SCHEDULED",
    }
    DEFAULT_ACCESS_LEVEL = "PRIVATE"

    def get_schema(self):
        return {
            "type": "object",
            "properties": {
                "apiKey": {"type": "string", "format": "password", "minLength": 1},
            },
            "required": ["apiKey"],
        }

    def getData(self, config, progress_callback):
        progress_callback(50)
        tmp = "/tmp/password_output.txt"
        # config["apiKey"] arrives decrypted at execution time
        with open(tmp, "w") as f:
            f.write(f"apiKey received, length={len(config.get('apiKey', ''))}\n")
        self.move_to_destination(tmp)
        progress_callback(100)
