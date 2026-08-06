"""Test plugin 17: All schema field types."""

from utils.plugin_base import BaseSubscription


class configValidationPlugin(BaseSubscription):
    metadata = {
        "name": "configValidationPlugin",
        "icon": "default_icon.png",
        "description": "All schema field types (Test 17)",
        "sub_type": "SCHEDULED",
    }
    DEFAULT_ACCESS_LEVEL = "PRIVATE"

    def get_schema(self):
        return {
            "type": "object",
            "properties": {
                "name": {"type": "string", "minLength": 1, "maxLength": 50},
                "combo": {"type": "string", "enum": ["A", "B", "C"]},
                "radio": {"type": "string", "enum": ["X", "Y"]},
                "checkbox": {"type": "boolean"},
                "secret": {"type": "string", "format": "password", "minLength": 1},
            },
            "required": ["name", "combo", "radio", "checkbox", "secret"],
        }

    def getData(self, config, progress_callback):
        progress_callback(50)
        tmp = "/tmp/configValidation_output.txt"
        with open(tmp, "w") as f:
            f.write(
                f"name={config.get('name', '')} combo={config.get('combo', '')} "
                f"radio={config.get('radio', '')} checkbox={config.get('checkbox', '')} "
                f"secret_len={len(config.get('secret', ''))}\n"
            )
        self.move_to_destination(tmp)
        progress_callback(100)
