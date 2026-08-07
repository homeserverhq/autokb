
from utils.plugin_base import BaseSubscription


class editMatchPlugin(BaseSubscription):
    metadata = {
        "name": "editMatchPlugin",
            "display_name": "editMatchPlugin",
        "icon": "default_icon.png",
        "description": "edit match plugin V1",
        "sub_type": "SCHEDULED",
    }
    DEFAULT_ACCESS_LEVEL = "PRIVATE"

    def get_schema(self):
        return {
            "type": "object",
            "properties": {
                "label": {"type": "string", "minLength": 1},
            },
            "required": ["label"],
        }

    def getData(self, config, progress_callback):
        import os
        tmp = "/tmp/editMatchPlugin_output.txt"
        with open(tmp, "w") as f:
            f.write("VERSION_1")
        os.makedirs("/output/editMatchPlugin", exist_ok=True)
        # Make the output visible to the test (which runs inside the
        # manager container and shares /output via the volume mount).
        self.move_to_destination(tmp)
        progress_callback(100)
