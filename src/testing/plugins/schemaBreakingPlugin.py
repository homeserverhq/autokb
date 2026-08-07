
from utils.plugin_base import BaseSubscription


class schemaBreakingPlugin(BaseSubscription):
    metadata = {
        "name": "schemaBreakingPlugin",
            "display_name": "schemaBreakingPlugin",
        "icon": "default_icon.png",
        "description": "Schema breaking change plugin — V1 (title+author)",
        "sub_type": "SCHEDULED",
    }
    DEFAULT_ACCESS_LEVEL = "PRIVATE"

    def get_schema(self):
        return {
            "type": "object",
            "properties": {
                "title": {"type": "string", "minLength": 1},
                "author": {"type": "string", "minLength": 1},
            },
            "required": ["title", "author"],
        }

    def getData(self, config, progress_callback):
        progress_callback(100)
