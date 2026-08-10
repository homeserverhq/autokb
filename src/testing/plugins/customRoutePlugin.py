"""Test plugin 13: Custom route."""

from utils.plugin_base import BaseSubscription, PluginRoute


def _custom_status():
    return {"status": "ok", "plugin": "customRoutePlugin"}


class customRoutePlugin(BaseSubscription):
    metadata = {
        "name": "customRoutePlugin",
        "display_name": "customRoutePlugin",
        "description": "Custom API routes plugin (Test 13)",
        "sub_type": "SCHEDULED",
    }
    DEFAULT_ACCESS_LEVEL = "PRIVATE"

    def get_schema(self):
        return {"type": "object", "properties": {"echo": {"type": "string"}}}

    def get_custom_routes(self):
        return [PluginRoute(path="/status", method="GET", handler=_custom_status)]

    def getData(self, config, progress_callback):
        progress_callback(50)
        tmp = "/tmp/customRoute_output.txt"
        with open(tmp, "w") as f:
            f.write(f"echo={config.get('echo', '')}\n")
        self.move_to_destination(tmp)
        progress_callback(100)
