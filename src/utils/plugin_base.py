"""Abstract base class (the contract) for AutoKB plugins."""

import structlog
import os
import shutil
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

from .constants import ACCESS_PRIVATE
from .misc_utils import sanitize_name, SubscriptionCancelledError


@dataclass
class PluginRoute:
    """A custom API route exposed by a plugin.

    Attributes:
        path: URL path under ``/api/plugins/{plugin_id}`` (e.g. ``/status``).
        method: HTTP method (e.g. ``GET``).
        handler: callable that receives a FastAPI Request and returns a JSON-serialisable value.
    """
    path: str
    method: str
    handler: Callable[..., Any]


class BaseSubscription(ABC):
    """The base class that all data source plugins must subclass.

    Subclasses must override the class-level ``metadata`` dict and
    ``DEFAULT_ACCESS_LEVEL`` and implement ``get_schema()``, ``getData()``,
    and (for event-driven plugins) ``monitor()``.
    """

    # ---- mandatory class-level overrides (validated at load time) ----
    metadata: Dict[str, Any] = {
        "name": "",
        "icon": "default_icon.png",
        "description": "",
        "sub_type": "SCHEDULED",
    }
    DEFAULT_ACCESS_LEVEL: str = ACCESS_PRIVATE

    # ---- internal state set by the Managed Execution Wrapper ----
    _heartbeat_event: Optional[Any] = None
    _subscription_id: Optional[str] = None
    _subscription_name: Optional[str] = None

    def __init__(self):
        self.log = structlog.get_logger(f"plugin.{self.__class__.__name__}")

    # ---- abstract methods ----
    @abstractmethod
    def get_schema(self) -> Dict[str, Any]:
        """Return the JSON Schema dict for this plugin."""
        raise NotImplementedError

    @abstractmethod
    def getData(self, config: Dict[str, Any], progress_callback: Callable[[int], None]) -> None:
        """Core logic of the plugin. Must call ``progress_callback`` periodically."""
        raise NotImplementedError

    # ---- optional overrides ----
    def get_custom_routes(self) -> List[PluginRoute]:
        """Return custom API routes for this plugin (default: none)."""
        return []

    async def monitor(self, config: Dict[str, Any], cancel_token: Any) -> bool:
        """Event-based hook. Return True to signal "enqueue a run", False to keep waiting.

        Default implementation raises ``NotImplementedError`` which is caught
        by the monitor loop in the scheduler.
        """
        raise NotImplementedError

    # ---- utilities provided to subclasses ----
    def move_to_destination(self, temp_file_path: str) -> str:
        """Move a temp file into the canonical output location for this subscription.

        Target path:
            ``/output/{sanitized_plugin_name}/{sanitized_subscription_name}/{sanitized_filename}``
        """
        if not self._subscription_name:
            raise RuntimeError("move_to_destination called before _subscription_name was set")
        plugin_name = sanitize_name(self.metadata["name"])
        sub_name = sanitize_name(self._subscription_name)
        file_name = sanitize_name(os.path.basename(temp_file_path))
        target_dir = os.path.join("/output", plugin_name, sub_name)
        os.makedirs(target_dir, exist_ok=True)
        target_path = os.path.join(target_dir, file_name)
        if os.path.isdir(temp_file_path):
            # if source is a directory, copytree then rmtree
            if os.path.exists(target_path):
                if os.path.isdir(target_path):
                    shutil.rmtree(target_path)
                else:
                    os.remove(target_path)
            shutil.copytree(temp_file_path, target_path)
            shutil.rmtree(temp_file_path)
            for root, dirs, files in os.walk(target_path):
                for f in files:
                    os.chmod(os.path.join(root, f), 0o400)
                for d in dirs:
                    os.chmod(os.path.join(root, d), 0o700)
            os.chmod(target_path, 0o700)
        else:
            shutil.move(temp_file_path, target_path)
            os.chmod(target_path, 0o400)
        return target_path

    def get_destination_path(self) -> str:
        """Return the canonical output directory for this subscription."""
        if not self._subscription_name:
            raise RuntimeError("get_destination_path called before _subscription_name was set")
        plugin_name = sanitize_name(self.metadata["name"])
        sub_name = sanitize_name(self._subscription_name)
        return os.path.join("/output", plugin_name, sub_name)


__all__ = ["BaseSubscription", "PluginRoute", "SubscriptionCancelledError"]
