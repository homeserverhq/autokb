"""Manager-specific wrapper around PluginRegistry.

The Manager runs a hot-swap file watcher that rebuilds the registry on
change. This module also exposes the breaking-change side effects
(disable affected subscriptions, send SMTP) and the dynamic route
mounting.
"""

import asyncio
import os
import shutil
import threading
import time
from typing import Any, Awaitable, Callable, Dict, List, Optional

from fastapi import APIRouter, FastAPI, Request
from fastapi.responses import JSONResponse

from utils.constants import STATE_DELETED, STATE_DISABLED
from utils.database import DatabaseManager
from utils.misc_utils import get_logger, send_smtp_notification
from utils.registry import PluginRecord, PluginRegistry


class ManagerPluginRegistry(PluginRegistry):
    """PluginRegistry variant that integrates with the Manager's services.

    Adds:
      * reference to the DatabaseManager (for hash tracking + breaking
        change disable);
      * reference to the FastAPI app (for hot-swap route re-mounting);
      * hook callbacks for SMTP notifications on breaking changes.
    """

    def __init__(self, db: DatabaseManager, app: FastAPI,
                 plugins_dir: str = "/src/plugins",
                 smtp_config: Optional[Dict[str, Any]] = None,
                 log_file: Optional[str] = None):
        super().__init__(plugins_dir=plugins_dir, component="plugin_registry", log_file=log_file)
        self._db = db
        self._app = app
        self._smtp_config = smtp_config or {}
        self._routes_mounted: List[str] = []
        self._mount_lock = threading.RLock()

    @property
    def db(self) -> DatabaseManager:
        return self._db

    def reload(self) -> Dict[str, str]:
        """Reload the registry, disabling subscriptions on breaking change."""
        # Fetch existing plugin state for hash comparison
        existing: Dict[str, str] = {}
        with self._db.get_session() as s:
            from utils.database import PluginRegistryState
            for row in s.query(PluginRegistryState).all():
                existing[row.plugin_id] = row.schema_hash
        errors = self.reload_all(
            schema_hash_lookup=existing,
            disable_subscriptions_callback=self._on_breaking_change,
            send_smtp_callback=self._send_smtp,
        )
        # Persist the new hashes and remount custom routes
        self._persist_hashes()
        self._remount_routes()
        return errors

    def _on_breaking_change(self, plugin_id: str) -> None:
        """Disable all subscriptions for ``plugin_id`` after a breaking change."""
        affected = self._db.list_subscriptions(plugin_id=plugin_id, include_deleted=False)
        count = 0
        for sub in affected:
            if sub.status == STATE_DISABLED or sub.status == STATE_DELETED:
                continue
            self._db.update_status(sub.id, STATE_DISABLED, last_error="Schema breaking change", guard="error_safe")
            count += 1
        self._log.warning("plugin_breaking_change", plugin_id=plugin_id, affected=count)
        if count and self._smtp_config:
            try:
                send_smtp_notification(
                    subject=f"[AutoKB] Schema breaking change: {plugin_id}",
                    body=(
                        f"Plugin {plugin_id!r} had a breaking schema change.\n"
                        f"{count} subscription(s) have been disabled.\n"
                        "Users must update their configuration before re-enabling."
                    ),
                    **self._smtp_config,
                )
            except Exception:
                pass

    def _send_smtp(self, subject: str, body: str) -> None:
        try:
            send_smtp_notification(subject=subject, body=body, **self._smtp_config)
        except Exception:
            pass

    def _persist_hashes(self) -> None:
        for rec in self.list_records():
            try:
                self._db.upsert_plugin_state(rec.plugin_id, rec.schema_hash_value)
            except Exception as exc:  # noqa: BLE001
                self._log.error("plugin_state_persist_failed", plugin_id=rec.plugin_id, error=str(exc))

    def _remount_routes(self) -> None:
        """Re-mount custom plugin routes under ``/api/plugins/{plugin_id}/*``.

        We use a dynamic catch-all route in manager.py (``/api/plugins/{id}/{path:path}``)
        that looks up the plugin's custom routes at request time, so we don't
        need to register/unregister routes here.
        """
        # No-op: the catch-all in manager.py handles dispatch.
        pass

    def list_metadata(self) -> List[Dict[str, Any]]:
        out = []
        for rec in self.list_records():
            out.append({
                "plugin_id": rec.plugin_id,
                "name": rec.name,
                "display_name": rec.display_name,
                "icon": rec.icon,
                "description": rec.description,
                "sub_type": rec.sub_type,
                "default_access_level": rec.default_access_level,
            })
        return out


# Patch DatabaseManager to expose ``query_plugin_state_all`` for the manager
# registry without altering the main file too much.
def _patch_db() -> None:
    """No-op placeholder kept for backward compatibility."""
    pass


_patch_db()
