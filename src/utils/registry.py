"""Plugin registry — discovery, validation, and reload logic."""

import importlib.util
import inspect
import json
import os
import sys
import time
import traceback
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Type

from .constants import (
    AUTOKB_RESERVED_NAMES,
    STATE_DELETED,
    STATE_DISABLED,
    STATE_ERROR,
    SUB_TYPE_EVENT_BASED,
    SUB_TYPE_SCHEDULED,
)
from .misc_utils import (
    augment_schema,
    collect_password_field_names,
    get_logger,
    plugin_id_from_metadata,
    resolve_service_icon,
    sanitize_name,
    schema_hash,
)
from .plugin_base import BaseSubscription


@dataclass
class PluginRecord:
    plugin_id: str
    name: str
    icon: str
    description: str
    sub_type: str
    cls: Type[BaseSubscription]
    file_path: str
    display_name: str = ""
    schema: Dict[str, Any] = field(default_factory=dict)
    augmented_schema: Dict[str, Any] = field(default_factory=dict)
    schema_hash_value: str = ""
    password_fields: List[str] = field(default_factory=list)


class PluginRegistry:
    """In-memory registry of all loaded plugins with file-watcher support.

    The same class is used by both the Manager and the Worker. Each
    builds its own instance at startup and again whenever the file
    watcher fires.
    """

    def __init__(self, plugins_dir: str = "/src/plugins", component: str = "plugin_registry",
                 log_file: Optional[str] = None):
        self.plugins_dir = plugins_dir
        self._records: Dict[str, PluginRecord] = {}
        self._log = get_logger(component, log_file)
        # Cached load failures by plugin_id (filename stem)
        self._failed: Dict[str, str] = {}

    # ----- accessors -----
    @property
    def records(self) -> Dict[str, PluginRecord]:
        return self._records

    def list_records(self) -> List[PluginRecord]:
        return list(self._records.values())

    def get(self, plugin_id: str) -> Optional[PluginRecord]:
        return self._records.get(plugin_id)

    def get_or_load(self, plugin_id: str) -> Optional[PluginRecord]:
        """Return the plugin record, loading it from disk on a miss.

        The Manager keeps a file watcher that updates its registry whenever
        a plugin file is added or modified. The Worker, by contrast, builds
        its registry ONCE at startup and does not run a file watcher — so a
        plugin created via ``dev_lab/save`` (or written directly to
        ``/src/plugins/``) after the worker started is invisible to the
        worker's in-memory registry. This method is the on-demand fallback:
        if the record is missing, we re-scan the plugins directory and
        import the single file matching ``plugin_id``. On success the record
        is added to ``self._records`` so subsequent calls are O(1).

        The ``schema_hash_lookup`` is empty in the lazy path because the
        worker does not have direct access to the DB at execution time;
        breaking-change detection is the Manager's responsibility and runs
        inside its file-watcher reload. The worker only executes whatever
        the Manager has already approved.
        """
        rec = self._records.get(plugin_id)
        if rec is not None:
            return rec
        path = os.path.join(self.plugins_dir, f"{plugin_id}.py")
        if not os.path.isfile(path):
            return None
        try:
            record = self._load_plugin_file(path, schema_hash_lookup={})
            self._records[record.plugin_id] = record
            self._log.info(
                "plugin_lazy_loaded", file=f"{plugin_id}.py", action="lazy_load", result="ok"
            )
            return record
        except Exception as exc:  # noqa: BLE001
            self._log.warning(
                "plugin_lazy_load_failed", file=f"{plugin_id}.py", action="lazy_load", result=str(exc)
            )
            return None

    # ----- scanning -----
    def scan_files(self) -> List[str]:
        if not os.path.isdir(self.plugins_dir):
            return []
        out = []
        for entry in sorted(os.listdir(self.plugins_dir)):
            if entry.startswith(".") or not entry.endswith(".py"):
                continue
            if entry == "__init__.py":
                continue
            out.append(entry)
        return out

    def reload_all(self, schema_hash_lookup: Optional[Dict[str, str]] = None,
                   disable_subscriptions_callback=None,
                   send_smtp_callback=None) -> Dict[str, str]:
        """Reload all plugins from disk. ``schema_hash_lookup`` provides the
        previously-stored schema_hash for each plugin_id (used for breaking
        change detection). ``disable_subscriptions_callback(plugin_id)`` is
        invoked when a plugin's schema hash differs from what's stored in
        the DB (and the plugin is therefore refused loading)."""
        schema_hash_lookup = schema_hash_lookup or {}
        errors: Dict[str, str] = {}
        new_records: Dict[str, PluginRecord] = {}

        for fname in self.scan_files():
            path = os.path.join(self.plugins_dir, fname)
            try:
                record = self._load_plugin_file(path, schema_hash_lookup)
                new_records[record.plugin_id] = record
            except Exception as exc:  # noqa: BLE001
                tb = traceback.format_exc()
                self._log.error("plugin_load_failed", file=fname, error=str(exc), traceback=tb)
                errors[fname] = str(exc)
                # If we previously had a record for this plugin_id, keep the old
                # one (we still have subscriptions referencing it) — but mark
                # the new file load as a failure.
                self._failed[fname] = str(exc)
                stem = os.path.splitext(fname)[0]
                pid_guess = sanitize_name(stem)
                old = self._records.get(pid_guess)
                if old is not None:
                    new_records[pid_guess] = old
                # If this was a breaking-change refusal, notify the caller so
                # it can disable affected subscriptions and send alerts.
                if "breaking change" in str(exc) and disable_subscriptions_callback is not None:
                    try:
                        disable_subscriptions_callback(pid_guess)
                    except Exception:
                        pass

        self._records = new_records
        return errors

    def _load_plugin_file(self, path: str, schema_hash_lookup: Dict[str, str]) -> PluginRecord:
        spec = importlib.util.spec_from_file_location(f"_plugin_{os.path.basename(path)}", path)
        if spec is None or spec.loader is None:
            raise ValueError(f"Could not load spec for {path}")
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module  # needed for relative imports
        spec.loader.exec_module(module)

        # Locate the BaseSubscription subclass.
        cls = None
        for _, obj in inspect.getmembers(module, inspect.isclass):
            if obj is BaseSubscription:
                continue
            if issubclass(obj, BaseSubscription) and obj.__module__ == module.__name__:
                cls = obj
                break
        if cls is None:
            raise ValueError(f"No BaseSubscription subclass found in {path}")

        meta = getattr(cls, "metadata", None)
        if not isinstance(meta, dict) or not meta.get("name"):
            raise ValueError(f"Plugin {path} missing or invalid 'metadata' dict")

        # Filename must match metadata["name"].
        file_stem = os.path.splitext(os.path.basename(path))[0]
        try:
            plugin_id = plugin_id_from_metadata(meta)
        except ValueError as exc:
            raise ValueError(f"Plugin name failed sanitization: {exc}") from exc
        if file_stem != plugin_id:
            raise ValueError(
                f"Plugin filename stem {file_stem!r} does not match metadata.name-derived plugin_id {plugin_id!r}"
            )

        if plugin_id in AUTOKB_RESERVED_NAMES:
            raise ValueError(
                f"Plugin {plugin_id!r} name is reserved by AUTOKB_RESERVED_DSN"
            )

        sub_type = meta.get("sub_type")
        if sub_type not in (SUB_TYPE_SCHEDULED, SUB_TYPE_EVENT_BASED):
            raise ValueError(f"metadata.sub_type must be SCHEDULED or EVENT_BASED, got {sub_type!r}")

        # Schema
        inst = cls()
        schema = inst.get_schema()
        if not isinstance(schema, dict):
            raise ValueError("get_schema() must return a dict")
        augmented = augment_schema(schema)
        h = schema_hash(schema)
        # If a previous hash exists and differs, refuse loading.
        prev_hash = schema_hash_lookup.get(plugin_id)
        if prev_hash and prev_hash != h:
            raise ValueError(
                f"Schema breaking change detected for plugin {plugin_id}: "
                f"prev_hash={prev_hash[:8]} new_hash={h[:8]}"
            )

        password_fields = collect_password_field_names(augmented)

        return PluginRecord(
            plugin_id=plugin_id,
            name=meta["name"],
            display_name=meta.get("display_name") or meta["name"],
            icon=resolve_service_icon(cls, meta),
            description=meta.get("description", ""),
            sub_type=sub_type,
            cls=cls,
            file_path=path,
            schema=schema,
            augmented_schema=augmented,
            schema_hash_value=h,
            password_fields=password_fields,
        )

    # ----- load a plugin fresh for a single execution (no sys.modules cache) -----
    def load_plugin_for_execution(self, plugin_id: str) -> Optional[Type[BaseSubscription]]:
        """Return the plugin class for a fresh load (used by the Worker)."""
        rec = self._records.get(plugin_id)
        if rec is None:
            return None
        path = rec.file_path
        spec = importlib.util.spec_from_file_location(f"_plugin_exec_{plugin_id}", path)
        if spec is None or spec.loader is None:
            return None
        module = importlib.util.module_from_spec(spec)
        # Do NOT add to sys.modules so subsequent reloads don't return the
        # cached class.
        spec.loader.exec_module(module)
        for _, obj in inspect.getmembers(module, inspect.isclass):
            if obj is BaseSubscription:
                continue
            if issubclass(obj, BaseSubscription) and obj.__module__ == module.__name__:
                return obj
        return None


__all__ = ["PluginRegistry", "PluginRecord"]
