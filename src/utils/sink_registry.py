"""Sink registry — discovery, loading, and in-memory cache.

Mirrors ``utils.registry.PluginRegistry``. Scans ``/src/sinks``
for ``*Sink.py`` files, imports the ``BaseSink`` subclass, and
exposes metadata.
"""

import importlib.util
import inspect
import os
import sys
from typing import Any, Dict, List, Optional

from .misc_utils import get_logger, sanitize_name
from .sink_base import BaseSink


class SinkServiceRecord:
    """Metadata for one loaded Sink service type."""

    def __init__(self, service_name: str, cls, metadata: Dict[str, str], file_path: str):
        self.service_name = service_name
        self.cls = cls
        self.metadata = metadata
        self.file_path = file_path
        self.display_name = metadata.get("display_name") or service_name
        self.icon = metadata.get("icon", "default_icon.png")


class SinkRegistry:
    """In-memory registry of loaded Sink service types.

    Loaded by both Manager and Worker. The Manager additionally upserts
    ``sink`` rows at startup.
    """

    def __init__(self, sinks_dir: str = "/src/sinks", component: str = "sink_registry",
                 log_file: Optional[str] = None):
        self._sinks_dir = sinks_dir
        self._records: Dict[str, SinkServiceRecord] = {}
        self._log = get_logger(component, log_file)

    @property
    def records(self) -> Dict[str, SinkServiceRecord]:
        return self._records

    def list_records(self) -> List["SinkServiceRecord"]:
        return list(self._records.values())

    def get(self, service_name: str) -> Optional["SinkServiceRecord"]:
        return self._records.get(service_name)

    def get_or_load(self, service_name: str) -> Optional["SinkServiceRecord"]:
        """Return the service record, loading it from disk on a miss.

        Mirrors ``PluginRegistry.get_or_load``. The Manager runs a file
        watcher that hot-swaps its registry whenever a ``*Sink.py`` file is
        added or removed. The Worker, by contrast, builds its registry once
        at startup and does not run a file watcher — so a Sink service added
        after the worker started (e.g. a test service synced into
        ``/src/sinks/``) is invisible to the worker's in-memory
        registry. This is the on-demand fallback: if the record is missing,
        we re-scan the sinks directory and import the single file
        matching ``service_name``. On success the record is added to
        ``self._records`` so subsequent calls are O(1).
        """
        rec = self._records.get(service_name)
        if rec is not None:
            return rec
        # Check the primary dir first, then fall back to the testing dir
        # (e.g. /src/testing/sinks/ in the image). The Worker's
        # /src/sinks may be a host bind mount that the Manager's
        # runtime sync cannot reach, so the worker lazy-loads from the
        # testing source present in its own image.
        for d in (self._sinks_dir, "/src/testing/sinks"):
            path = os.path.join(d, f"{sanitize_name(service_name)}.py")
            if os.path.isfile(path):
                try:
                    record = self._load_file(path)
                    self._records[record.service_name] = record
                    self._log.info(
                        "sink_service_lazy_loaded", file=os.path.basename(path),
                        action="lazy_load", result="ok",
                    )
                    return record
                except Exception as exc:  # noqa: BLE001
                    self._log.warning(
                        "sink_service_lazy_load_failed", file=os.path.basename(path),
                        action="lazy_load", result=str(exc),
                    )
                    return None
        return None

    def scan_files(self) -> List[str]:
        if not os.path.isdir(self._sinks_dir):
            return []
        out = []
        for entry in sorted(os.listdir(self._sinks_dir)):
            if entry.startswith(".") or not entry.endswith("Sink.py"):
                continue
            out.append(entry)
        return out

    def reload_all(self) -> None:
        self._records = {}
        for fname in self.scan_files():
            path = os.path.join(self._sinks_dir, fname)
            try:
                record = self._load_file(path)
                self._records[record.service_name] = record
                self._log.info("sink_service_loaded", service=record.service_name, file=fname)
            except Exception as exc:
                self._log.error("sink_load_failed", file=fname, error=str(exc))

    def _load_file(self, path: str) -> "SinkServiceRecord":
        stem = os.path.splitext(os.path.basename(path))[0]
        if not stem.endswith("Sink"):
            raise ValueError(f"File {path} does not end with Sink.py")
        raw_service_name = stem

        spec = importlib.util.spec_from_file_location(f"_sink_{stem}", path)
        if spec is None or spec.loader is None:
            raise ValueError(f"Could not load spec for {path}")
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)

        cls = None
        for _, obj in inspect.getmembers(module, inspect.isclass):
            if obj is BaseSink:
                continue
            if issubclass(obj, BaseSink) and obj.__module__ == module.__name__:
                cls = obj
                break
        if cls is None:
            raise ValueError(f"No BaseSink subclass in {path}")

        meta = getattr(cls, "metadata", None)
        if not isinstance(meta, dict) or not meta.get("name"):
            raise ValueError(f"Sink service {path} missing metadata dict with 'name'")

        svc_name = meta["name"]
        sname = sanitize_name(svc_name)
        if sname != raw_service_name:
            raise ValueError(
                f"Sink file stem {raw_service_name!r} != sanitized metadata.name {sname!r}"
            )

        return SinkServiceRecord(
            service_name=svc_name,
            cls=cls,
            metadata=meta,
            file_path=path,
        )

    def load_service_for_recon(self, service_name: str, target_row: Any, db: Any) -> Optional[BaseSink]:
        """Instantiate a Sink service for a specific target row."""
        rec = self.get_or_load(service_name)
        if rec is None:
            return None
        return rec.cls(target_row, db)

    def lookup_service_name(self, db_service_name: str) -> str:
        """Map a ``sink.name`` (from DB) to the class metadata name."""
        return db_service_name


__all__ = ["SinkRegistry", "SinkServiceRecord"]
