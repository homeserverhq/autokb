"""DKB service registry — discovery, loading, and in-memory cache.

Mirrors ``utils.registry.PluginRegistry``. Scans ``/src/dkbservices``
for ``*_DKB.py`` files, imports the ``BaseDKBService`` subclass, and
exposes metadata.
"""

import importlib.util
import inspect
import os
import sys
from typing import Any, Dict, List, Optional

from .dkb_service_base import BaseDKBService
from .misc_utils import get_logger, sanitize_name


class DKBServiceRecord:
    """Metadata for one loaded DKB service type."""

    def __init__(self, service_name: str, cls, metadata: Dict[str, str], file_path: str):
        self.service_name = service_name
        self.cls = cls
        self.metadata = metadata
        self.file_path = file_path
        self.icon = metadata.get("icon", "default_icon.png")


class DKBRegistry:
    """In-memory registry of loaded DKB service types.

    Loaded by both Manager and Worker. The Manager additionally upserts
    ``dkb_service`` rows at startup.
    """

    def __init__(self, dkbs_dir: str = "/src/dkbservices", component: str = "dkb_registry",
                 log_file: Optional[str] = None):
        self._dkbs_dir = dkbs_dir
        self._records: Dict[str, DKBServiceRecord] = {}
        self._log = get_logger(component, log_file)

    @property
    def records(self) -> Dict[str, DKBServiceRecord]:
        return self._records

    def list_records(self) -> List["DKBServiceRecord"]:
        return list(self._records.values())

    def get(self, service_name: str) -> Optional["DKBServiceRecord"]:
        return self._records.get(service_name)

    def scan_files(self) -> List[str]:
        if not os.path.isdir(self._dkbs_dir):
            return []
        out = []
        for entry in sorted(os.listdir(self._dkbs_dir)):
            if entry.startswith(".") or not entry.endswith("_DKB.py"):
                continue
            out.append(entry)
        return out

    def reload_all(self) -> None:
        self._records = {}
        for fname in self.scan_files():
            path = os.path.join(self._dkbs_dir, fname)
            try:
                record = self._load_file(path)
                self._records[record.service_name] = record
                self._log.info("dkb_service_loaded", service=record.service_name, file=fname)
            except Exception as exc:
                self._log.error("dkb_load_failed", file=fname, error=str(exc))

    def _load_file(self, path: str) -> "DKBServiceRecord":
        stem = os.path.splitext(os.path.basename(path))[0]
        if not stem.endswith("_DKB"):
            raise ValueError(f"File {path} does not end with _DKB.py")
        raw_service_name = stem[:-4]  # strip "_DKB"

        spec = importlib.util.spec_from_file_location(f"_dkb_{stem}", path)
        if spec is None or spec.loader is None:
            raise ValueError(f"Could not load spec for {path}")
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)

        cls = None
        for _, obj in inspect.getmembers(module, inspect.isclass):
            if obj is BaseDKBService:
                continue
            if issubclass(obj, BaseDKBService) and obj.__module__ == module.__name__:
                cls = obj
                break
        if cls is None:
            raise ValueError(f"No BaseDKBService subclass in {path}")

        meta = getattr(cls, "metadata", None)
        if not isinstance(meta, dict) or not meta.get("name"):
            raise ValueError(f"DKB service {path} missing metadata dict with 'name'")

        svc_name = meta["name"]
        sname = sanitize_name(svc_name)
        if sname != raw_service_name:
            raise ValueError(
                f"DKB file stem {raw_service_name!r} != sanitized metadata.name {sname!r}"
            )

        return DKBServiceRecord(
            service_name=svc_name,
            cls=cls,
            metadata=meta,
            file_path=path,
        )

    def load_service_for_recon(self, service_name: str, datastore_row: Any, db: Any) -> Optional[BaseDKBService]:
        """Instantiate a DKB service for a specific datastore row."""
        rec = self._records.get(service_name)
        if rec is None:
            return None
        return rec.cls(datastore_row, db)

    def lookup_service_name(self, db_service_name: str) -> str:
        """Map a ``dkb_service.name`` (from DB) to the class metadata name."""
        return db_service_name


__all__ = ["DKBRegistry", "DKBServiceRecord"]
