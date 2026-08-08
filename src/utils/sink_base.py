"""Base class for Sink (downstream destination) service drop-ins.

Each concrete Sink service (OpenWebUI, Cognee, etc.) subclasses BaseSink
and implements only the abstract remote-operation methods. The base class
provides wrapper methods (base_add_datafile etc.) that handle all DB
bookkeeping, hashing, etc.
"""

import hashlib
import os
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional


_CHUNK_SIZE = 1 << 20  # 1 MiB


def compute_file_hash(path: str) -> str:
    """SHA-256 hex digest of the file at *path* (chunked for large files)."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            buf = f.read(_CHUNK_SIZE)
            if not buf:
                break
            h.update(buf)
    return h.hexdigest()


class BaseSink(ABC):
    """Abstract base for a single Sink *service type* (e.g. OpenWebUI).

    Each concrete subclass:
      * Sets class-level ``metadata`` dict:
          ``{"name": "...", "description": "...", "icon": "..."}``
      * Implements the six abstract method stubs.

    The *instance* is bound to one ``target`` row at construction time
    and receives a reference to the ``DatabaseManager`` for DB bookkeeping.
    """

    metadata: Dict[str, str] = {}  # overridden by subclass

    # Optional per-service defaulting. Subclasses may override:
    #   * default_api_url — fallback base URL when the target row has none
    #   * api_key_env_var — env var that supplies the API key default
    default_api_url: str = ""
    api_key_env_var: Optional[str] = None

    def __init__(self, target_row: Any, db: Any):
        """*target_row* is an ORM row with attributes:
        ``id, service_id, name, api_url, api_key, remote_target_id, target_extra_params``
        """
        self.target_id = target_row.id
        self.service_id = target_row.service_id
        self.name = target_row.name
        self.api_url = target_row.api_url
        self.api_key = target_row.api_key  # already decrypted by caller
        self.remote_target_id = target_row.remote_target_id
        self.target_extra_params = target_row.target_extra_params or {}
        self.db = db

    @classmethod
    def get_defaults(cls) -> Dict[str, Any]:
        """Class-level defaults surfaced to the web UI (create-target form).

        ``has_api_key_default`` is a boolean so the actual secret never leaves
        the backend; it resolves at recon time in ``__init__``.
        """
        env_key = getattr(cls, "api_key_env_var", None)
        return {
            "api_url": getattr(cls, "default_api_url", "") or "",
            "has_api_key_default": bool(env_key and os.environ.get(env_key)),
        }

    # ---- abstract methods (remote operations only) ----

    @abstractmethod
    def add_datafile(self, path: str) -> str:
        """Upload a local file to the remote target.

        Returns the remote_datafile_id assigned by the remote instance.
        """
        ...

    @abstractmethod
    def update_datafile(self, remote_datafile_id: str, path: str) -> str:
        """Re-upload (update) an existing file on the remote target.

        Returns the NEW remote_datafile_id assigned after the re-upload.
        It may equal the old id if the remote instance dedupes by content.
        Callers must persist the returned id.
        """
        ...

    @abstractmethod
    def remove_datafile(self, remote_datafile_id: str) -> None:
        """Delete a file from the remote target. Must be idempotent."""
        ...

    @abstractmethod
    def add_target(self) -> str:
        """Create the remote target (knowledge base / dataset).

        Returns the remote_target_id assigned by the remote instance.
        Called when ``remote_target_id`` is null on first recon.
        """
        ...

    @abstractmethod
    def remove_target(self) -> None:
        """Destroy the remote target object and all its datafiles.
        Called when the last target_subscription is removed.
        """
        ...

    @abstractmethod
    def clear_target(self) -> None:
        """Remove all datafiles from the remote target, but keep the
        target object intact (for a full re-import). Not currently
        invoked — contract only.
        """
        ...

    # ---- concrete wrapper methods (DB bookkeeping + abstract calls) ----

    def base_add_datafile(self, sub_id: str, path: str, known_hash: Optional[str] = None) -> None:
        """Add a local file to this target: DB bookkeeping + remote upload.

        Pass ``known_hash`` when the caller has already verified the file is
        unchanged (size/mtime match) to avoid re-hashing the contents.
        """
        size = os.path.getsize(path)
        mtime = os.path.getmtime(path)
        datafile_hash = known_hash if known_hash is not None else compute_file_hash(path)

        # get-or-create akb_datafile
        df = self.db.get_or_create_datafile(sub_id, path, size, mtime, datafile_hash)

        # check if already tracked for this target
        existing = self.db.get_target_datafile(self.target_id, df.id)
        if existing:
            return  # already added

        remote_id = self.add_datafile(path)
        self.db.insert_target_datafile(self.target_id, df.id, remote_id, datafile_hash)

    def base_update_datafile(self, datafile_id: str, new_hash: str) -> None:
        """Update a file on the remote target and sync the hash + remote id."""
        t_df = self.db.get_target_datafile(self.target_id, datafile_id)
        if not t_df:
            return
        df = self.db.get_datafile(datafile_id)
        if not df:
            return
        new_remote_id = self.update_datafile(t_df.remote_datafile_id, df.path)
        if new_remote_id:
            self.db.update_target_datafile_remote_id(self.target_id, datafile_id, new_remote_id)
        self.db.update_target_datafile_hash(self.target_id, datafile_id, new_hash)

    def base_remove_datafile(self, datafile_id: str) -> None:
        """Remove a file from the remote target and delete the join row."""
        t_df = self.db.get_target_datafile(self.target_id, datafile_id)
        if not t_df:
            return
        self.remove_datafile(t_df.remote_datafile_id)
        self.db.delete_target_datafile(self.target_id, datafile_id)

    def base_add_target(self) -> str:
        """Create the remote target and persist the returned id."""
        remote_id = self.add_target()
        if remote_id:
            self.db.set_target_remote_id(self.target_id, remote_id)
            self.remote_target_id = remote_id
        return remote_id


__all__ = ["BaseSink", "compute_file_hash"]
