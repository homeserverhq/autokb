"""Base class for DKB (Downstream Knowledge Base) service drop-ins.

Each concrete DKB service (OpenWebUI, Cognee, etc.) subclasses BaseDKBService
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


class BaseDKBService(ABC):
    """Abstract base for a single DKB *service type* (e.g. OpenWebUI).

    Each concrete subclass:
      * Sets class-level ``metadata`` dict:
          ``{"name": "...", "description": "...", "icon": "..."}``
      * Implements the six abstract method stubs.

    The *instance* is bound to one ``dkb_datastore`` row at construction time
    and receives a reference to the ``DatabaseManager`` for DB bookkeeping.
    """

    metadata: Dict[str, str] = {}  # overridden by subclass

    def __init__(self, datastore_row: Any, db: Any):
        """*datastore_row* is an ORM row with attributes:
        ``id, service_id, name, api_url, api_key, remote_datastore_id, ds_extra_params``
        """
        self.datastore_id = datastore_row.id
        self.service_id = datastore_row.service_id
        self.name = datastore_row.name
        self.api_url = datastore_row.api_url
        self.api_key = datastore_row.api_key  # already decrypted by caller
        self.remote_datastore_id = datastore_row.remote_datastore_id
        self.ds_extra_params = datastore_row.ds_extra_params or {}
        self.db = db

    # ---- abstract methods (remote operations only) ----

    @abstractmethod
    def add_datafile(self, path: str) -> str:
        """Upload a local file to the remote datastore.

        Returns the remote_datafile_id assigned by the remote instance.
        """
        ...

    @abstractmethod
    def update_datafile(self, remote_datafile_id: str, path: str) -> None:
        """Re-upload (update) an existing file on the remote datastore."""
        ...

    @abstractmethod
    def remove_datafile(self, remote_datafile_id: str) -> None:
        """Delete a file from the remote datastore. Must be idempotent."""
        ...

    @abstractmethod
    def add_datastore(self) -> str:
        """Create the remote datastore (knowledge base / dataset).

        Returns the remote_datastore_id assigned by the remote instance.
        Called when ``remote_datastore_id`` is null on first recon.
        """
        ...

    @abstractmethod
    def remove_datastore(self) -> None:
        """Destroy the remote datastore object and all its datafiles.
        Called when the last datastore_subscription is removed.
        """
        ...

    @abstractmethod
    def clear_datastore(self) -> None:
        """Remove all datafiles from the remote datastore, but keep the
        datastore object intact (for a full re-import). Not currently
        invoked — contract only.
        """
        ...

    # ---- concrete wrapper methods (DB bookkeeping + abstract calls) ----

    def base_add_datafile(self, sub_id: str, path: str) -> None:
        """Add a local file to this datastore: DB bookkeeping + remote upload."""
        size = os.path.getsize(path)
        mtime = os.path.getmtime(path)
        datafile_hash = compute_file_hash(path)

        # get-or-create akb_datafile
        df = self.db.get_or_create_datafile(sub_id, path, size, mtime, datafile_hash)

        # check if already tracked for this datastore
        existing = self.db.get_datastore_datafile(self.datastore_id, df.id)
        if existing:
            return  # already added

        remote_id = self.add_datafile(path)
        self.db.insert_datastore_datafile(self.datastore_id, df.id, remote_id, datafile_hash)

    def base_update_datafile(self, datafile_id: str, new_hash: str) -> None:
        """Update a file on the remote datastore and sync the hash."""
        ds_df = self.db.get_datastore_datafile(self.datastore_id, datafile_id)
        if not ds_df:
            return
        df = self.db.get_datafile(datafile_id)
        if not df:
            return
        self.update_datafile(ds_df.remote_datafile_id, df.path)
        self.db.update_datastore_datafile_hash(self.datastore_id, datafile_id, new_hash)

    def base_remove_datafile(self, datafile_id: str) -> None:
        """Remove a file from the remote datastore and delete the join row."""
        ds_df = self.db.get_datastore_datafile(self.datastore_id, datafile_id)
        if not ds_df:
            return
        self.remove_datafile(ds_df.remote_datafile_id)
        self.db.delete_datastore_datafile(self.datastore_id, datafile_id)

    def base_add_datastore(self) -> str:
        """Create the remote datastore and persist the returned id."""
        remote_id = self.add_datastore()
        if remote_id:
            self.db.set_datastore_remote_id(self.datastore_id, remote_id)
            self.remote_datastore_id = remote_id
        return remote_id


__all__ = ["BaseDKBService", "compute_file_hash"]
