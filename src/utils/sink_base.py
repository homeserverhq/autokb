"""Base class for Sink (downstream destination) service drop-ins.

Each concrete Sink service (OpenWebUI, Cognee, etc.) subclasses BaseSink
and implements only the abstract remote-operation methods. The base class
provides wrapper methods (base_add_datafile etc.) that handle all DB
bookkeeping, hashing, etc.
"""

import hashlib
import os
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, Optional

from utils.misc_utils import SinkCancelledError, in_schedule_window, parse_schedule_window, sanitize_name


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

    @classmethod
    def icon(cls) -> str:
        """Icon asset filename for this service, derived from the service name.

        Returns ``{sanitize_name(metadata["name"])}.png`` so concrete sinks do
        not declare an ``icon`` key in their ``metadata``. Falls back to
        ``default_icon.png`` when the class has no usable ``name``.
        """
        name = (getattr(cls, "metadata", None) or {}).get("name", "")
        try:
            return f"{sanitize_name(name)}.png"
        except ValueError:
            return "default_icon.png"

    # Optional per-service defaulting. Subclasses may override:
    #   * default_api_url — fallback base URL when the target row has none
    #   * api_key_env_var — env var that supplies the API key default
    default_api_url: str = ""
    api_key_env_var: Optional[str] = None

    def __init__(self, target_row: Any, db: Any):
        """*target_row* is an ORM row with attributes:
        ``id, service_id, name, api_url, api_key, remote_target_id, target_extra_params, include_path_in_filename, access_level``
        """
        self.target_id = target_row.id
        self.service_id = target_row.service_id
        self.name = target_row.name
        self.api_url = target_row.api_url
        self.api_key = target_row.api_key  # already decrypted by caller
        self.remote_target_id = target_row.remote_target_id
        self.target_extra_params = target_row.target_extra_params or {}
        self.include_path_in_filename = bool(
            getattr(target_row, "include_path_in_filename", False)
        )
        self.access_level = getattr(target_row, "access_level", "PRIVATE")
        self._output_root = "/output"
        self.db = db
        self._cancel_check = None
        self._progress_cb = None
        self._schedule_window = parse_schedule_window(
            getattr(target_row, "schedule_start", None),
            getattr(target_row, "schedule_end", None),
        )

    def set_cancel_check(self, check) -> None:
        """Install a cancellation callback.

        The recon engine calls this before driving the six abstract methods.
        ``check`` is a zero-arg callable returning a ``SinkCancelledError``
        kind string when the target-subscription link / subscription is no
        longer active, or ``None`` while it is. Sinks never call this
        themselves — they only consult it via ``_check_cancel``.
        """
        self._cancel_check = check

    def _check_cancel(self) -> None:
        """Abort the current remote operation if a cancellation is pending.

        Raises :class:`SinkCancelledError` when the installed check fires.
        No-op when no check is installed (the default). Concrete sinks must
        call this at the top of each remote operation and inside any
        long-running loop / polling loop (mirrors the plugin contract where
        ``progress_callback`` raises ``SubscriptionCancelledError``).
        """
        if self._cancel_check is not None:
            kind = self._cancel_check()
            if kind:
                raise SinkCancelledError(kind)

    def set_progress_callback(self, cb) -> None:
        """Install a progress callback for the recon engine's status updates.

        ``cb`` is called as ``cb(done, in_flight)`` where ``done`` is the
        number of upserts already persisted to the remote and ``in_flight``
        is the size of the batch currently being sent. Batching sinks fire it
        on each batch flush; the recon engine turns it into a human-readable
        ``Upserting X of Y to remote sink...`` status message on the link.
        No-op when never set.
        """
        self._progress_cb = cb

    def _report_progress(self, done: int, in_flight: int) -> None:
        """Notify the installed progress callback (best-effort)."""
        if self._progress_cb is not None:
            try:
                self._progress_cb(done, in_flight)
            except Exception:  # noqa: BLE001
                pass

    def _check_schedule(self) -> None:
        """Abort the recon pass when outside this target's upload window.

        Called by the recon engine before each file *upsert* (add/update).
        Raises :class:`SinkCancelledError` (kind ``"outside_schedule"``) when
        a window is configured and the current local time is outside it.
        Removals are intentionally NOT gated.
        """
        if self._schedule_window is not None and not in_schedule_window(
            datetime.now(), self._schedule_window
        ):
            raise SinkCancelledError("outside_schedule")

    def remote_file_name(self, path: str) -> str:
        """Deterministic remote filename for *path*.

        Default scheme: ``autokb_{target}_{basename}``. When the target has
        ``include_path_in_filename`` enabled, the relative directory structure
        under the output root is folded into the name with underscores::

            autokb_{target}_{rel_dir_with_underscores}_{basename}

        Example with the flag on::

            /output/crawl4AIWebScraperPlugin/Scrapling/02b7.md
            → autokb_ScraplingKB_crawl4AIWebScraperPlugin_Scrapling_02b7.md
        """
        base = f"autokb_{sanitize_name(self.name)}"
        if self.include_path_in_filename:
            rel = os.path.relpath(path, self._output_root)
            if not rel.startswith(".."):
                return f"{base}_{rel.replace(os.sep, '_')}"
        return f"{base}_{os.path.basename(path)}"

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
        Called synchronously by the Manager at target create/update time
        (``_ensure_target_remote``) — never by the recon engine.
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

    def flush(self) -> None:
        """Flush any buffered remote operations (e.g. batched upserts).

        No-op by default. Sinks that buffer work across upsets (such as
        page-bounded upload batches) override this to send their remaining
        pending work; the sink reconciliation engine calls it at the end of
        each target's recon pass and before removals.
        """
        return None

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
