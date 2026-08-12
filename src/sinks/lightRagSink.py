"""LightRAG Sink service — syncs AutoKB output files into a LightRAG instance.

Implements the six abstract ``BaseSink`` methods against the LightRAG REST API
(``{api_url}/documents``). Each Data Target maps to one LightRAG server
instance — a single knowledge base — so document operations run against the
server's startup ``WORKSPACE`` (single-KB mode).

Lifecycle:

  * ``add_datafile`` uploads a file (deterministic ``autokb_{target}_{basename}``
    name) via ``/documents/upload``, then polls ``/documents/track_status``
    until the document id is assigned and returns it as the remote id.
  * ``update_datafile`` deletes the old document (idempotent) and re-uploads the
    changed file, retrying on a transient HTTP 409 while the async delete
    settles the filename.
  * ``remove_datafile`` deletes by document id via ``/documents/delete_document``;
    404 is treated as success so deletes stay idempotent across retries.
  * ``add_target`` returns a deterministic remote_target_id and probes the
    server (``GET /documents/status_counts``) so a bad URL/API key surfaces at
    target-create time. ``remove_target`` is a **no-op** — the connected
    LightRAG server holds a single shared knowledge base, so destroying it when
    the last subscription leaves a target would wipe unrelated data. Multi-KB
    mode later restores per-workspace destruction here. ``clear_target`` calls
    ``DELETE /documents`` (idempotent on 404).

Batched upserts:

  The recon engine drives ``base_add_datafile`` / ``base_update_datafile`` one
  file at a time. Instead of uploading each file immediately, this sink buffers
  the upsert ops into batches bounded by the target's ``pages_per_batch``
  (a first-class target column, min 1 / max 100 / default 10). Each document's
  page count is *estimated* from its token count (~500 tokens/page). A batch is
  considered full when adding another document would push the running total
  over ``pages_per_batch``; a single document larger than the limit is its own
  batch. When a batch flushes:

    1. every file is uploaded (one ``/documents/upload`` per file, tracking
       each ``track_id``);
    2. the sink **blocks** until the entire batch reaches a terminal state
       (``processed`` or ``failed``) via ``/documents/track_status``, raising
       on ``failed`` and pulsing the subscription heartbeat on every poll so
       the watchdog never kills a long wait;
    3. on success the DB rows are written as the base wrappers would have.
    Batches also flush when a removal runs and when the recon pass ends
    (``BaseSink.flush``, invoked by the sink reconciliation engine).

Multi-workspace (multi-KB) extension point: once the connected LightRAG build
routes document operations by a per-request workspace header (e.g.
``LIGHTRAG-KNOWLEDGE-BASE`` or ``LIGHTRAG-WORKSPACE``, with lazy workspace
creation), the sink needs only three changes:

  1. ``_headers()`` adds ``{WORKSPACE_HEADER: self.workspace}``.
  2. ``add_target()`` returns ``self.workspace`` as the remote target id.
  3. ``__init__`` honors ``target_extra_params["workspace"]`` instead of
     warning that it is ignored, and ``remove_target()`` deletes the workspace.

Dependencies: none beyond the image-default ``requests``.
"""

import os
import time

import requests

from utils.misc_utils import SinkCancelledError, sanitize_name
from utils.sink_base import BaseSink, compute_file_hash


_WHITESPACE = frozenset(b" \t\r\n\f\v")
_TOKENS_PER_PAGE = 500  # rough token→page estimate used for batch sizing


class LightRagSink(BaseSink):
    metadata = {
        "name": "lightRagSink",
        "display_name": "LightRAG Knowledge Base",
        "description": (
            "Synchronizes AutoKB output files into a LightRAG knowledge graph. "
            "Each Data Target maps to one LightRAG instance; data files are "
            "uploaded in page-bounded batches with deterministic names, "
            "replaced on change, and deleted when removed from /output/."
        ),
    }
    default_api_url = "http://lightrag-app:9621"
    api_key_env_var = "LIGHTRAG_API_KEY"

    # Multi-KB extension point: header that selects a workspace when the
    # connected LightRAG build supports per-request workspace routing.
    _WORKSPACE_HEADER = "LIGHTRAG-KNOWLEDGE-BASE"
    _TIMEOUT = 60
    _POLL_INTERVAL = 1
    _POLL_TIMEOUT = 120
    _UPDATE_CONFLICT_TIMEOUT = 30
    _CANCEL_CLEANUP_GRACE = 15
    # How long a full batch may wait for processing before raising. Chosen to
    # comfortably exceed realistic LLM-extraction times; the heartbeat keeps
    # the subscription alive for the whole wait.
    _BATCH_COMPLETE_TIMEOUT = 7200

    def __init__(self, target_row, db):
        super().__init__(target_row, db)
        self.api_url = (self.api_url or self.default_api_url).rstrip("/")
        self.api_key = self.api_key or (
            os.environ.get(self.api_key_env_var, "") if self.api_key_env_var else ""
        )
        self.workspace = (self.target_extra_params.get("workspace") or "").strip()
        try:
            ppb = int(getattr(target_row, "pages_per_batch", 10) or 10)
        except (TypeError, ValueError):
            ppb = 10
        self.pages_per_batch = max(1, min(100, ppb))
        # Pending batch state. A batch holds up to ``pages_per_batch``
        # estimated pages of upserts (single over-threshold docs are their own
        # batch); it flushes across the page ceiling, before a removal, and at
        # recon-pass end via ``flush()``.
        self._batch_ops = []
        self._batch_pages = 0

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _headers(self):
        headers = {
            "X-API-Key": self.api_key,
            "Accept": "application/json",
        }
        # Multi-KB extension point: when the connected LightRAG build routes
        # document operations by a workspace header, add
        # ``headers[self._WORKSPACE_HEADER] = self.workspace`` here (and return
        # ``self.workspace`` from ``add_target()``).
        return headers

    def _url(self, endpoint: str) -> str:
        return f"{self.api_url}/{endpoint.lstrip('/')}"

    def _check(self, resp: requests.Response, action: str) -> None:
        if resp.status_code >= 400:
            body = resp.text[:500]
            raise RuntimeError(
                f"{self.metadata['name']} {action} failed: HTTP {resp.status_code}: {body}"
            )

    def _remote_file_name(self, path: str) -> str:
        return self.remote_file_name(path)

    @staticmethod
    def _estimate_pages(path: str) -> int:
        """Estimate a source document's page count from its token count.

        Streams the file in 1 MiB chunks counting whitespace-delimited runs,
        so very large documents are not loaded into memory. Rough estimate —
        exact pagination is not available for markdown/text output files.
        """
        tokens = 0
        in_word = False
        try:
            with open(path, "rb") as f:
                while True:
                    chunk = f.read(1 << 20)
                    if not chunk:
                        break
                    for b in chunk:
                        if b in _WHITESPACE:
                            in_word = False
                        elif not in_word:
                            in_word = True
                            tokens += 1
        except OSError:
            return 1
        return max(1, (tokens + _TOKENS_PER_PAGE - 1) // _TOKENS_PER_PAGE)

    def _do_upload(self, path: str) -> str:
        """Upload *path* and return the LightRAG track_id."""
        fname = self._remote_file_name(path)
        with open(path, "rb") as f:
            files = {"file": (fname, f, "application/octet-stream")}
            resp = requests.post(
                self._url("documents/upload"),
                headers=self._headers(),
                files=files,
                timeout=self._TIMEOUT,
            )
        self._check(resp, "upload datafile")
        track_id = resp.json().get("track_id") if isinstance(resp.json(), dict) else None
        if not track_id:
            raise RuntimeError(f"{self.metadata['name']} upload returned no track_id")
        return track_id

    def _upload_track_after_delete(self, remote_datafile_id: str, path: str) -> str:
        """Delete the old doc, then upload again, returning the new track_id.

        Tolerates a transient 409 while the async delete settles the
        deterministic filename (mirrors ``_upload_after_delete`` but returns a
        track_id so the caller can batch the wait phase).
        """
        self._delete_doc(remote_datafile_id)
        deadline = time.time() + self._UPDATE_CONFLICT_TIMEOUT
        while True:
            self._check_cancel()
            try:
                return self._do_upload(path)
            except RuntimeError as exc:
                if "HTTP 409" not in str(exc):
                    raise
                if time.time() >= deadline:
                    raise RuntimeError(
                        f"{self.metadata['name']} update datafile {remote_datafile_id} "
                        f"timed out waiting for the old document to be deleted: {exc}"
                    )
                time.sleep(self._POLL_INTERVAL)

    def _wait_for_doc_id(self, track_id: str, max_wait: float) -> str | None:
        """Poll track_status until a document id appears; return it or None.

        Raises RuntimeError if the document lands in FAILED state (e.g. an
        async content-duplicate detection). Checks cancellation on every poll.
        """
        deadline = time.time() + max_wait
        while time.time() < deadline:
            self._check_cancel()
            resp = requests.get(
                self._url(f"documents/track_status/{track_id}"),
                headers=self._headers(),
                timeout=self._TIMEOUT,
            )
            self._check(resp, f"track status {track_id}")
            data = resp.json()
            documents = data.get("documents") if isinstance(data, dict) else None
            if documents:
                first = documents[0]
                if first.get("status") == "FAILED":
                    raise RuntimeError(
                        f"{self.metadata['name']} upload failed processing: "
                        f"{first.get('error_msg') or 'unknown error'}"
                    )
                if first.get("id"):
                    return first["id"]
            time.sleep(self._POLL_INTERVAL)
        return None

    def _resolve_doc_id_best_effort(self, track_id: str, max_wait: float) -> str | None:
        """Ignore cancellation briefly to resolve an orphaned upload's doc id."""
        deadline = time.time() + max_wait
        while time.time() < deadline:
            try:
                resp = requests.get(
                    self._url(f"documents/track_status/{track_id}"),
                    headers=self._headers(),
                    timeout=self._TIMEOUT,
                )
            except Exception:
                time.sleep(self._POLL_INTERVAL)
                continue
            if resp.status_code < 400:
                documents = resp.json().get("documents") if isinstance(resp.json(), dict) else None
                if documents and documents[0].get("id"):
                    return documents[0]["id"]
            time.sleep(self._POLL_INTERVAL)
        return None

    def _raw_delete_doc(self, remote_datafile_id: str) -> None:
        """DELETE a document without any cancellation check (cleanup helper)."""
        try:
            resp = requests.delete(
                self._url("documents/delete_document"),
                headers=self._headers(),
                json={"doc_ids": [remote_datafile_id], "delete_file": True},
                timeout=self._TIMEOUT,
            )
        except Exception:
            return
        if resp.status_code == 404:
            return  # already gone → idempotent
        if resp.status_code < 400:
            return
        status = resp.json().get("status") if isinstance(resp.json(), dict) else None
        if status == "busy":
            pass  # best-effort only — swallow

    def _delete_doc(self, remote_datafile_id: str) -> None:
        self._check_cancel()
        resp = requests.delete(
            self._url("documents/delete_document"),
            headers=self._headers(),
            json={"doc_ids": [remote_datafile_id], "delete_file": True},
            timeout=self._TIMEOUT,
        )
        if resp.status_code == 404:
            return  # already gone → idempotent
        self._check(resp, f"delete datafile {remote_datafile_id}")
        status = resp.json().get("status") if isinstance(resp.json(), dict) else None
        if status == "busy":
            raise RuntimeError(
                f"{self.metadata['name']} delete datafile {remote_datafile_id} "
                "refused: pipeline busy"
            )

    def _clear_documents(self) -> None:
        resp = requests.delete(
            self._url("documents"),
            headers=self._headers(),
            timeout=self._TIMEOUT,
        )
        if resp.status_code == 404:
            return  # already gone → idempotent
        self._check(resp, "clear documents")
        status = resp.json().get("status") if isinstance(resp.json(), dict) else None
        if status in ("busy", "fail"):
            raise RuntimeError(
                f"{self.metadata['name']} clear documents returned status={status}"
            )

    def _kb_files(self, kb_id: str | None = None) -> list:
        """Enumerate the documents currently on the remote knowledge base.

        Single-KB mode means the connected LightRAG instance is one shared
        knowledge base, so ``kb_id`` is accepted purely for signature
        compatibility with the recon engine's Pass III heal pass and ignored.
        Returns ``[{"id": ...}]`` for every document in *all* processing
        statuses, so a healing decision never mistakes a still-pending
        document for a deleted one.
        """
        self._check_cancel()
        resp = requests.get(
            self._url("documents"),
            headers=self._headers(),
            timeout=self._TIMEOUT,
        )
        self._check(resp, "list documents")
        data = resp.json()
        statuses = data.get("statuses") if isinstance(data, dict) else None
        if not isinstance(statuses, dict):
            raise RuntimeError(
                f"{self.metadata['name']} list documents returned an unexpected payload"
            )
        results = []
        seen = set()
        for docs in statuses.values():
            if isinstance(docs, list):
                for doc in docs:
                    if isinstance(doc, dict) and doc.get("id") and doc["id"] not in seen:
                        seen.add(doc["id"])
                        results.append({"id": doc["id"]})
        return results

    def _upload_file(self, path: str) -> str:
        """Upload *path* and return its document id, cleaning up on cancel."""
        self._check_cancel()
        track_id = self._do_upload(path)
        try:
            doc_id = self._wait_for_doc_id(track_id, self._POLL_TIMEOUT)
        except SinkCancelledError:
            # Cancel fired while the upload was landing — best-effort remove
            # the now-orphaned document so a future add of the same file does
            # not collide on its deterministic filename.
            orphan = self._resolve_doc_id_best_effort(track_id, self._CANCEL_CLEANUP_GRACE)
            if orphan:
                self._raw_delete_doc(orphan)
            raise
        if not doc_id:
            raise RuntimeError(f"{self.metadata['name']} upload of {path} did not produce a document id")
        return doc_id

    def _upload_after_delete(self, remote_datafile_id: str, path: str) -> str:
        """Delete the old doc, then re-upload, tolerating a transient 409."""
        self._delete_doc(remote_datafile_id)
        deadline = time.time() + self._UPDATE_CONFLICT_TIMEOUT
        while True:
            self._check_cancel()
            try:
                return self._upload_file(path)
            except RuntimeError as exc:
                # 409 means the async delete has not yet freed the deterministic
                # filename; keep retrying until the window elapses.
                if "HTTP 409" not in str(exc):
                    raise
                if time.time() >= deadline:
                    raise RuntimeError(
                        f"{self.metadata['name']} update datafile {remote_datafile_id} "
                        f"timed out waiting for the old document to be deleted: {exc}"
                    )
                time.sleep(self._POLL_INTERVAL)

    # ------------------------------------------------------------------
    # Batched upsert machinery
    # ------------------------------------------------------------------
    def _enqueue_upsert(self, op: dict) -> None:
        """Queue one add/update op into the current page-bounded batch.

        Flushes the pending batch first when adding *op* would push the
        running page total over ``pages_per_batch`` so every flushed batch
        stays at or under the ceiling (an over-threshold single doc becomes
        its own batch).
        """
        pages = op.get("pages", 1)
        if self._batch_ops and self._batch_pages + pages > self.pages_per_batch:
            self._flush_ops(self._batch_ops)
            self._batch_ops = []
            self._batch_pages = 0
        self._batch_ops.append(op)
        self._batch_pages += pages

    def _wait_for_batch(self, track_ids) -> dict:
        """Block until all *track_ids* resolve to terminal documents.

        Returns ``{track_id: doc_id}`` once every document is ``processed``.
        Raises on ``failed`` or on timeout. Checks cancellation on every poll,
        which also pulses the recon heartbeat so the watchdog stays satisfied
        during a long wait. On any failure, best-effort removes documents that
        were uploaded before the failure so a later retry does not collide on
        the deterministic filenames.
        """
        seen_ids = {}
        resolved = {}
        pending = set(track_ids)
        deadline = time.time() + self._BATCH_COMPLETE_TIMEOUT
        try:
            while pending:
                self._check_cancel()
                for tid in list(pending):
                    resp = requests.get(
                        self._url(f"documents/track_status/{tid}"),
                        headers=self._headers(),
                        timeout=self._TIMEOUT,
                    )
                    self._check(resp, f"track status {tid}")
                    data = resp.json()
                    documents = data.get("documents") if isinstance(data, dict) else None
                    if not documents:
                        continue
                    first = documents[0]
                    status = str(first.get("status") or "").lower()
                    if status == "failed":
                        raise RuntimeError(
                            f"{self.metadata['name']} upload failed processing: "
                            f"{first.get('error_msg') or 'unknown error'}"
                        )
                    doc_id = first.get("id")
                    if doc_id:
                        seen_ids[tid] = doc_id
                    if doc_id and status == "processed":
                        resolved[tid] = doc_id
                        pending.discard(tid)
                if pending and time.time() >= deadline:
                    raise RuntimeError(
                        f"{self.metadata['name']} batch did not complete within "
                        f"{int(self._BATCH_COMPLETE_TIMEOUT)}s; unfinished: "
                        f"{sorted(pending)}"
                    )
                if pending:
                    time.sleep(self._POLL_INTERVAL)
            return dict(resolved)
        except Exception:
            for doc_id in set(seen_ids.values()):
                self._raw_delete_doc(doc_id)
            raise

    def _flush_ops(self, ops: list) -> None:
        """Upload *ops* as one batch, wait for full completion, then persist.

        Order of operations for data integrity:

          1. Stage every file upload (adds upload directly; updates delete the
             old doc first). Collects one ``track_id`` per op.
          2. Wait until the ENTIRE batch has finished processing on the remote.
          3. Write the DB rows the base wrappers would have written.
        """
        if not ops:
            return
        staged = []
        for op in ops:
            self._check_cancel()
            if op.get("op") == "update":
                track_id = self._upload_track_after_delete(
                    op["remote_datafile_id"], op["path"]
                )
            else:
                track_id = self._do_upload(op["path"])
            staged.append((op, track_id))

        doc_ids = self._wait_for_batch([tid for _, tid in staged])

        for op, track_id in staged:
            doc_id = doc_ids.get(track_id)
            if not doc_id:
                raise RuntimeError(
                    f"{self.metadata['name']} batch upload of {op['path']} "
                    "did not produce a document id"
                )
            if op.get("op") == "update":
                self.db.update_target_datafile_remote_id(
                    self.target_id, op["datafile_id"], doc_id
                )
                self.db.update_target_datafile_hash(
                    self.target_id, op["datafile_id"], op["hash"]
                )
            else:
                self.db.insert_target_datafile(
                    self.target_id, op["datafile_id"], doc_id, op["hash"]
                )

    def flush(self) -> None:
        """Send any pending upsert batch (no-op when nothing is buffered)."""
        ops = self._batch_ops
        self._batch_ops = []
        self._batch_pages = 0
        self._flush_ops(ops)

    # ------------------------------------------------------------------
    # The six abstract methods
    # ------------------------------------------------------------------
    def add_datafile(self, path: str) -> str:
        self._check_cancel()
        return self._upload_file(path)

    def update_datafile(self, remote_datafile_id: str, path: str) -> str:
        self._check_cancel()
        return self._upload_after_delete(remote_datafile_id, path)

    def remove_datafile(self, remote_datafile_id: str) -> None:
        self._check_cancel()
        self._delete_doc(remote_datafile_id)

    def add_target(self) -> str:
        self._check_cancel()
        # Probe for connectivity + API key validity so a misconfigured target
        # fails fast at create/update time instead of on the first recon.
        resp = requests.get(
            self._url("documents/status_counts"),
            headers=self._headers(),
            timeout=self._TIMEOUT,
        )
        self._check(resp, "probe target")
        # Single-KB mode: the whole instance is the knowledge base, so the
        # remote target id is a deterministic label. Multi-KB mode returns
        # self.workspace here instead.
        return f"autokb_{sanitize_name(self.name)}"

    def remove_target(self) -> None:
        # No-op on purpose: the connected LightRAG server holds a single
        # shared knowledge base. Deleting all documents here when the last
        # subscription leaves one target would destroy unrelated data. Once
        # multi-KB routing exists, delete this target's workspace instead.
        pass

    def clear_target(self) -> None:
        self._check_cancel()
        if not self.remote_target_id:
            return
        self._clear_documents()

    # ------------------------------------------------------------------
    # Base wrapper overrides — batched upserts for the recon engine
    # ------------------------------------------------------------------
    def base_add_datafile(self, sub_id: str, path: str, known_hash: str | None = None) -> None:
        """Register an add with the page-bounded batch (DB rows on flush)."""
        self._check_cancel()
        size = os.path.getsize(path)
        mtime = os.path.getmtime(path)
        datafile_hash = known_hash if known_hash is not None else compute_file_hash(path)

        # get-or-create akb_datafile
        df = self.db.get_or_create_datafile(sub_id, path, size, mtime, datafile_hash)

        # check if already tracked for this target
        existing = self.db.get_target_datafile(self.target_id, df.id)
        if existing:
            return  # already added

        self._enqueue_upsert({
            "op": "add",
            "path": path,
            "datafile_id": df.id,
            "hash": datafile_hash,
            "pages": self._estimate_pages(path),
        })

    def base_update_datafile(self, datafile_id: str, new_hash: str) -> None:
        """Register an update with the page-bounded batch (DB rows on flush)."""
        self._check_cancel()
        t_df = self.db.get_target_datafile(self.target_id, datafile_id)
        if not t_df:
            return
        df = self.db.get_datafile(datafile_id)
        if not df:
            return
        self._enqueue_upsert({
            "op": "update",
            "path": df.path,
            "datafile_id": datafile_id,
            "remote_datafile_id": t_df.remote_datafile_id,
            "hash": new_hash,
            "pages": self._estimate_pages(df.path),
        })

    def base_remove_datafile(self, datafile_id: str) -> None:
        """Delete a file immediately, flushing any pending upserts first."""
        self.flush()
        t_df = self.db.get_target_datafile(self.target_id, datafile_id)
        if not t_df:
            return
        self.remove_datafile(t_df.remote_datafile_id)
        self.db.delete_target_datafile(self.target_id, datafile_id)


__all__ = ["LightRagSink"]