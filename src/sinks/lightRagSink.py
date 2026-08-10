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
    target-create time. ``remove_target`` / ``clear_target`` call
    ``DELETE /documents`` (idempotent on 404).

Multi-workspace (multi-KB) extension point: once the connected LightRAG build
routes document operations by a per-request workspace header (e.g.
``LIGHTRAG-KNOWLEDGE-BASE`` or ``LIGHTRAG-WORKSPACE``, with lazy workspace
creation), the sink needs only three changes:

  1. ``_headers()`` adds ``{WORKSPACE_HEADER: self.workspace}``.
  2. ``add_target()`` returns ``self.workspace`` as the remote target id.
  3. ``__init__`` honors ``target_extra_params["workspace"]`` instead of
     warning that it is ignored.

Dependencies: none beyond the image-default ``requests``.
"""

import os
import time

import requests

from utils.misc_utils import SinkCancelledError, sanitize_name
from utils.sink_base import BaseSink


class LightRagSink(BaseSink):
    metadata = {
        "name": "lightRagSink",
        "display_name": "LightRAG Knowledge Base",
        "description": (
            "Synchronizes AutoKB output files into a LightRAG knowledge graph. "
            "Each Data Target maps to one LightRAG instance; data files are "
            "uploaded with deterministic names, replaced on change, and "
            "deleted when removed from /output/."
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

    def __init__(self, target_row, db):
        super().__init__(target_row, db)
        self.api_url = (self.api_url or self.default_api_url).rstrip("/")
        self.api_key = self.api_key or (
            os.environ.get(self.api_key_env_var, "") if self.api_key_env_var else ""
        )
        self.workspace = (self.target_extra_params.get("workspace") or "").strip()
        if self.workspace:
            self.log.warning(
                "workspace_ignored",
                action="init",
                workspace=self.workspace,
                result="multi-workspace not yet supported by this LightRAG build",
            )

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
                try:
                    self._delete_doc(orphan)
                except Exception:
                    pass
            raise
        if not doc_id:
            raise RuntimeError(f"{self.metadata['name']} upload of {path} did not produce a document id")
        return doc_id

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
        self._check_cancel()
        if not self.remote_target_id:
            return
        self._clear_documents()

    def clear_target(self) -> None:
        self._check_cancel()
        if not self.remote_target_id:
            return
        self._clear_documents()


__all__ = ["LightRagSink"]
