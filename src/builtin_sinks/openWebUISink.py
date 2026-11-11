"""OpenWebUI Sink service — syncs files to an Open WebUI Knowledge Base.

Implements the six abstract ``BaseSink`` methods against the Open WebUI
REST API (``{api_url}/api/v1``). The upload/link lifecycle mirrors the legacy
``owui_sync`` flow:

  * A Knowledge Base (``AutoKB_{target}``) is the remote target.
  * Files are uploaded to the global file repository, then *linked* into the
    Knowledge Base via ``knowledge/{kb_id}/file/add``.
  * Updates upload a new version, link it, then delete the old one so no
    orphaned versions linger. Deletes are idempotent (404 == success).
"""

import hashlib
import os
import time

import requests

from utils.misc_utils import (
    SinkCancelledError,
    SinkDuplicateContent,
    get_logger,
    guess_content_type,
    sanitize_name,
)
from utils.sink_base import BaseSink


class OpenWebUISink(BaseSink):
    metadata = {
        "name": "openWebUISink",
        "display_name": "Open WebUI Knowledge Base",
        "description": (
            "Synchronizes AutoKB output files into an Open WebUI Knowledge "
            "Base. Each Data Target creates its own knowledge base "
            "(AutoKB_{target}); data files are uploaded to the file "
            "repository, linked into the knowledge base, and replaced "
            "on update so no orphaned versions linger."
        ),
    }
    default_api_url = "http://openwebui-app:8080"
    api_key_env_var = "OPENWEBUI_API_KEY"
    # OpenWebUI keeps every uploaded file in ONE global file repo and links it
    # into per-target knowledge bases, so a file can be referenced by several
    # targets at once. Removal must never delete a still-shared file.
    shared_file_store = True

    def __init__(self, target_row, db):
        super().__init__(target_row, db)
        self.api_url = (self.api_url or self.default_api_url).rstrip("/")
        self.api_key = self.api_key or (os.environ.get(self.api_key_env_var, "") if self.api_key_env_var else "")
        self._api_root = self.api_url + "/api/v1"
        # Short timeout for metadata/list/delete calls; uploads and KB links
        # can trigger synchronous indexing/embedding and legitimately take
        # much longer for large documents.
        self._timeout = 60
        self._operation_timeout = 300
        self._process_timeout = 300
        self._process_interval = 1
        self._log = get_logger("openWebUISink")

    # ---- HTTP helpers ----

    def _headers(self):
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/json",
        }

    def _url(self, endpoint: str) -> str:
        return f"{self._api_root}/{endpoint.lstrip('/')}"

    def _check(self, resp: requests.Response, action: str) -> None:
        if resp.status_code >= 400:
            body = resp.text[:500]
            raise RuntimeError(f"openWebUI {action} failed: HTTP {resp.status_code}: {body}")

    def _kb_name(self) -> str:
        return f"AutoKB_{self.name}"

    def _remote_file_name(self, path: str) -> str:
        return self.remote_file_name(path)

    def _paginated_get(self, endpoint: str):
        items = []
        page = 1
        while True:
            self._check_cancel()
            resp = requests.get(
                self._url(endpoint),
                params={"page": page},
                headers=self._headers(),
                timeout=self._timeout,
            )
            self._check(resp, f"list {endpoint}")
            data = resp.json()
            batch = data.get("items") if isinstance(data, dict) else data
            if not batch:
                break
            items.extend(batch)
            page += 1
        return items

    def _upload_file(self, path: str) -> str:
        """Upload *path* to the global file repo and wait for processing."""
        self._check_cancel()
        fname = self._remote_file_name(path)
        with open(path, "rb") as f:
            files = {"file": (fname, f, guess_content_type(path))}
            resp = requests.post(
                self._url("files/"),
                headers=self._headers(),
                files=files,
                timeout=self._operation_timeout,
            )
        self._check(resp, "upload file")
        file_id = resp.json().get("id")
        if not file_id:
            raise RuntimeError("openWebUI upload returned no file id")
        try:
            if not self._wait_for_processing(file_id):
                raise RuntimeError(f"openWebUI file {file_id} did not finish processing")
        except SinkCancelledError:
            # Cancel fired while the file was processing — best-effort remove
            # the uploaded-but-unlinked file so nothing is orphaned in the
            # repo. Never delete when another linked file owns the same hash.
            self._safe_delete_unlinked(file_id)
            raise
        return file_id

    def _wait_for_processing(self, file_id: str) -> bool:
        deadline = time.time() + self._process_timeout
        while time.time() < deadline:
            self._check_cancel()
            resp = requests.get(
                self._url(f"files/{file_id}/process/status"),
                headers=self._headers(),
                timeout=self._timeout,
            )
            if resp.status_code == 404:
                return True  # status endpoint unavailable → assume processed
            if resp.status_code >= 400:
                raise RuntimeError(
                    f"openWebUI processing status failed: HTTP {resp.status_code}: {resp.text[:500]}"
                )
            status = resp.json().get("status") if isinstance(resp.json(), dict) else None
            if status == "completed":
                return True
            if status in ("failed", "error"):
                return False
            time.sleep(self._process_interval)
        return False

    def _link_to_kb(self, kb_id: str, file_id: str) -> None:
        self._check_cancel()
        resp = requests.post(
            self._url(f"knowledge/{kb_id}/file/add"),
            headers=self._headers(),
            json={"file_id": file_id},
            timeout=self._operation_timeout,
        )
        self._check(resp, f"link file {file_id} to KB {kb_id}")

    def _delete_file(self, file_id: str) -> None:
        self._check_cancel()
        resp = requests.delete(
            self._url(f"files/{file_id}"),
            headers=self._headers(),
            timeout=self._timeout,
        )
        if resp.status_code == 404:
            return  # already gone → idempotent
        self._check(resp, f"delete file {file_id}")

    def _delete_kb(self, kb_id: str) -> None:
        resp = requests.delete(
            self._url(f"knowledge/{kb_id}/delete"),
            headers=self._headers(),
            timeout=self._timeout,
        )
        if resp.status_code == 404:
            return
        self._check(resp, f"delete KB {kb_id}")

    def _kb_files(self, kb_id: str):
        return self._paginated_get(f"knowledge/{kb_id}/files")

    def _is_duplicate_error(self, exc: Exception) -> bool:
        """True when the exception is OpenWebUI's duplicate-content rejection."""
        return "Duplicate content detected" in str(exc)

    def _content_hash(self, file_id: str) -> str:
        """Content hash of a processed file, computed exactly like OpenWebUI's
        ``calculate_sha256_string`` (sha256 of the extracted UTF-8 text). The
        dedup logic stores this hash in ``metadata.hash`` / ``file.hash``, so a
        match against the KB file listing identifies the file holding the same
        content under a different id.
        """
        resp = requests.get(
            self._url(f"files/{file_id}/data/content"),
            headers=self._headers(),
            timeout=self._timeout,
        )
        self._check(resp, f"get content for {file_id}")
        payload = resp.json() if isinstance(resp.json(), dict) else {}
        content = payload.get("content", "")
        if not isinstance(content, str) or not content:
            raise RuntimeError(f"openWebUI file {file_id} has no retrievable content")
        return hashlib.sha256(content.encode("utf-8")).hexdigest()

    def _find_duplicate_owner(self, new_file_id: str):
        """Return ``(owner_id, content_hash)`` for a duplicate rejection.

        ``owner_id`` is the currently linked file that owns the same content,
        or ``None`` when the content hash is held only by state we cannot see
        (e.g. orphaned OpenWebUI vectors). Lookup errors propagate so the
        caller can treat them as transient rather than permanently blocked.
        """
        content_hash = self._content_hash(new_file_id)
        kb_files = self._kb_files(self.remote_target_id)
        for f in kb_files:
            if f.get("id") != new_file_id and f.get("hash") == content_hash:
                return f.get("id"), content_hash
        self._log.error(
            "sink_duplicate_unrecoverable",
            target_id=self.target_id,
            file_id=new_file_id,
            content_hash=content_hash,
            kb_files=len(kb_files),
            reason="no_linked_owner",
        )
        return None, content_hash

    def _safe_delete_unlinked(self, file_id: str, content_hash: str = None) -> None:
        """Delete an uploaded-but-unlinked file only when it cannot be the
        hash owner of another linked file.

        OpenWebUI's file delete can remove KB points by content hash, so
        deleting a duplicate upload would wipe the very vectors that caused
        the rejection. When the hash cannot be determined, leave the upload
        alone rather than risk data loss.
        """
        try:
            content_hash = content_hash or self._content_hash(file_id)
            for f in self._kb_files(self.remote_target_id):
                if f.get("id") != file_id and f.get("hash") == content_hash:
                    self._log.warning(
                        "sink_cleanup_skipped_hash_owner",
                        target_id=self.target_id,
                        file_id=file_id,
                        content_hash=content_hash,
                    )
                    return
            self._delete_file(file_id)
        except Exception as exc:  # noqa: BLE001
            self._log.warning(
                "sink_cleanup_skipped_unknown",
                target_id=self.target_id,
                file_id=file_id,
                error=str(exc),
            )

    def _handle_duplicate(self, path: str, file_id: str):
        """Resolve a duplicate rejection by reuse, or raise a typed block.

        Returns the existing linked owner id when one exists. Never deletes
        the existing owner or the fresh upload: either delete could remove
        vectors shared by hash.
        """
        try:
            owner_id, content_hash = self._find_duplicate_owner(file_id)
        except Exception as exc:  # noqa: BLE001
            self._log.error(
                "sink_duplicate_lookup_failed",
                target_id=self.target_id,
                file_id=file_id,
                error=str(exc),
            )
            raise
        if owner_id:
            self._log.info(
                "sink_duplicate_reused",
                target_id=self.target_id,
                file_id=file_id,
                owner_id=owner_id,
                content_hash=content_hash,
            )
            return owner_id
        raise SinkDuplicateContent(
            path=path,
            file_id=file_id,
            content_hash=content_hash,
            reason="orphan_vectors",
        )

    def _create_kb(self, name: str) -> str:
        payload = {"name": name, "description": f"AutoKB sync: {name}"}
        if self.access_level == "PUBLIC":
            payload["access_grants"] = [
                {"principal_type": "user", "principal_id": "*", "permission": "read"}
            ]
        resp = requests.post(
            self._url("knowledge/create"),
            headers=self._headers(),
            json=payload,
            timeout=self._timeout,
        )
        self._check(resp, "create KB")
        return resp.json().get("id")

    # ---- abstract methods ----

    def add_datafile(self, path: str) -> str:
        file_id = self._upload_file(path)
        try:
            self._link_to_kb(self.remote_target_id, file_id)
        except RuntimeError as exc:
            if self._is_duplicate_error(exc):
                return self._handle_duplicate(path, file_id)
            self._safe_delete_unlinked(file_id)
            raise
        return file_id

    def update_datafile(self, remote_datafile_id: str, path: str,
                        previous_hash: str = None, new_hash: str = None,
                        datafile_id: str = None) -> str:
        new_id = self._upload_file(path)
        if new_id == remote_datafile_id:
            return new_id  # remote deduped by content → nothing to clean up
        duplicate_reuse = False
        try:
            self._link_to_kb(self.remote_target_id, new_id)
        except RuntimeError as exc:
            if not self._is_duplicate_error(exc):
                self._safe_delete_unlinked(new_id)
                raise
            owner_id = self._handle_duplicate(path, new_id)
            duplicate_reuse = True
            new_id = owner_id
        if remote_datafile_id and remote_datafile_id != new_id and not duplicate_reuse:
            if previous_hash and new_hash and previous_hash == new_hash:
                self._log.warning(
                    "sink_old_version_retained_same_hash",
                    target_id=self.target_id,
                    old_id=remote_datafile_id,
                    new_id=new_id,
                )
            elif datafile_id and self.db.count_remote_datafile_references(
                remote_datafile_id,
                excluding_target_id=self.target_id,
                excluding_datafile_id=datafile_id,
            ):
                self._log.info(
                    "sink_old_version_retained_shared",
                    target_id=self.target_id,
                    old_id=remote_datafile_id,
                    new_id=new_id,
                )
            else:
                self._delete_file(remote_datafile_id)
        return new_id

    def remove_datafile(self, remote_datafile_id: str) -> None:
        self._delete_file(remote_datafile_id)

    def add_target(self) -> str:
        kb_id = self._create_kb(self._kb_name())
        if not kb_id:
            raise RuntimeError("openWebUI create KB returned no id")
        return kb_id

    def remove_target(self) -> None:
        kb_id = self.remote_target_id
        if not kb_id:
            return
        for f in self._kb_files(kb_id):
            if f.get("id"):
                self._delete_file(f["id"])
        self._delete_kb(kb_id)

    def clear_target(self) -> None:
        kb_id = self.remote_target_id
        if not kb_id:
            return
        for f in self._kb_files(kb_id):
            if f.get("id"):
                self._delete_file(f["id"])


__all__ = ["OpenWebUISink"]
