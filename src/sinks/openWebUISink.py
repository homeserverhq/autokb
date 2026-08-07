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

import os
import time

import requests

from utils.misc_utils import sanitize_name
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
        "icon": "openwebui.png",
    }
    default_api_url = "http://openwebui-app:8080"
    api_key_env_var = "OPENWEBUI_API_KEY"

    def __init__(self, target_row, db):
        super().__init__(target_row, db)
        self.api_url = (self.api_url or self.default_api_url).rstrip("/")
        self.api_key = self.api_key or (os.environ.get(self.api_key_env_var, "") if self.api_key_env_var else "")
        self._api_root = self.api_url + "/api/v1"
        self._timeout = 60
        self._process_timeout = 300
        self._process_interval = 1

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
        return f"autokb_{sanitize_name(self.name)}_{os.path.basename(path)}"

    def _paginated_get(self, endpoint: str):
        items = []
        page = 1
        while True:
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
        fname = self._remote_file_name(path)
        with open(path, "rb") as f:
            files = {"file": (fname, f, "application/octet-stream")}
            resp = requests.post(
                self._url("files/"),
                headers=self._headers(),
                files=files,
                timeout=self._timeout,
            )
        self._check(resp, "upload file")
        file_id = resp.json().get("id")
        if not file_id:
            raise RuntimeError("openWebUI upload returned no file id")
        if not self._wait_for_processing(file_id):
            raise RuntimeError(f"openWebUI file {file_id} did not finish processing")
        return file_id

    def _wait_for_processing(self, file_id: str) -> bool:
        deadline = time.time() + self._process_timeout
        while time.time() < deadline:
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
        resp = requests.post(
            self._url(f"knowledge/{kb_id}/file/add"),
            headers=self._headers(),
            json={"file_id": file_id},
            timeout=self._timeout,
        )
        self._check(resp, f"link file {file_id} to KB {kb_id}")

    def _delete_file(self, file_id: str) -> None:
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

    def _create_kb(self, name: str) -> str:
        resp = requests.post(
            self._url("knowledge/create"),
            headers=self._headers(),
            json={"name": name, "description": f"AutoKB sync: {name}"},
            timeout=self._timeout,
        )
        self._check(resp, "create KB")
        return resp.json().get("id")

    # ---- abstract methods ----

    def add_datafile(self, path: str) -> str:
        file_id = self._upload_file(path)
        try:
            self._link_to_kb(self.remote_target_id, file_id)
        except Exception:
            try:
                self._delete_file(file_id)
            except Exception:
                pass
            raise
        return file_id

    def update_datafile(self, remote_datafile_id: str, path: str) -> str:
        new_id = self._upload_file(path)
        if new_id == remote_datafile_id:
            return new_id  # remote deduped by content → nothing to clean up
        try:
            self._link_to_kb(self.remote_target_id, new_id)
        except Exception:
            try:
                self._delete_file(new_id)
            except Exception:
                pass
            raise
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
