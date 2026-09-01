"""RagflowSink service — syncs AutoKB output files into a RAGFlow dataset.

Each AutoKB Data Target maps to one RAGFlow dataset (knowledge base), created
as ``AutoKB_{target}``. Files are uploaded as documents and chunk parsing is
triggered asynchronously (fire-and-forget — upload returns before RAGFlow
finishes indexing). Documents are re-created on content change (RAGFlow cannot
edit a document in place) and removed when they disappear from ``/output/``.

RAGFlow REST API (``{api_url}/api/v1``), auth: ``Authorization: Bearer <key>``.
The API reports failures with HTTP 200 and a non-zero ``code`` in the body,
which ``_check`` inspects. The dataset is provisioned with the ``General``
chunking method (``chunk_method: "naive"``) and the Docling layout parser
(``parser_config: {"layout_recognize": "Docling"}``) so PDFs are recognized
with Docling rather than the default DeepDoc. RAGFlow dataset visibility is
``me`` (private) or ``team`` (shared with the tenant team); a PUBLIC target
maps to ``team``.

Required dependencies: ``requests`` and ``utils.misc_utils`` (already in the
AutoKB runtime image).
"""

import os

import requests

from utils.misc_utils import guess_content_type
from utils.sink_base import BaseSink


class RagflowSink(BaseSink):
    metadata = {
        "name": "ragflowSink",
        "display_name": "RAGFlow Dataset",
        "description": (
            "Synchronizes AutoKB output files into a RAGFlow dataset "
            "(knowledge base). Each Data Target creates one dataset "
            "named AutoKB_{target}; documents are uploaded with the General "
            "chunking method and Docling parser, re-created on change, and "
            "removed on deletion."
        ),
    }
    default_api_url = "http://ragflow-app"
    api_key_env_var = "RAGFLOW_API_KEY"
    _TIMEOUT = 60
    _PAGE_SIZE = 100

    def __init__(self, target_row, db):
        super().__init__(target_row, db)
        self.api_url = (self.api_url or self.default_api_url).rstrip("/")
        self.api_key = self.api_key or (
            os.environ.get(self.api_key_env_var, "") if self.api_key_env_var else ""
        )
        self._api_root = self.api_url + "/api/v1"

    # ---- HTTP helpers ----

    def _headers(self):
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/json",
        }

    def _url(self, endpoint: str) -> str:
        return f"{self._api_root}/{endpoint.lstrip('/')}"

    def _try_json(self, resp):
        try:
            return resp.json()
        except ValueError:
            return None

    def _check(self, resp: requests.Response, action: str) -> None:
        if resp.status_code >= 400:
            body = resp.text[:500]
            raise RuntimeError(f"ragflow {action} failed: HTTP {resp.status_code}: {body}")
        body = self._try_json(resp)
        if body is None:
            raise RuntimeError(f"ragflow {action} failed: non-JSON response: {resp.text[:500]}")
        if body.get("code") != 0:
            raise RuntimeError(f"ragflow {action} failed: {body.get('message') or body}")

    def _not_found(self, body) -> bool:
        if not isinstance(body, dict):
            return False
        msg = str(body.get("message", ""))
        return any(
            token in msg
            for token in (
                "not belong to dataset",
                "Document not found",
                "lacks permission for dataset",
                "don't own the dataset",
                "doesn't own the dataset",
            )
        )

    def _kb_name(self) -> str:
        return f"AutoKB_{self.name}"

    def _remote_file_name(self, path: str) -> str:
        return self.remote_file_name(path)

    def _delete_docs(self, doc_ids, action: str) -> None:
        self._check_cancel()
        resp = requests.delete(
            self._url(f"datasets/{self.remote_target_id}/documents"),
            headers=self._headers(),
            json={"ids": doc_ids},
            timeout=self._TIMEOUT,
        )
        body = self._try_json(resp)
        if body is not None and body.get("code") != 0 and self._not_found(body):
            return  # already gone → idempotent
        self._check(resp, action)

    def _list_docs(self, kb_id: str):
        page = 1
        collected = []
        while True:
            self._check_cancel()
            resp = requests.get(
                self._url(f"datasets/{kb_id}/documents"),
                headers=self._headers(),
                params={"page": page, "page_size": self._PAGE_SIZE},
                timeout=self._TIMEOUT,
            )
            self._check(resp, "list documents")
            data = resp.json().get("data") or {}
            docs = data.get("docs") or []
            collected.extend(docs)
            total = int(data.get("total", 0) or 0)
            if not docs or page * self._PAGE_SIZE >= total:
                break
            page += 1
        return collected

    def _upload_doc(self, path: str) -> str:
        self._check_cancel()
        fname = self._remote_file_name(path)
        with open(path, "rb") as f:
            resp = requests.post(
                self._url(f"datasets/{self.remote_target_id}/documents"),
                headers=self._headers(),
                files={"file": (fname, f, guess_content_type(path))},
                timeout=self._TIMEOUT,
            )
        self._check(resp, "upload document")
        docs = (resp.json() or {}).get("data") or []
        if not docs or not docs[0].get("id"):
            raise RuntimeError("ragflow upload returned no document id")
        return docs[0]["id"]

    def _trigger_parse(self, doc_id: str) -> None:
        self._check_cancel()
        resp = requests.post(
            self._url(f"datasets/{self.remote_target_id}/chunks"),
            headers=self._headers(),
            json={"document_ids": [doc_id]},
            timeout=self._TIMEOUT,
        )
        self._check(resp, "start document parse")

    def _upload_and_parse(self, path: str) -> str:
        doc_id = self._upload_doc(path)
        try:
            self._trigger_parse(doc_id)
        except Exception:
            # The document is on the remote but not tracked locally — remove
            # the orphan so the next recon retries cleanly.
            try:
                self._delete_docs([doc_id], "cleanup after upload")
            except Exception:
                pass
            raise
        return doc_id

    # ---- abstract methods ----

    def add_datafile(self, path: str) -> str:
        self._check_cancel()
        return self._upload_and_parse(path)

    def update_datafile(self, remote_datafile_id: str, path: str) -> str:
        # RAGFlow cannot replace a document's content in place — remove the
        # old document first (so the re-upload keeps the deterministic name
        # instead of becoming a "name(1)" duplicate), then upload the new one.
        self._delete_docs([remote_datafile_id], "update datafile")
        return self._upload_and_parse(path)

    def remove_datafile(self, remote_datafile_id: str) -> None:
        self._check_cancel()
        self._delete_docs([remote_datafile_id], "remove datafile")

    def add_target(self) -> str:
        self._check_cancel()
        payload = {
            "name": self._kb_name(),
            "chunk_method": "naive",  # "General" in the RAGFlow UI
            "permission": "team" if self.access_level == "PUBLIC" else "me",
            "parser_config": {"layout_recognize": "Docling"},
        }
        resp = requests.post(
            self._url("datasets"),
            headers=self._headers(),
            json=payload,
            timeout=self._TIMEOUT,
        )
        self._check(resp, "add target")
        dataset_id = ((resp.json() or {}).get("data") or {}).get("id")
        if not dataset_id:
            raise RuntimeError("ragflow add target returned no dataset id")
        return dataset_id

    def remove_target(self) -> None:
        self._check_cancel()
        dataset_id = self.remote_target_id
        if not dataset_id:
            return
        resp = requests.delete(
            self._url("datasets"),
            headers=self._headers(),
            json={"ids": [dataset_id]},
            timeout=self._TIMEOUT,
        )
        body = self._try_json(resp)
        if body is not None and body.get("code") != 0 and self._not_found(body):
            return  # already gone → idempotent
        self._check(resp, "remove target")

    def clear_target(self) -> None:
        self._check_cancel()
        dataset_id = self.remote_target_id
        if not dataset_id:
            return
        resp = requests.delete(
            self._url(f"datasets/{dataset_id}/documents"),
            headers=self._headers(),
            json={"delete_all": True},
            timeout=self._TIMEOUT,
        )
        self._check(resp, "clear target")

    def _kb_files(self, kb_id: str):
        return [{"id": d["id"]} for d in self._list_docs(kb_id) if d.get("id")]


__all__ = ["RagflowSink"]