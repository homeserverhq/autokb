"""Watches a Paperless-ngx storage path, sends new/changed documents to
Docling for OCR/conversion, and writes full markdown output."""

import os
import re
import time

import requests
import tiktoken

from utils.plugin_base import BaseSubscription

_DOCLING_OPTIONS = {
    "convert_do_ocr": True,
    "convert_force_ocr": True,
    "convert_ocr_engine": "easyocr",
    "convert_pdf_backend": "docling_parse",
    "convert_table_mode": "accurate",
    "convert_image_export_mode": "placeholder",
    "convert_document_timeout": 86400,
    "chunking_use_markdown_tables": True,
    "chunking_merge_peers": True,
}
_CHUNKING_MAX_TOKENS = 490
_HEARTBEAT_INTERVAL = 20


class ePaperlessDoclingPlugin(BaseSubscription):
    metadata = {
        "name": "ePaperlessDoclingPlugin",
            "display_name": "ePaperlessDoclingPlugin",
        "icon": "ePaperlessDoclingPlugin.png",
        "description": (
            "Watches a Paperless-ngx storage path, sends new and changed "
            "documents to Docling for OCR and parsing, and writes chunked "
            "markdown output (~490 tokens per chunk)."
        ),
        "sub_type": "SCHEDULED",
    }
    DEFAULT_ACCESS_LEVEL = "PRIVATE"

    def get_schema(self):
        return {
            "type": "object",
            "properties": {
                "storage_path_id": {
                    "type": "integer",
                    "minimum": 1,
                    "description": "Paperless storage path ID to watch",
                },
                "owner_username": {
                    "type": "string",
                    "default": "",
                    "description": (
                        "Optional: only process documents owned by this "
                        "username. Required for storage paths that use "
                        "{{owner_username}} in their template (e.g. "
                        "PersonalProcessed). Leave empty for shared paths."
                    ),
                },
                "paperless_url": {
                    "type": "string",
                    "default": os.environ.get(
                        "PAPERLESS_URL", "http://paperless-app:8000"
                    ),
                    "description": "Paperless API base URL",
                },
                "paperless_token": {
                    "type": "string",
                    "format": "password",
                    "default": os.environ.get("PAPERLESS_TOKEN", ""),
                    "description": "Paperless API authentication token",
                },
                "docling_url": {
                    "type": "string",
                    "default": os.environ.get(
                        "DOCLING_URL", "http://docling-app:5001"
                    ),
                    "description": "Docling API base URL",
                },
                "docling_api_key": {
                    "type": "string",
                    "format": "password",
                    "default": os.environ.get("DOCLING_API_KEY", ""),
                    "description": "Docling API key",
                },
                "chunking_enabled": {
                    "type": "boolean",
                    "default": True,
                    "description": "Chunk output (~490 tokens per file). Disable for one markdown file per document.",
                },
            },
            "required": [
                "storage_path_id",
                "paperless_url",
                "paperless_token",
                "docling_url",
                "docling_api_key",
            ],
        }

    # ------------------------------------------------------------------
    # getData
    # ------------------------------------------------------------------
    def getData(self, config, progress_callback):
        progress_callback(0, message="Starting...")

        pl_url = (config.get("paperless_url") or os.environ.get("PAPERLESS_URL") or "http://paperless-app:8000").rstrip("/")
        pl_token = config.get("paperless_token") or os.environ.get("PAPERLESS_TOKEN") or ""
        dl_url = (config.get("docling_url") or os.environ.get("DOCLING_URL") or "http://docling-app:5001").rstrip("/")
        dl_key = config.get("docling_api_key") or os.environ.get("DOCLING_API_KEY") or ""
        sp_id = config["storage_path_id"]
        owner_username = config.get("owner_username", "").strip()
        chunking_enabled = config.get("chunking_enabled", True)

        pl_headers = {"Authorization": f"Token {pl_token}"}

        self.log.info(
            "plugin_start",
            storage_path_id=sp_id,
            owner_username=owner_username or "(none)",
        )

        # 1. Resolve owner username to ID if provided
        owner_id = None
        if owner_username:
            progress_callback(2, message="Resolving owner...")
            try:
                owner_id = self._resolve_owner(pl_url, pl_headers, owner_username)
                self.log.info("owner_resolved", username=owner_username, id=owner_id)
            except Exception as exc:
                self.log.error("owner_resolve_failed", username=owner_username, error=str(exc))
                progress_callback(100, message="Failed to resolve owner username")
                return

        # 2. Query Paperless for documents in the storage path
        progress_callback(5, message="Querying Paperless...")
        try:
            documents = self._query_documents(
                pl_url, pl_headers, sp_id, owner_id
            )
        except Exception as exc:
            self.log.error("paperless_query_failed", error=str(exc))
            progress_callback(100, message="Paperless query failed")
            return

        if not documents:
            progress_callback(100, message="No documents found")
            return

        doc_count = len(documents)
        progress_callback(5, message=f"{doc_count} documents found")

        # 3. Build API index
        api_index = {}
        for doc in documents:
            doc_id = doc["id"]
            checksum = doc["versions"][0]["checksum"]
            api_index[doc_id] = {
                "checksum": checksum,
                "checksum_prefix": checksum[:16],
                "title": doc.get("title", "Untitled"),
                "original_file_name": doc.get("original_file_name", f"document-{doc_id}.pdf"),
            }

        # 4. Build disk index from output directory
        output_dir = self.get_destination_path()
        disk_index = {}  # doc_id -> {"prefix": str, "chunked": bool}
        if os.path.isdir(output_dir):
            for fname in os.listdir(output_dir):
                if not fname.endswith(".md"):
                    continue
                match = re.match(r"^(\d+)\.([a-f0-9]{16})-\d+\.md$", fname)
                if match:
                    disk_index[int(match.group(1))] = {"prefix": match.group(2), "chunked": True}
                    continue
                match = re.match(r"^(\d+)\.([a-f0-9]{16})\.md$", fname)
                if match:
                    disk_index[int(match.group(1))] = {"prefix": match.group(2), "chunked": False}

        # 5. Three-way set comparison
        api_ids = set(api_index.keys())
        disk_ids = set(disk_index.keys())

        to_add = api_ids - disk_ids
        to_update = set()
        to_delete = disk_ids - api_ids
        to_skip = set()

        for doc_id in api_ids & disk_ids:
            disk_entry = disk_index[doc_id]
            checksum_matches = api_index[doc_id]["checksum"].startswith(disk_entry["prefix"])
            pattern_matches = disk_entry["chunked"] == chunking_enabled
            if checksum_matches and pattern_matches:
                to_skip.add(doc_id)
            else:
                to_update.add(doc_id)

        self.log.info(
            "reconciliation",
            add=len(to_add),
            update=len(to_update),
            delete=len(to_delete),
            skip=len(to_skip),
        )

        total_work = len(to_add) + len(to_update) + len(to_delete) + len(to_skip)
        done_work = 0
        failed = 0

        # 6. Process adds and updates
        for doc_id in sorted(to_add | to_update):
            entry = api_index[doc_id]
            original_name = entry["original_file_name"]
            checksum_prefix = entry["checksum_prefix"]

            pct = 10 + int(80 * done_work / total_work) if total_work else 90
            progress_callback(pct, message=f"Processing {original_name}")

            try:
                pdf_bytes = self._download_document(
                    pl_url, pl_headers, doc_id
                )
                chunks = self._process_with_docling(
                    dl_url, dl_key, pdf_bytes, original_name, chunking_enabled, progress_callback, pct
                )
                for chunk in chunks:
                    suffix = f"-{chunk['chunk_index']:03d}" if chunking_enabled else ""
                    tmp = f"/tmp/{doc_id}.{checksum_prefix}{suffix}.md"
                    with open(tmp, "w", encoding="utf-8") as f:
                        f.write(f"Original file: {original_name}\n\n{chunk['text']}")
                    self.move_to_destination(tmp)

                # Delete stale files if this is an update
                if doc_id in to_update:
                    for fname in os.listdir(output_dir):
                        if re.match(rf"^{doc_id}\.", fname):
                            os.remove(os.path.join(output_dir, fname))
            except Exception as exc:
                self.log.warning("doc_skipped", doc_id=doc_id, error=str(exc))
                failed += 1

            done_work += 1

        # 7. Process deletions
        for doc_id in sorted(to_delete):
            pct = 10 + int(80 * done_work / total_work) if total_work else 90
            progress_callback(pct, message=f"Removing {disk_index[doc_id]}")

            for fname in os.listdir(output_dir):
                if re.match(rf"^{doc_id}\.", fname):
                    os.remove(os.path.join(output_dir, fname))
            self.log.debug("deleted", doc_id=doc_id)
            done_work += 1

        # 8. Stray file cleanup
        if os.path.isdir(output_dir):
            stray_count = 0
            for fname in os.listdir(output_dir):
                if not fname.endswith(".md"):
                    continue
                match = re.match(r"^(\d+)\.([a-f0-9]{16})(?:-\d+)?\.md$", fname)
                if match and int(match.group(1)) not in api_ids:
                    os.remove(os.path.join(output_dir, fname))
                    stray_count += 1
            if stray_count:
                progress_callback(95, message=f"Cleaning up {stray_count} stray files")
                self.log.info("stray_removed", count=stray_count)

        # 9. Done
        msg_parts = [
            f"{len(to_add)} added",
            f"{len(to_update)} updated",
            f"{len(to_delete)} deleted",
            f"{len(to_skip)} unchanged",
        ]
        if failed:
            msg_parts.append(f"{failed} failed")

        progress_callback(100, message=", ".join(msg_parts))
        self.log.info("plugin_complete", **{k: v for k, v in (("added", len(to_add)), ("updated", len(to_update)), ("deleted", len(to_delete)), ("skipped", len(to_skip)), ("failed", failed))})

    # ------------------------------------------------------------------
    # Paperless helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _resolve_owner(base_url, headers, username):
        resp = requests.get(
            f"{base_url}/api/users/?username__iexact={username}",
            headers=headers,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        if data["count"] == 0:
            raise ValueError(f"User '{username}' not found")
        return data["results"][0]["id"]

    @staticmethod
    def _query_documents(base_url, headers, storage_path_id, owner_id=None):
        params = {"storage_path__id": storage_path_id}
        if owner_id is not None:
            params["owner__id"] = owner_id
        all_results = []
        url = f"{base_url}/api/documents/"
        while url:
            resp = requests.get(url, headers=headers, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            all_results.extend(data["results"])
            url = data.get("next")
            params = {}
        return all_results

    @staticmethod
    def _download_document(base_url, headers, doc_id):
        resp = requests.get(
            f"{base_url}/api/documents/{doc_id}/download/",
            headers=headers,
            timeout=120,
        )
        resp.raise_for_status()
        return resp.content

    # ------------------------------------------------------------------
    # Docling helpers
    # ------------------------------------------------------------------
    def _process_with_docling(
        self, base_url, api_key, pdf_bytes, filename, chunking_enabled, progress_callback, current_pct
    ):
        dl_headers = {}
        if api_key:
            dl_headers["x-api-key"] = api_key

        files = {"files": (filename, pdf_bytes, "application/pdf")}

        if chunking_enabled:
            overhead = len(tiktoken.get_encoding("cl100k_base").encode(
                f"Original file: {filename}\n\n"
            ))
            data = {
                **_DOCLING_OPTIONS,
                "chunking_max_tokens": _CHUNKING_MAX_TOKENS - overhead,
            }
            bool_keys = [
                "convert_do_ocr",
                "convert_force_ocr",
                "chunking_use_markdown_tables",
                "chunking_merge_peers",
            ]
            for bool_key in bool_keys:
                data[bool_key] = str(data[bool_key]).lower()
            data["convert_document_timeout"] = str(data["convert_document_timeout"])

            resp = requests.post(
                f"{base_url}/v1/chunk/hybrid/file/async",
                headers=dl_headers, files=files, data=data, timeout=30,
            )
            resp.raise_for_status()
            task_id = resp.json()["task_id"]

            self.log.info("docling_chunk_submitted", task_id=task_id, filename=filename)
            self._poll_docling_task(base_url, dl_headers, task_id, progress_callback, current_pct)

            resp = requests.get(
                f"{base_url}/v1/result/{task_id}", headers=dl_headers, timeout=30,
            )
            resp.raise_for_status()
            result_data = resp.json()

            chunks = result_data.get("chunks", [])
            if not chunks:
                raise RuntimeError("Docling returned empty chunks")
            return chunks

        else:
            data = {
                "to_formats": "md",
                "convert_do_ocr": str(True).lower(),
                "convert_force_ocr": str(True).lower(),
                "convert_ocr_engine": "easyocr",
                "convert_pdf_backend": "docling_parse",
                "convert_table_mode": "accurate",
                "convert_image_export_mode": "placeholder",
                "convert_document_timeout": str(86400),
            }

            resp = requests.post(
                f"{base_url}/v1/convert/file/async",
                headers=dl_headers, files=files, data=data, timeout=30,
            )
            resp.raise_for_status()
            task_id = resp.json()["task_id"]

            self.log.info("docling_convert_submitted", task_id=task_id, filename=filename)
            self._poll_docling_task(base_url, dl_headers, task_id, progress_callback, current_pct)

            resp = requests.get(
                f"{base_url}/v1/result/{task_id}", headers=dl_headers, timeout=30,
            )
            resp.raise_for_status()
            result_data = resp.json()

            if result_data["status"] != "success":
                raise RuntimeError(f"Docling conversion failed: {result_data.get('status')}")

            md_content = result_data.get("document", {}).get("md_content", "")
            if not md_content:
                raise RuntimeError("Docling returned empty md_content")

            return [{"chunk_index": 0, "text": md_content}]

    def _poll_docling_task(self, base_url, headers, task_id, progress_callback, current_pct):
        poll_url = f"{base_url}/v1/status/poll/{task_id}"
        last_heartbeat = time.time()

        while True:
            try:
                resp = requests.get(
                    poll_url, params={"wait": 30}, headers=headers, timeout=35
                )
                resp.raise_for_status()
                data = resp.json()
            except requests.RequestException as exc:
                self.log.error("docling_poll_error", task_id=task_id, error=str(exc))
                raise

            status = data["task_status"]

            now = time.time()
            if now - last_heartbeat >= _HEARTBEAT_INTERVAL:
                progress_callback(current_pct)
                last_heartbeat = now

            if status == "success":
                return data
            elif status == "failure":
                err_msg = data.get("error_message", "Unknown error")
                self.log.error("docling_job_failed", task_id=task_id, error=err_msg)
                raise RuntimeError(f"Docling failed: {err_msg}")
