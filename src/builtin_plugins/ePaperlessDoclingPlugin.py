"""Queries Paperless-ngx documents matching a filter, sends new/changed
documents to Docling for OCR/conversion, and writes full markdown output."""

import os
import re
import time

import requests
import tiktoken

from utils.misc_utils import (
    SubscriptionCancelledError,
    sanitize_name,
    send_smtp_notification,
)
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
_MAX_FILENAME_LEN = 240
_DISK_NAME_RE = re.compile(
    r"^(?P<stem>.+)_(?P<doc_id>\d+)\.(?P<hash>[a-f0-9]{16})"
    r"(?:-(?P<chunk>\d+))?\.(?P<ext>[a-z0-9]+)$"
)
_DOT_DISK_NAME_RE = re.compile(
    r"^(?P<stem>.+)\.(?P<doc_id>\d+)\.(?P<hash>[a-f0-9]{16})"
    r"(?:-(?P<chunk>\d+))?\.(?P<ext>[a-z0-9]+)$"
)
_LEGACY_DISK_NAME_RE = re.compile(
    r"^(?P<doc_id>\d+)\.(?P<hash>[a-f0-9]{16})"
    r"(?:-(?P<chunk>\d+))?\.(?P<ext>[a-z0-9]+)$"
)


class DoclingAuthError(RuntimeError):
    """Fatal Docling authentication/authorization failure."""


class ePaperlessDoclingPlugin(BaseSubscription):
    metadata = {
        "name": "ePaperlessDoclingPlugin",
        "display_name": "Paperless Docling Parser",
        "description": (
            "Queries Paperless-ngx documents matching a filter, sends new and "
            "changed documents to Docling for OCR and parsing, and writes "
            "chunked markdown output (~490 tokens per chunk)."
        ),
        "sub_type": "SCHEDULED",
    }

    def get_schema(self):
        return {
            "type": "object",
            "properties": {
                "document_filter": {
                    "type": "string",
                    "default": "",
                    "format": "textarea",
                    "description": "Filter params appended to the /api/documents/ call to select documents. Leave blank to return all documents. Scope to a storage path with storage_path__id.",
                    "ui_hint": "Example: storage_path__id=3&owner__id=7&document_type__id=2&tags__id__in=3,5",
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
                    "default": False,
                    "description": "Chunk output (~490 tokens per file). Disable for one markdown file per document.",
                },
                "processing_mode": {
                    "type": "string",
                    "enum": ["Paperless Content", "Parse With Docling", "Raw PDF"],
                    "default": "Paperless Content",
                    "description": (
                        "Paperless Content: reuse Paperless's own OCR content "
                        "instead of re-parsing with Docling (fetches the content "
                        "field first, falls back to Docling when empty). "
                        "Parse With Docling: download the PDF and re-parse it "
                        "with Docling. "
                        "Raw PDF: download the stored PDF and write it as the "
                        "output instead of markdown."
                    ),
                },
            },
            "required": [
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
        try:
            self._run(config, progress_callback)
        except SubscriptionCancelledError:
            raise
        except Exception as exc:
            self._halt_run(exc, progress_callback)

    def _run(self, config, progress_callback):
        progress_callback(0, message="Starting...")

        pl_url = (config.get("paperless_url") or os.environ.get("PAPERLESS_URL") or "http://paperless-app:8000").rstrip("/")
        pl_token = config.get("paperless_token") or os.environ.get("PAPERLESS_TOKEN") or ""
        dl_url = (config.get("docling_url") or os.environ.get("DOCLING_URL") or "http://docling-app:5001").rstrip("/")
        dl_key = config.get("docling_api_key") or os.environ.get("DOCLING_API_KEY") or ""
        document_filter = config.get("document_filter", "")
        chunking_enabled = config.get("chunking_enabled", False)
        processing_mode = config.get("processing_mode", "Paperless Content")
        use_paperless_content = processing_mode == "Paperless Content"
        raw_pdf = processing_mode == "Raw PDF"

        pl_headers = {"Authorization": f"Token {pl_token}"}

        self.log.info(
            "plugin_start",
            document_filter=document_filter or "(none)",
        )

        # 1. Query Paperless for documents matching the filter
        progress_callback(5, message="Querying Paperless...")
        documents = self._query_documents(
            pl_url, pl_headers, document_filter
        )

        if not documents:
            self.log.warning(
                "no_documents",
                document_filter=document_filter or "(none)",
                message="No documents matched the filter; reconciling output to empty",
            )

        doc_count = len(documents)
        progress_callback(5, message=f"{doc_count} documents found")

        # 3. Build API index
        api_index = {}
        for doc in documents:
            doc_id = doc["id"]
            checksum = (doc["versions"][0]["checksum"] or "").lower()
            original_file_name = (
                doc.get("original_file_name") or f"document-{doc_id}.pdf"
            )
            stem, source_ext = self._original_name_parts(
                original_file_name, doc_id
            )
            if doc.get("archived_file_name"):
                source_ext = "pdf"
            api_index[doc_id] = {
                "checksum": checksum,
                "checksum_prefix": checksum[:16],
                "title": doc.get("title", "Untitled"),
                "original_file_name": original_file_name,
                "stem": stem,
                "ext": source_ext,
            }

        # 4. Build disk index from output directory
        output_dir = self.get_destination_path()
        disk_index = {}
        if os.path.isdir(output_dir):
            for fname in os.listdir(output_dir):
                if not os.path.isfile(os.path.join(output_dir, fname)):
                    continue
                parsed = self._parse_disk_name(fname)
                if not parsed:
                    continue
                stem, doc_id, checksum_prefix, chunked, ext = parsed
                entry = disk_index.setdefault(
                    doc_id,
                    {
                        "stems": set(),
                        "hashes": set(),
                        "chunked": False,
                        "exts": set(),
                        "files": [],
                    },
                )
                entry["stems"].add(stem)
                entry["hashes"].add(checksum_prefix)
                entry["chunked"] = entry["chunked"] or chunked
                entry["exts"].add(ext)
                entry["files"].append(fname)

        expected_chunked = False if raw_pdf else chunking_enabled

        # 5. Three-way set comparison
        api_ids = set(api_index.keys())
        disk_ids = set(disk_index.keys())

        to_add = api_ids - disk_ids
        to_update = set()
        to_delete = disk_ids - api_ids
        to_skip = set()

        for doc_id in api_ids & disk_ids:
            disk_entry = disk_index[doc_id]
            api_entry = api_index[doc_id]
            expected_ext = api_entry["ext"] if raw_pdf else "md"
            checksum_matches = disk_entry["hashes"] == {
                api_entry["checksum_prefix"]
            }
            name_matches = disk_entry["stems"] == {api_entry["stem"]}
            pattern_matches = (
                disk_entry["chunked"] == expected_chunked
                and disk_entry["exts"] == {expected_ext}
            )
            if checksum_matches and name_matches and pattern_matches:
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

        # 6. Process adds and updates
        for doc_id in sorted(to_add | to_update):
            entry = api_index[doc_id]
            original_name = entry["original_file_name"]
            checksum_prefix = entry["checksum_prefix"]

            pct = 10 + int(80 * done_work / total_work) if total_work else 90
            progress_callback(pct, message=f"Processing {original_name}")

            try:
                # Remove any stale output files for an update before writing
                # fresh ones (including legacy doc-id-only names).
                if doc_id in to_update:
                    self._remove_doc_files(output_dir, doc_id)

                if raw_pdf:
                    file_bytes = self._download_document(
                        pl_url, pl_headers, doc_id
                    )
                    output_name = self._output_filename(
                        entry["stem"], doc_id, checksum_prefix, entry["ext"]
                    )
                    tmp = f"/tmp/{output_name}"
                    with open(tmp, "wb") as f:
                        f.write(file_bytes)
                    self.move_to_destination(tmp)
                else:
                    if use_paperless_content:
                        content_text = self._fetch_content(pl_url, pl_headers, doc_id)
                    else:
                        content_text = ""
                    if content_text:
                        chunks = self._chunks_from_content(
                            dl_url, dl_key, content_text, original_name,
                            chunking_enabled, progress_callback, pct,
                        )
                    else:
                        pdf_bytes = self._download_document(
                            pl_url, pl_headers, doc_id
                        )
                        chunks = self._process_with_docling(
                            dl_url, dl_key, pdf_bytes, original_name, chunking_enabled, progress_callback, pct
                        )
                    for chunk in chunks:
                        chunk_index = (
                            chunk["chunk_index"] if chunking_enabled else None
                        )
                        output_name = self._output_filename(
                            entry["stem"], doc_id, checksum_prefix, "md",
                            chunk_index,
                        )
                        tmp = f"/tmp/{output_name}"
                        with open(tmp, "w", encoding="utf-8") as f:
                            f.write(f"Original file: {original_name}\n\n{chunk['text']}")
                        self.move_to_destination(tmp)
            except Exception as exc:
                # Any per-document failure aborts the run: the wrapper emails
                # once and the subscription stays enabled for the next run.
                self.log.error("doc_processing_failed", doc_id=doc_id, file=original_name, error=str(exc))
                raise

            done_work += 1

        # 7. Process deletions
        for doc_id in sorted(to_delete):
            pct = 10 + int(80 * done_work / total_work) if total_work else 90
            progress_callback(pct, message=f"Removing document {doc_id}")

            self._remove_doc_files(output_dir, doc_id)
            self.log.debug("deleted", doc_id=doc_id)
            done_work += 1

        # 8. Stray file cleanup
        if os.path.isdir(output_dir):
            stray_count = 0
            for fname in os.listdir(output_dir):
                if not os.path.isfile(os.path.join(output_dir, fname)):
                    continue
                parsed = self._parse_disk_name(fname)
                if parsed:
                    if parsed[1] not in api_ids:
                        os.remove(os.path.join(output_dir, fname))
                        stray_count += 1
                elif not fname.startswith("."):
                    # The subscription owns this directory. Remove any regular
                    # file that is not a recognized output name.
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

        progress_callback(100, message=", ".join(msg_parts))
        self.log.info("plugin_complete", **{k: v for k, v in (("added", len(to_add)), ("updated", len(to_update)), ("deleted", len(to_delete)), ("skipped", len(to_skip)))})

    def _halt_run(self, exc, progress_callback):
        """Halt the current run after a failure.

        Logs the failure, sends a single SMTP notification, and reports 100%
        progress so the run ends cleanly without transitioning the subscription
        to ERROR — the next scheduled run will retry.
        """
        self.log.error("run_aborted", error=f"{type(exc).__name__}: {exc}")
        subject = f"[AutoKB] {self.metadata.get('display_name', 'Paperless Docling Parser')} run aborted"
        body = (
            f"Subscription {self._subscription_name!r} aborted during execution.\n\n"
            f"Error ({type(exc).__name__}): {exc}\n\n"
            "No further documents were processed and no changes were made. "
            "The subscription remains enabled and will retry on its next scheduled run."
        )
        try:
            sent = send_smtp_notification(subject=subject, body=body)
        except Exception:
            sent = False
        if not sent:
            self.log.warning("notification_send_failed", error="smtp")
        progress_callback(100, message=f"Run aborted (emailed): {exc}")

    # ------------------------------------------------------------------
    # Output naming helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _original_name_parts(original_file_name, doc_id):
        """Return a sanitized (stem, extension) from a Paperless filename."""
        base = os.path.basename(original_file_name or "")
        stem, ext = os.path.splitext(base)
        if not stem:
            stem, ext = base, ""
        stem = stem.strip(".")
        try:
            stem = sanitize_name(stem)
        except ValueError:
            stem = f"document-{doc_id}"
        if not stem:
            stem = f"document-{doc_id}"

        ext = ext.lstrip(".").lower()
        if not re.fullmatch(r"[a-z0-9]+", ext):
            ext = "pdf"
        return stem, ext

    @staticmethod
    def _output_filename(stem, doc_id, checksum_prefix, ext, chunk_index=None):
        """Build ``<original-stem>_<doc-id>.<hash>[-<chunk>].<ext>``."""
        chunk_suffix = (
            f"-{chunk_index:03d}" if chunk_index is not None else ""
        )
        suffix = f"_{doc_id}.{checksum_prefix}{chunk_suffix}.{ext}"
        max_stem_len = _MAX_FILENAME_LEN - len(suffix)
        if max_stem_len < 1:
            max_stem_len = 1
        if len(stem) > max_stem_len:
            stem = stem[:max_stem_len].rstrip(".")
            if not stem:
                stem = "d"
        return f"{stem}{suffix}"

    @staticmethod
    def _parse_disk_name(filename):
        """Parse a new or legacy output name.

        Returns ``(stem, doc_id, hash, chunked, ext)``. ``stem`` is ``None``
        for legacy ``<doc_id>.<hash>[-<chunk>].<ext>`` names and for the
        intermediate dot-separated original-name format.
        """
        match = _DISK_NAME_RE.match(filename)
        if match:
            return (
                match.group("stem"),
                int(match.group("doc_id")),
                match.group("hash"),
                match.group("chunk") is not None,
                match.group("ext"),
            )

        # Files written by the previous version used
        # ``<original-stem>.<doc-id>.<hash>[-<chunk>].<ext>``. Treat them as
        # stale so reconciliation replaces them with underscore-separated
        # names instead of skipping them forever.
        match = _DOT_DISK_NAME_RE.match(filename)
        if match:
            return (
                None,
                int(match.group("doc_id")),
                match.group("hash"),
                match.group("chunk") is not None,
                match.group("ext"),
            )

        match = _LEGACY_DISK_NAME_RE.match(filename)
        if match:
            return (
                None,
                int(match.group("doc_id")),
                match.group("hash"),
                match.group("chunk") is not None,
                match.group("ext"),
            )
        return None

    @classmethod
    def _remove_doc_files(cls, output_dir, doc_id):
        """Remove every new- or legacy-named file belonging to ``doc_id``."""
        removed = 0
        if not os.path.isdir(output_dir):
            return removed
        for fname in os.listdir(output_dir):
            path = os.path.join(output_dir, fname)
            if not os.path.isfile(path):
                continue
            parsed = cls._parse_disk_name(fname)
            if parsed and parsed[1] == doc_id:
                os.remove(path)
                removed += 1
        return removed

    # ------------------------------------------------------------------
    # Paperless helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _query_documents(base_url, headers, document_filter=""):
        from urllib.parse import parse_qsl
        params = dict(parse_qsl(document_filter)) if document_filter else {}
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

    @staticmethod
    def _fetch_content(base_url, headers, doc_id):
        resp = requests.get(
            f"{base_url}/api/documents/{doc_id}/",
            headers=headers,
            params={"fields": "content"},
            timeout=30,
        )
        resp.raise_for_status()
        return (resp.json().get("content") or "").strip()

    # ------------------------------------------------------------------
    # Docling helpers
    # ------------------------------------------------------------------
    def _process_with_docling(
        self, base_url, api_key, pdf_bytes, filename, chunking_enabled, progress_callback, current_pct
    ):
        dl_headers = {}
        if api_key:
            dl_headers["x-api-key"] = api_key

        if chunking_enabled:
            return self._chunk_with_docling(
                base_url, dl_headers, pdf_bytes, filename, "application/pdf",
                progress_callback, current_pct,
            )

        files = {"files": (filename, pdf_bytes, "application/pdf")}
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
        if resp.status_code in (401, 403):
            raise DoclingAuthError(
                f"Docling auth failed (HTTP {resp.status_code}): {resp.text[:300]}"
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

    def _chunks_from_content(
        self, base_url, api_key, content, filename, chunking_enabled, progress_callback, current_pct
    ):
        if not chunking_enabled:
            return [{"chunk_index": 0, "text": content}]

        text = f"Original file: {filename}\n\n{content}"
        dl_headers = {}
        if api_key:
            dl_headers["x-api-key"] = api_key
        stem, ext = os.path.splitext(filename)
        upload_name = f"{stem}.txt" if ext else f"{filename}.txt"
        return self._chunk_with_docling(
            base_url, dl_headers, text.encode("utf-8"), upload_name, "text/plain",
            progress_callback, current_pct,
        )

    def _chunk_with_docling(
        self, base_url, dl_headers, file_bytes, filename, content_type, progress_callback, current_pct
    ):
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

        files = {"files": (filename, file_bytes, content_type)}

        resp = requests.post(
            f"{base_url}/v1/chunk/hybrid/file/async",
            headers=dl_headers, files=files, data=data, timeout=30,
        )
        if resp.status_code in (401, 403):
            raise DoclingAuthError(
                f"Docling auth failed (HTTP {resp.status_code}): {resp.text[:300]}"
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
