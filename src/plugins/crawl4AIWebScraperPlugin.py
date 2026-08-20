"""Crawl4AI web scraper plugin: Deep-crawls a website via BFS and stores
extracted markdown, one file per page."""

import hashlib
import json
import os
import re
import time
from urllib.parse import urlparse

import requests
import tiktoken

from utils.plugin_base import BaseSubscription

_API_URL = os.environ.get("CRAWL4AI_API_URL", "http://crawl4ai-app:11235")
_DOCLING_OPTIONS = {
    "convert_document_timeout": 86400,
}
_HEARTBEAT_INTERVAL = 20


class DoclingAuthError(RuntimeError):
    """Fatal Docling authentication/authorization failure."""


def _url_hash(url: str) -> str:
    return hashlib.sha256(url.encode()).hexdigest()[:16]


def _content_hash_prefix(content: str) -> str:
    return hashlib.sha256(content.encode()).hexdigest()[:16]


class crawl4AIWebScraperPlugin(BaseSubscription):
    metadata = {
        "name": "crawl4AIWebScraperPlugin",
        "display_name": "Crawl4AI Web Scraper",
        "description": (
            "Deep-crawls a website via the Crawl4AI API (BFS strategy) and "
            "stores the extracted markdown content, one file per page. "
            "Optionally chunks output through Docling (~490 tokens)."
        ),
        "sub_type": "SCHEDULED",
    }

    def get_schema(self):
        return {
            "type": "object",
            "properties": {
                "url": {
                    "type": "string",
                    "minLength": 1,
                    "description": "Base URL of the website to crawl (e.g. https://example.com)",
                },
                "max_depth": {
                    "type": "integer",
                    "minimum": 0,
                    "default": 10,
                    "description": "Crawl depth beyond the start page (0 = no limit)",
                },
                "max_pages": {
                    "type": "integer",
                    "minimum": 0,
                    "default": 0,
                    "description": "Maximum pages to crawl (0 = unlimited)",
                },
                "chunking_enabled": {
                    "type": "boolean",
                    "default": False,
                    "description": "Chunk page content through Docling (~490 tokens per file). Disable for one file per page.",
                },
                "docling_url": {
                    "type": "string",
                    "default": os.environ.get("DOCLING_URL", "http://docling-app:5001"),
                    "description": "Docling API base URL (only used when chunking)",
                },
                "docling_api_key": {
                    "type": "string",
                    "format": "password",
                    "default": os.environ.get("DOCLING_API_KEY", ""),
                    "description": "Docling API key (only used when chunking)",
                },
            },
            "required": ["url"],
        }

    def getData(self, config, progress_callback):
        url = config["url"]
        max_depth = config.get("max_depth", 10)
        max_pages = config.get("max_pages", 0)
        chunking_enabled = config.get("chunking_enabled", False)
        dl_url = (config.get("docling_url") or os.environ.get("DOCLING_URL") or "http://docling-app:5001").rstrip("/")
        dl_key = config.get("docling_api_key") or os.environ.get("DOCLING_API_KEY") or ""

        self.log.info(
            "webscraper_start",
            url=url,
            max_depth=max_depth,
            max_pages=max_pages,
            chunking_enabled=chunking_enabled,
        )
        progress_callback(0, message="Starting crawl...")

        progress_callback(2, message="Submitting crawl job...")
        task_id = self._submit_job(url, max_depth, max_pages)

        results = self._poll_job(task_id, progress_callback)

        if not results:
            self.log.warning("webscraper_no_results", url=url)
            progress_callback(100, message="No pages returned")
            return

        total_api = len(results)
        self.log.info("webscraper_api_results", count=total_api)
        progress_callback(10, message=f"API returned {total_api} pages")

        # Build API index from results
        api_index = {}
        for r in results:
            if not r.get("success"):
                self.log.warning(
                    "webscraper_page_failed",
                    url=r.get("url"),
                    error=r.get("error_message"),
                )
                continue
            page_url = r.get("url", "")
            if not page_url:
                continue
            uh = _url_hash(page_url)
            md = r.get("markdown", {})
            content = (
                md.get("raw_markdown", "")
                if isinstance(md, dict)
                else (md or "")
            )
            api_index[uh] = {
                "url": page_url,
                "content": content,
                "content_prefix": _content_hash_prefix(content),
            }

        if not api_index:
            progress_callback(100, message="No successful pages")
            return

        # Build disk index from output directory
        output_dir = self.get_destination_path()
        disk_index = {}  # uh -> {"prefix": str|None, "chunked": bool|None}
        if os.path.isdir(output_dir):
            for fname in os.listdir(output_dir):
                if not fname.endswith(".md"):
                    continue
                match = re.match(r"^([a-f0-9]{16})\.([a-f0-9]{16})-\d+\.md$", fname)
                if match:
                    disk_index[match.group(1)] = {"prefix": match.group(2), "chunked": True}
                    continue
                match = re.match(r"^([a-f0-9]{16})\.([a-f0-9]{16})\.md$", fname)
                if match:
                    disk_index[match.group(1)] = {"prefix": match.group(2), "chunked": False}
                    continue
                match = re.match(r"^([a-f0-9]{16})\.md$", fname)
                if match:
                    disk_index[match.group(1)] = {"prefix": None, "chunked": None}

        # Three-way set comparison
        api_keys = set(api_index.keys())
        disk_keys = set(disk_index.keys())

        to_add = api_keys - disk_keys
        to_update = set()
        to_delete = disk_keys - api_keys
        to_skip = set()

        for uh in api_keys & disk_keys:
            disk_entry = disk_index[uh]
            if disk_entry["prefix"] is None:
                to_update.add(uh)
                continue
            prefix_matches = api_index[uh]["content_prefix"] == disk_entry["prefix"]
            pattern_matches = disk_entry["chunked"] == chunking_enabled
            if prefix_matches and pattern_matches:
                to_skip.add(uh)
            else:
                to_update.add(uh)

        self.log.info(
            "webscraper_reconciliation",
            add=len(to_add),
            update=len(to_update),
            delete=len(to_delete),
            skip=len(to_skip),
        )

        total_work = len(to_add) + len(to_update) + len(to_delete) + len(to_skip)
        done_work = 0
        failed = 0

        # Process adds and updates
        for uh in sorted(to_add | to_update):
            entry = api_index[uh]
            pct = 10 + int(80 * done_work / total_work) if total_work else 90
            progress_callback(pct, message=f"Processing {entry['url']}")

            try:
                if chunking_enabled:
                    chunks = self._process_with_docling(
                        dl_url, dl_key, entry["content"], entry["url"],
                        progress_callback, pct,
                    )
                else:
                    chunks = [{"chunk_index": 0, "text": entry["content"]}]

                # Delete stale files before writing new ones (order matters — avoid self-deletion)
                if uh in to_update:
                    for fname in os.listdir(output_dir):
                        if re.match(rf"^{re.escape(uh)}\.", fname):
                            os.remove(os.path.join(output_dir, fname))

                for chunk in chunks:
                    suffix = f"-{chunk['chunk_index']:03d}" if chunking_enabled else ""
                    tmp = f"/tmp/{uh}.{entry['content_prefix']}{suffix}.md"
                    with open(tmp, "w", encoding="utf-8") as f:
                        f.write(f"Original URL: {entry['url']}\n\n{chunk['text']}")
                    self.move_to_destination(tmp)
            except (requests.HTTPError, DoclingAuthError) as exc:
                # Docling (or any HTTP) failure is fatal — surface as an error
                # instead of silently counting the page as skipped.
                self.log.error("webscraper_http_error", url=entry["url"], error=str(exc))
                raise
            except Exception as exc:
                self.log.warning("webscraper_page_skipped", url=entry["url"], error=str(exc))
                failed += 1

            done_work += 1

        # Process deletions
        for uh in sorted(to_delete):
            pct = 10 + int(80 * done_work / total_work) if total_work else 90
            progress_callback(pct, message=f"Removing {uh}")
            for fname in os.listdir(output_dir):
                if re.match(rf"^{re.escape(uh)}\.", fname):
                    os.remove(os.path.join(output_dir, fname))
            self.log.debug("webscraper_deleted", uh=uh)
            done_work += 1

        # Stray file cleanup
        if os.path.isdir(output_dir):
            for fname in os.listdir(output_dir):
                if not fname.endswith(".md"):
                    continue
                match = re.match(r"^([a-f0-9]{16})", fname)
                if match and match.group(1) not in api_index:
                    os.remove(os.path.join(output_dir, fname))
                    self.log.debug("webscraper_removed_stray", filename=fname)

        msg_parts = [
            f"{len(to_add)} added",
            f"{len(to_update)} updated",
            f"{len(to_delete)} deleted",
            f"{len(to_skip)} unchanged",
        ]
        if failed:
            msg_parts.append(f"{failed} failed")

        self.log.info(
            "webscraper_complete",
            url=url,
            added=len(to_add),
            updated=len(to_update),
            deleted=len(to_delete),
            skipped=len(to_skip),
            failed=failed,
        )
        progress_callback(100, message=", ".join(msg_parts))

    def _process_with_docling(self, base_url, api_key, markdown, page_url,
                              progress_callback, current_pct):
        dl_headers = {}
        if api_key:
            dl_headers["x-api-key"] = api_key

        overhead = len(tiktoken.get_encoding("cl100k_base").encode(
            f"Original URL: {page_url}\n\n"
        ))
        chunking_max_tokens = 490 - overhead

        files = {"files": ("page.md", markdown.encode("utf-8"), "text/markdown")}
        data = {
            **_DOCLING_OPTIONS,
            "chunking_max_tokens": chunking_max_tokens,
            "chunking_use_markdown_tables": str(True).lower(),
            "chunking_merge_peers": str(True).lower(),
        }
        data["convert_document_timeout"] = str(data["convert_document_timeout"])

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

        self.log.info("docling_chunk_submitted", task_id=task_id, url=page_url)

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

            time.sleep(5)

    def _submit_job(self, url, max_depth, max_pages):
        domain = urlparse(url).netloc
        payload = {
            "urls": [url],
            "browser_config": {"headless": True},
            "crawler_config": {
                "cache_mode": "bypass",
                "deep_crawl_strategy": {
                    "type": "BFSDeepCrawlStrategy",
                    "params": {
                        "max_depth": max_depth,
                        "include_external": False,
                        **({"max_pages": max_pages} if max_pages > 0 else {}),
                        "filter_chain": {
                            "type": "FilterChain",
                            "params": {
                                "filters": [
                                    {
                                        "type": "ContentTypeFilter",
                                        "params": {
                                            "allowed_types": ["text/html"],
                                        },
                                    },
                                    {
                                        "type": "DomainFilter",
                                        "params": {
                                            "allowed_domains": [domain],
                                            "blocked_domains": [],
                                        },
                                    },
                                ],
                            },
                        },
                    },
                },
            },
        }

        try:
            resp = requests.post(f"{_API_URL}/crawl/job", json=payload)
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as exc:
            self.log.error("webscraper_submit_error", error=str(exc))
            raise

        task_id = data.get("task_id")
        if not task_id:
            raise RuntimeError("No task_id in crawl job response")

        self.log.info("webscraper_job_submitted", task_id=task_id)
        return task_id

    def _poll_job(self, task_id, progress_callback):
        while True:
            try:
                resp = requests.get(f"{_API_URL}/crawl/job/{task_id}")
                resp.raise_for_status()
                data = resp.json()
            except requests.RequestException as exc:
                self.log.error("webscraper_poll_error", error=str(exc))
                raise

            status = data.get("status")

            if status == "completed":
                raw = data.get("result")
                if not raw:
                    raise RuntimeError("Job completed but no result data")
                result_data = json.loads(raw) if isinstance(raw, str) else raw
                return result_data.get("results", [])

            elif status == "failed":
                error_msg = data.get("error", "Unknown error")
                self.log.error("webscraper_job_failed", error=error_msg)
                raise RuntimeError(f"Crawl job failed: {error_msg}")

            elif status == "processing":
                progress_callback(0)
                time.sleep(30)

            else:
                raise RuntimeError(f"Unknown crawl job status: {status}")
