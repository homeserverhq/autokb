"""IMAP folder watcher: IDLE-pushes events, chunks new mail as markdown."""

import asyncio
import email
import os
import re
from datetime import timezone
from email.header import decode_header, make_header
from email.utils import parsedate_to_datetime

import aioimaplib
import tiktoken
from langchain_text_splitters import RecursiveCharacterTextSplitter

from utils.plugin_base import BaseSubscription

TOKEN_BUDGET = 490
SAFETY_FLOOR = 50
IDLE_RENEWAL_S = 1500
CONNECT_BACKOFF_S = 10


class imapFolderWatchPlugin(BaseSubscription):
    metadata = {
        "name": "imapFolderWatchPlugin",
        "display_name": "IMAP Folder Watch",
        "icon": "imapFolderWatchPlugin.png",
        "description": "Watches an email IMAP folder via IDLE; optionally chunks new mail as markdown. When monitoring subfolders, a shorter cron interval (e.g. every 5 minutes) is recommended.",
        "sub_type": "EVENT_BASED",
        "monitor_timeout": 1500,
    }
    DEFAULT_ACCESS_LEVEL = "PRIVATE"

    def __init__(self):
        super().__init__()
        self._host = ""
        self._port = 993
        self._use_ssl = True
        self._user = ""
        self._password = ""
        self._folder = "INBOX"
        self._monitor_subfolders = True
        self._chunking_enabled = True
        self._sep = "/"

    def get_schema(self):
        return {
            "type": "object",
            "properties": {
                "host": {"type": "string", "minLength": 1, "format": "hostname"},
                "port": {"type": "int", "minimum": 1, "maximum": 65535, "default": 993},
                "use_ssl": {"type": "bool", "default": True},
                "user": {"type": "string", "minLength": 1},
                "password": {"type": "string", "minLength": 1, "format": "password"},
                "folder": {"type": "string", "minLength": 1, "default": "INBOX"},
                "monitor_subfolders": {
                    "type": "boolean",
                    "default": True,
                    "description": "Monitor SubFolders?",
                },
                "chunking_enabled": {
                    "type": "boolean",
                    "default": True,
                    "description": "Chunk emails by token budget (~490 tokens per file). Disable to write the full email as a single document.",
                },
            },
            "required": ["host", "user", "password", "folder"],
        }

    def _apply_config(self, config):
        self._host = config["host"]
        self._port = int(config.get("port", 993))
        self._use_ssl = bool(config.get("use_ssl", True))
        self._user = config["user"]
        self._password = config["password"]
        self._folder = config.get("folder", "INBOX")
        self._monitor_subfolders = bool(config.get("monitor_subfolders", True))
        self._chunking_enabled = bool(config.get("chunking_enabled", True))

    async def monitor(self, config, cancel_token):
        self._apply_config(config)
        try:
            mail = await self._connect()
            await self._select_folder(mail, self._folder)
        except Exception as exc:
            self.log.warning("imap_connect_failed", host=self._host, error=str(exc))
            try:
                await asyncio.wait_for(cancel_token.wait(), timeout=CONNECT_BACKOFF_S)
            except asyncio.TimeoutError:
                pass
            return False
        try:
            return await self._idle_once(mail, cancel_token)
        finally:
            try:
                await mail.idle_done()
            except Exception:
                pass
            try:
                await mail.logout()
            except Exception:
                pass

    async def _connect(self):
        if self._use_ssl:
            mail = aioimaplib.IMAP4_SSL(self._host, self._port)
        else:
            mail = aioimaplib.IMAP4(self._host, self._port)
        await mail.wait_hello_from_server()
        resp = await mail.login(self._user, self._password)
        if resp.result != 'OK':
            raise ConnectionError(f"IMAP login failed: {resp.result}")
        return mail

    async def _select_folder(self, mail, folder):
        await mail.select(folder)

    async def _list_subfolders(self, mail):
        if not self._monitor_subfolders:
            self.log.debug("list_subfolders_disabled", folder=self._folder)
            return {self._folder}
        status, data = await mail.list('""', "*")
        self.log.debug("list_subfolders_raw", status=status, data=str(data))
        all_folders = set()
        self._sep = "/"
        for item in data:
            if isinstance(item, bytes):
                line = item.decode("utf-8", errors="replace")
            elif isinstance(item, str):
                line = item
            else:
                self.log.debug("list_subfolders_skip_type", line=str(item), typ=type(item).__name__)
                continue
            m = re.match(
                r'\(.*?\)\s+(?P<delim>"[^"]*"|NIL|[^\s]+)\s+(?P<name>.+?)\s*$',
                line,
            )
            if not m:
                self.log.debug("list_subfolders_no_match", line=line)
                continue
            delim = m.group("delim")
            name = m.group("name").strip('"')
            if delim not in ("NIL", '""'):
                self._sep = delim.strip('"')
            self.log.debug("list_subfolders_matched", delim=delim, name=name, sep=self._sep)
            all_folders.add(name)
        root_prefix = self._folder + self._sep
        self.log.debug("list_subfolders_all", folders=sorted(all_folders), root_prefix=root_prefix, sep=self._sep)
        result = {self._folder}
        for name in all_folders:
            if name == self._folder or name.startswith(root_prefix):
                self.log.debug("list_subfolders_included", name=name)
                result.add(name)
            else:
                self.log.debug("list_subfolders_excluded", name=name, root_prefix=root_prefix)
        self.log.debug("list_subfolders_result", folders=sorted(result))
        return result

    async def _idle_once(self, mail, cancel_token):
        while not cancel_token.is_set():
            await mail.idle_start()
            push_task = asyncio.create_task(mail.wait_server_push())
            cancel_task = asyncio.create_task(cancel_token.wait())
            try:
                done, pending = await asyncio.wait(
                    {push_task, cancel_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if cancel_task in done:
                    return False
                if push_task in done:
                    exc = push_task.exception()
                    if exc is not None:
                        self.log.warning("imap_push_error", error=str(exc))
                        return False
                    return True
                return False
            finally:
                for t in (push_task, cancel_task):
                    if not t.done():
                        t.cancel()
                        try:
                            await t
                        except (asyncio.CancelledError, Exception):
                            pass
        return False

    def getData(self, config, progress_callback):
        self._apply_config(config)
        asyncio.run(self._reconcile(progress_callback))

    async def _reconcile(self, progress_callback):
        progress_callback(0)
        async def _keepalive():
            while True:
                await asyncio.sleep(2)
                progress_callback(0)
        hb = asyncio.create_task(_keepalive())
        try:
            mail = await self._connect()
        finally:
            hb.cancel()
            try:
                await hb
            except asyncio.CancelledError:
                pass
        try:
            folders = await self._list_subfolders(mail)
            api_index = {}

            for folder in sorted(folders):
                await self._select_folder(mail, folder)
                status, data = await mail.uid_search("ALL", charset=None)
                uids = self._parse_uid_list(data)
                safe = re.sub(r"[^a-zA-Z0-9]", "-", folder)
                for uid in uids:
                    api_index[(safe, uid)] = folder

            output_dir = self.get_destination_path()
            disk_by_key = {}
            if os.path.isdir(output_dir):
                disk_files = os.listdir(output_dir)
                disk_by_key = self._index_chunks(disk_files)
                old_pattern = re.compile(r"^(?P<uid>\d+)\.(?P<n>\d+)\.txt$")
                safe_root = re.sub(r"[^a-zA-Z0-9]", "-", self._folder)
                for fname in disk_files:
                    m = old_pattern.match(fname)
                    if m:
                        uid = int(m.group("uid"))
                        disk_by_key.setdefault((safe_root, uid), set()).add(fname)

            api_keys = set(api_index.keys())
            disk_keys = set(disk_by_key.keys())

            to_add = api_keys - disk_keys
            to_remove = disk_keys - api_keys

            total_work = max(len(to_add) + len(to_remove), 1)
            done_work = 0
            progress_callback(5)

            for safe, uid in sorted(to_add):
                progress_callback(5 + int(85 * done_work / total_work))
                await self._chunk_and_emit(mail, api_index[(safe, uid)], uid)
                done_work += 1

            for safe, uid in to_remove:
                for filename in disk_by_key[(safe, uid)]:
                    try:
                        os.remove(os.path.join(output_dir, filename))
                        self.log.debug("imap_chunk_removed", uid=uid, safe_folder=safe, filename=filename)
                    except FileNotFoundError:
                        pass
                done_work += 1
                progress_callback(5 + int(85 * done_work / total_work))

            if os.path.isdir(output_dir):
                expected_keys = set(disk_by_key.keys()) | api_keys
                for fname in os.listdir(output_dir):
                    new_m = re.match(r"^(?P<folder>.+?)\.(?P<uid>\d+)\.(?P<n>\d+)\.txt$", fname)
                    if new_m:
                        key = (new_m.group("folder"), int(new_m.group("uid")))
                        if key not in expected_keys:
                            os.remove(os.path.join(output_dir, fname))
                    else:
                        old_m = re.match(r"^(?P<uid>\d+)\.(?P<n>\d+)\.txt$", fname)
                        if old_m:
                            os.remove(os.path.join(output_dir, fname))
        finally:
            try:
                await mail.logout()
            except Exception:
                pass
        progress_callback(100)

    async def _chunk_and_emit(self, mail, folder, uid):
        await self._select_folder(mail, folder)
        status, msg_data = await mail.uid("fetch", str(uid), "(RFC822)")
        raw = self._extract_rfc822_bytes(msg_data)
        if not raw:
            self.log.warning("imap_fetch_empty", uid=uid, folder=folder)
            return
        msg = email.message_from_bytes(raw)
        body_text = self._extract_best_text(msg)
        subject = self._decode_header(msg.get("Subject", ""))
        from_addr = self._decode_header(msg.get("From", ""))
        date_iso = self._format_date_iso(msg.get("Date", ""))

        folder_line = folder.replace(self._sep, ".")
        safe_folder = re.sub(r"[^a-zA-Z0-9]", "-", folder)

        if not self._chunking_enabled:
            header = (
                f"Folder: {folder_line}\n"
                f"Subject: {subject}\n"
                f"Date: {date_iso}\n"
                f"From: {from_addr}\n"
                f"Chunk: 1 of 1\n"
                f"Body: "
            )
            tmp = f"/tmp/{safe_folder}.{uid}.1.txt"
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(header)
                if body_text:
                    f.write(body_text)
                f.write("\n")
            self.move_to_destination(tmp)
            return

        enc = tiktoken.get_encoding("cl100k_base")
        header_prefix = (
            f"Folder: {folder_line}\n"
            f"Subject: {subject}\n"
            f"Date: {date_iso}\n"
            f"From: {from_addr}\n"
            f"Chunk: 1 of 1\n"
            f"Body: "
        )
        header_tokens = len(enc.encode(header_prefix))
        body_budget = max(TOKEN_BUDGET - header_tokens, SAFETY_FLOOR)

        if not body_text:
            chunks = [""]
        else:
            splitter = RecursiveCharacterTextSplitter(
                chunk_size=body_budget,
                chunk_overlap=0,
                length_function=lambda t: len(enc.encode(t)),
                separators=["\n\n", "\n", ". ", "? ", "! ", " ", ""],
            )
            chunks = splitter.split_text(body_text)

            min_chunk_tokens = max(body_budget // 4, 1)
            i = 0
            while i < len(chunks) - 1:
                if len(enc.encode(chunks[i])) < min_chunk_tokens:
                    candidate = chunks[i] + "\n\n" + chunks[i + 1]
                    if len(enc.encode(candidate)) <= body_budget:
                        chunks[i] = candidate
                        chunks.pop(i + 1)
                        continue
                i += 1

        total_chunks = len(chunks)
        for i, body_chunk in enumerate(chunks, start=1):
            tmp = f"/tmp/{safe_folder}.{uid}.{i}.txt"
            header = (
                f"Folder: {folder_line}\n"
                f"Subject: {subject}\n"
                f"Date: {date_iso}\n"
                f"From: {from_addr}\n"
                f"Chunk: {i} of {total_chunks}\n"
                f"Body: "
            )
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(header)
                if body_chunk:
                    f.write(body_chunk)
                f.write("\n")
            self.move_to_destination(tmp)

    @staticmethod
    def _parse_uid_list(data):
        result = set()
        if not data:
            return result
        for part in data:
            if not part:
                continue
            if isinstance(part, bytes):
                part = part.decode("ascii", errors="replace")
            for token in part.split():
                if token.isdigit():
                    result.add(int(token))
        return result

    @staticmethod
    def _index_chunks(files):
        out = {}
        pattern = re.compile(r"^(?P<folder>.+?)\.(?P<uid>\d+)\.(?P<n>\d+)\.txt$")
        for f in files:
            m = pattern.match(f)
            if not m:
                continue
            folder = m.group("folder")
            uid = int(m.group("uid"))
            out.setdefault((folder, uid), set()).add(f)
        return out

    @staticmethod
    def _extract_rfc822_bytes(msg_data):
        for resp in msg_data:
            if isinstance(resp, (bytes, bytearray)) and len(resp) > 100:
                return bytes(resp)
        return b""

    @staticmethod
    def _extract_best_text(msg):
        if not msg.is_multipart():
            ctype = msg.get_content_type()
            payload = msg.get_payload(decode=True) or b""
            charset = msg.get_content_charset() or "utf-8"
            try:
                text = payload.decode(charset, errors="replace")
            except (LookupError, UnicodeDecodeError):
                text = payload.decode("utf-8", errors="replace")
            if ctype == "text/plain":
                return text
            if ctype == "text/html":
                return imapFolderWatchPlugin._strip_html(text)
            return text
        plain_parts = []
        html_parts = []
        for part in msg.walk():
            ctype = part.get_content_type()
            disp = str(part.get("Content-Disposition", ""))
            if "attachment" in disp.lower():
                continue
            payload = part.get_payload(decode=True) or b""
            charset = part.get_content_charset() or "utf-8"
            try:
                text = payload.decode(charset, errors="replace")
            except (LookupError, UnicodeDecodeError):
                text = payload.decode("utf-8", errors="replace")
            if ctype == "text/plain":
                plain_parts.append(text)
            elif ctype == "text/html":
                html_parts.append(text)
        if plain_parts:
            return "\n\n".join(plain_parts)
        if html_parts:
            return imapFolderWatchPlugin._strip_html("\n\n".join(html_parts))
        return ""

    @staticmethod
    def _strip_html(html):
        text = re.sub(r"<style[^>]*>.*?</style>", "", html, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r"<script[^>]*>.*?</script>", "", text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r"<[^>]+>", " ", text)
        text = re.sub(r"&nbsp;", " ", text)
        text = re.sub(r"&amp;", "&", text)
        text = re.sub(r"&lt;", "<", text)
        text = re.sub(r"&gt;", ">", text)
        text = re.sub(r"&quot;", '"', text)
        text = re.sub(r"&#39;", "'", text)
        text = re.sub(r"\s+", " ", text).strip()
        return text

    @staticmethod
    def _decode_header(header_val):
        if not header_val:
            return ""
        try:
            return str(make_header(decode_header(header_val)))
        except Exception:
            return header_val

    @staticmethod
    def _format_date_iso(date_header):
        if not date_header:
            return ""
        try:
            dt = parsedate_to_datetime(date_header)
            if dt is None:
                return date_header
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc).isoformat()
        except Exception:
            return date_header
