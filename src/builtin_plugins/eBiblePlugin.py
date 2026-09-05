"""eBible.org Plugin: Downloads Bible scripture and applies custom chunking strategy.

Cron should be set to yearly (0 0 1 1 *) since Bible data doesn't change.
"""

import json
import math
import os
import time

import requests
import tiktoken

from utils.plugin_base import BaseSubscription, PluginRoute


BASE_URL = "https://bible.helloao.org/api/"
MAX_CHUNK_TOKENS = 490
TARGET_CHUNK_TOKENS = 475
METADATA_OVERHEAD = 35  # Fixed constant covering all versions (max observed: 34)
EFFECTIVE_TARGET = 440  # MAX_CHUNK_TOKENS - METADATA_OVERHEAD, split for buffer


def fetch_books(version):
    """Fetch the book list for a Bible version from the helloao.org API.
    
    Returns a list of dicts sorted by canonical order, each containing:
      id, name, commonName, numberOfChapters, firstChapterNumber, order
    Raises on HTTP or parsing failure.
    """
    url = f"{BASE_URL}{version}/books.json"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    data = response.json()
    books = data.get("books", [])
    if not books:
        raise ValueError(f"No books returned for version '{version}'")
    result = []
    for book in books:
        result.append({
            "id": book["id"],
            "name": book.get("name", ""),
            "commonName": book.get("commonName", book.get("name", "")),
            "numberOfChapters": book.get("numberOfChapters", 0),
            "firstChapterNumber": book.get("firstChapterNumber", 1),
            "order": book.get("order", 999),
        })
    result.sort(key=lambda b: b["order"])
    return result


def extract_verse_text(content):
    """Extract text from verse content list.
    
    Handles multiple content patterns:
    - Most books: list of strings and/or dicts with noteId
    - Psalms: list of dicts with 'text' key, plus optional poem, lineBreak, noteId
    """
    text_parts = []
    for item in content:
        if isinstance(item, str):
            text_parts.append(item)
        elif isinstance(item, dict):
            if "text" in item:
                text_parts.append(item["text"])
    return " ".join(text_parts)


def count_tokens(enc, text):
    """Count tokens in text using tiktoken encoder."""
    return len(enc.encode(text))


def distribute_verses(verses, low_threshold):
    """Distribute verses into chunks. Each chunk must EXCEED low_threshold."""
    chunks = []
    current_chunk = []
    current_tokens = 0

    for verse in verses:
        current_chunk.append(verse)
        current_tokens += verse["estTokens"]
        if current_tokens >= low_threshold and current_chunk:
            chunks.append(current_chunk)
            current_chunk = []
            current_tokens = 0

    if current_chunk:
        chunks.append(current_chunk)

    return chunks


def chunk_chapter(verses, total_tokens):
    """Chunk a chapter's verses following the specification.

    1. Start with floor(total_tokens / EFFECTIVE_TARGET) chunks (minimum possible).
    2. low_threshold = total_tokens / chunk_count (may exceed EFFECTIVE_TARGET).
    3. Distribute verses: add verse first, then finalize chunk if it
       exceeds low_threshold.
    4. Check invariant: every chunk + METADATA_OVERHEAD <= MAX_CHUNK_TOKENS.
    5. If any chunk exceeds, increment chunk_count and retry.
    """
    chunk_count = max(1, math.floor(total_tokens / EFFECTIVE_TARGET))

    while True:
        low_threshold = total_tokens / chunk_count
        chunks = distribute_verses(verses, low_threshold)

        all_within_limits = True
        for chunk in chunks:
            chunk_tokens = sum(v["estTokens"] for v in chunk)
            if chunk_tokens + METADATA_OVERHEAD > MAX_CHUNK_TOKENS:
                all_within_limits = False
                break

        if all_within_limits:
            return chunks

        chunk_count += 1


def _get_bible_versions():
    """Custom route handler: returns the full version list for the frontend dropdown."""
    return {"versions": VERSION_ENUM, "labels": VERSION_LABELS, "groups": VERSION_GROUPS}


class eBiblePlugin(BaseSubscription):
    metadata = {
        "name": "eBiblePlugin",
        "display_name": "Bible Scriptures",
        "description": "Downloads all Bible scripture for the selected version and applies custom chunking strategy on each chapter. Set cron to leap-day (0 0 30 2 *) since Bible data doesn't change (Hebrews 13:8). All data is courtesy of  https://bible.helloao.org/ and https://ebible.org/",
        "sub_type": "SCHEDULED",
    }

    def get_custom_routes(self):
        return [PluginRoute(path="/versions", method="GET", handler=_get_bible_versions)]

    def get_schema(self):
        return {
            "type": "object",
            "properties": {
                "version": {
                    "type": "string",
                    "default": "eng_kjv",
                    "x-enum-source": "/api/plugins/eBiblePlugin/versions",
                }
            },
            "required": ["version"],
        }

    def getData(self, config, progress_callback):
        version = config.get("version", "eng_kjv")
        enc = tiktoken.get_encoding("cl100k_base")
        
        # Fetch book list dynamically from the API
        books = fetch_books(version)
        
        total_chapters = sum(b["numberOfChapters"] for b in books)
        chapters_processed = 0
        
        summary_lines = []
        
        # Reconciliation: scan output directory for already-processed chapters
        output_dir = self.get_destination_path()
        completed_chapters = set()
        # Build lookup of book_filename -> commonName for parsing filenames
        _book_lookup = {}
        for b in books:
            fn = b["commonName"].lower().replace(" ", "")
            _book_lookup[fn] = b["commonName"]
        if os.path.isdir(output_dir):
            for fname in os.listdir(output_dir):
                if not fname.endswith(".txt"):
                    continue
                stem = fname[:-4]  # strip .txt
                for bk_fn, bk_cn in _book_lookup.items():
                    prefix = bk_fn + "-"
                    if stem.startswith(prefix):
                        remainder = stem[len(prefix):]
                        parts = remainder.split("-")
                        if len(parts) == 3:
                            try:
                                ch_num = int(parts[0])
                                completed_chapters.add((bk_fn, ch_num))
                            except ValueError:
                                pass
                        break
        
        for book in books:
            book_id = book["id"]
            common_name = book["commonName"]
            first_ch = book["firstChapterNumber"]
            chapter_count = book["numberOfChapters"]
            book_filename = common_name.lower().replace(" ", "")
            
            for chapter_num in range(first_ch, first_ch + chapter_count):
                chapters_processed += 1
                progress = int(100 * chapters_processed / total_chapters) if total_chapters > 0 else 0
                progress_callback(progress)
                
                # Reconciliation: skip if already processed
                if (book_filename, chapter_num) in completed_chapters:
                    continue
                
                try:
                    # Fetch chapter data
                    url = f"{BASE_URL}{version}/{book_id}/{chapter_num}.json"
                    response = requests.get(url, timeout=30)
                    response.raise_for_status()
                    data = response.json()
                    
                    # Extract verses
                    content_items = data.get("chapter", {}).get("content", [])
                    verses = []
                    
                    for item in content_items:
                        if item.get("type") == "verse":
                            verse_num = item.get("number")
                            if verse_num is None:
                                continue
                            
                            content = item.get("content", [])
                            text = extract_verse_text(content)
                            
                            if not text:
                                continue
                            
                            # Format verse text with verse number
                            formatted_text = f"*{verse_num}* {text}"
                            token_count = count_tokens(enc, formatted_text)
                            
                            verses.append({
                                "verse": verse_num,
                                "estTokens": token_count,
                                "text": formatted_text,
                            })
                    
                    if not verses:
                        continue
                    
                    # Calculate total tokens for chapter
                    total_tokens = sum(v["estTokens"] for v in verses)
                    
                    # Chunk the chapter
                    chunks = chunk_chapter(verses, total_tokens)
                    
                    # Output chunks
                    for chunk_verses in chunks:
                        verse_start = chunk_verses[0]["verse"]
                        verse_end = chunk_verses[-1]["verse"]
                        pre_tokens = sum(v["estTokens"] for v in chunk_verses)
                        total_tokens_with_meta = pre_tokens + METADATA_OVERHEAD
                        
                        # Create temp file
                        filename = f"{book_filename}-{chapter_num}-{verse_start}-{verse_end}.txt"
                        tmp_path = f"/tmp/{filename}"
                        
                        # Build chunk content
                        scripture_text = " ".join(v["text"] for v in chunk_verses)
                        chunk_content = f"""Version: {version}
Book: {common_name}
Chapter: {chapter_num}
VerseRange: {verse_start}-{verse_end}
Scripture: {scripture_text}
"""
                        
                        with open(tmp_path, "w", encoding="utf-8") as f:
                            f.write(chunk_content)
                        
                        self.move_to_destination(tmp_path)
                        
                        # Add to summary
                        summary_lines.append(
                            f"{common_name.lower()},{chapter_num},{verse_start}-{verse_end},{pre_tokens},{total_tokens_with_meta}"
                        )
                
                except requests.RequestException as e:
                    self.log.error(
                        "bible_fetch_failed",
                        version=version, book=book_id, chapter=chapter_num,
                        error=f"{type(e).__name__}: {e}",
                    )
                    raise
                except (json.JSONDecodeError, KeyError) as e:
                    self.log.error(
                        "bible_parse_failed",
                        version=version, book=book_id, chapter=chapter_num,
                        error=f"{type(e).__name__}: {e}",
                    )
                    raise
        
        # Write summary file
        version_lower = version.lower()
        summary_path = f"/tmp/{version_lower}_summary.txt"
        with open(summary_path, "w", encoding="utf-8") as f:
            f.write("Book,Chapter,VerseRange,PreTokenCount,TotalTokenCount\n")
            for line in summary_lines:
                f.write(f"{line}\n")
        
        progress_callback(100)


if __name__ == "__main__":
    # For testing
    plugin = eBiblePlugin()
    plugin._subscription_name = "test"
    
    def progress(p):
        print(f"Progress: {p}%")
    
    plugin.getData({"version": "eng_kjv"}, progress)


# ── Version data (externalized; was auto-generated from helloao.org inline) ──
_VERSION_DATA_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "ebible_versions.json")


def _load_version_tables():
    """Load the bundled version catalog (enum / labels / groups).

    The catalog ships in the image under ``data/ebible_versions.json``; if it
    is ever missing or corrupt, fall back to an empty catalog rather than
    failing the plugin load.
    """
    try:
        with open(_VERSION_DATA_PATH, "r", encoding="utf-8") as f:
            payload = json.load(f)
        return payload["enum"], payload["labels"], payload["groups"]
    except Exception:
        return [], {}, {}


VERSION_ENUM, VERSION_LABELS, VERSION_GROUPS = _load_version_tables()

