"""YouTube Channel Transcript Plugin: Downloads transcripts for all videos
in a YouTube channel and applies chunking strategy with natural break points.

Set cron to daily (0 0 * * *) or as needed to pick up new uploads.
"""

import json
import math
import os
import random
import re
import time

import requests
import tiktoken
from langchain_text_splitters import RecursiveCharacterTextSplitter
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api._errors import (
    IpBlocked,
    NoTranscriptFound,
    TranscriptsDisabled,
    VideoUnavailable,
    YouTubeRequestFailed,
)

from utils.misc_utils import SubscriptionCancelledError
from utils.plugin_base import BaseSubscription

BASE_URL = "https://www.youtube.com/watch?v="

MAX_CHUNK_TOKENS = 490
EFFECTIVE_TARGET = 440
MAX_DURATION_SECONDS = 150  # 2.5 minutes


def seconds_to_hms(seconds):
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def resolve_channel_id(channel_input, api_key=None):
    """Resolve a channel handle (@name) or URL to a canonical channel ID.

    Accepts:
      - Raw channel ID (starts with UC...) → returned as-is
      - Handle (@name) → resolved via yt-dlp
      - Full URL → extracted or resolved

    Returns a dict with keys: channel_id, title (if available).
    """
    channel_input = channel_input.strip()

    # Already a channel ID
    if channel_input.startswith("UC") and len(channel_input) == 24:
        return {"channel_id": channel_input, "title": None}

    # Try to extract channel ID from URL patterns
    patterns = [
        r"youtube\.com/channel/(UC[\w-]{22})",
        r"youtube\.com/@([\w-]+)",
        r"youtube\.com/c/([\w-]+)",
        r"youtube\.com/user/([\w-]+)",
    ]
    for pat in patterns:
        m = re.search(pat, channel_input)
        if m:
            extracted = m.group(1)
            if extracted.startswith("UC") and len(extracted) == 24:
                return {"channel_id": extracted, "title": None}
            # It's a handle or custom URL, resolve via yt-dlp
            channel_input = extracted
            break

    # Resolve handle/name via yt-dlp
    if channel_input.startswith("@"):
        url = f"https://www.youtube.com/{channel_input}"
    elif channel_input.startswith("UC") and len(channel_input) == 24:
        return {"channel_id": channel_input, "title": None}
    else:
        url = f"https://www.youtube.com/@{channel_input}"

    try:
        import yt_dlp

        ydl_opts = {"quiet": True, "no_warnings": True, "skip_download": True, "extract_flat": True}
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)
            channel_id = info.get("channel_id") or info.get("id")
            channel_title = info.get("channel") or info.get("title")
            if channel_id:
                return {"channel_id": channel_id, "title": channel_title}
    except Exception:
        pass

    # If it looks like a raw channel ID already, use it directly
    if channel_input.startswith("UC") and len(channel_input) == 24:
        return {"channel_id": channel_input, "title": None}

    raise ValueError(
        f"Could not resolve channel identifier: {channel_input}. "
        "Provide a channel ID (UCxxx), handle (@name), or full URL."
    )


def enumerate_videos_api(channel_id, api_key, max_videos=0):
    """Enumerate video IDs for a channel using YouTube Data API v3.

    Returns list of dicts: [{id, title, published_at, view_count}, ...]
    """
    videos = []
    page_token = None
    base_url = "https://www.googleapis.com/youtube/v3/search"

    while True:
        params = {
            "part": "snippet",
            "channelId": channel_id,
            "maxResults": 50,
            "order": "date",
            "type": "video",
            "key": api_key,
        }
        if page_token:
            params["pageToken"] = page_token

        resp = requests.get(base_url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        for item in data.get("items", []):
            vid = item["id"]["videoId"]
            snippet = item["snippet"]
            videos.append(
                {
                    "id": vid,
                    "title": snippet.get("title", f"Video {vid}"),
                    "published_at": snippet.get("publishedAt", "")[:10],
                    "view_count": None,  # Fetched separately below
                }
            )
            if max_videos > 0 and len(videos) >= max_videos:
                break

        if max_videos > 0 and len(videos) >= max_videos:
            break

        page_token = data.get("nextPageToken")
        if not page_token:
            break

    # Batch-fetch statistics (view counts)
    if videos:
        _fetch_view_counts(videos, api_key)

    return videos


def _fetch_view_counts(videos, api_key):
    """Fetch view counts for videos in batches of 50."""
    base_url = "https://www.googleapis.com/youtube/v3/videos"
    for i in range(0, len(videos), 50):
        batch = videos[i : i + 50]
        ids = ",".join(v["id"] for v in batch)
        params = {
            "part": "statistics",
            "id": ids,
            "key": api_key,
        }
        try:
            resp = requests.get(base_url, params=params, timeout=15)
            resp.raise_for_status()
            items = {item["id"]: item for item in resp.json().get("items", [])}
            for v in batch:
                item = items.get(v["id"])
                if item:
                    v["view_count"] = (
                        item.get("statistics", {}).get("viewCount", "Unknown")
                    )
        except Exception:
            pass  # View counts are non-critical


def enumerate_videos_ytdlp(channel_id, max_videos=0):
    """Enumerate video IDs for a channel using yt-dlp (no API key needed).

    Returns list of dicts: [{id, title, published_at, view_count}, ...]
    """
    import yt_dlp

    url = f"https://www.youtube.com/channel/{channel_id}/videos"
    ydl_opts = {
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "extract_flat": True,
    }

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(url, download=False)

    videos = []
    for entry in info.get("entries", []):
        vid = entry.get("id")
        if not vid:
            continue
        videos.append(
            {
                "id": vid,
                "title": entry.get("title", f"Video {vid}"),
                "published_at": entry.get("upload_date", ""),
                "view_count": entry.get("view_count"),
            }
        )
        if max_videos > 0 and len(videos) >= max_videos:
            break

    return videos


def get_video_metadata(video_id, api_key=None):
    """Fetch metadata for a single video.

    Returns dict with: title, published_at, view_count.
    """
    if api_key:
        try:
            url = "https://www.googleapis.com/youtube/v3/videos"
            params = {
                "part": "snippet,statistics",
                "id": video_id,
                "key": api_key,
            }
            resp = requests.get(url, params=params, timeout=10)
            resp.raise_for_status()
            items = resp.json().get("items", [])
            if items:
                snippet = items[0]["snippet"]
                stats = items[0].get("statistics", {})
                return {
                    "title": snippet.get("title", f"Video {video_id}"),
                    "published_at": snippet.get("publishedAt", "")[:10],
                    "view_count": stats.get("viewCount", "Unknown"),
                }
        except Exception:
            pass

    # Fallback: yt-dlp
    try:
        import yt_dlp

        url = f"https://www.youtube.com/watch?v={video_id}"
        ydl_opts = {"quiet": True, "no_warnings": True, "skip_download": True}
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)
            return {
                "title": info.get("title", f"Video {video_id}"),
                "published_at": info.get("upload_date", "Unknown"),
                "view_count": info.get("view_count", "Unknown"),
            }
    except Exception:
        return {
            "title": f"Video {video_id}",
            "published_at": "Unknown",
            "view_count": "Unknown",
        }


def _is_indexed_chunk(fname: str, vid_id: str) -> bool:
    """True if *fname* is a chunking-on output file for *vid_id*: ``<vid>-NNN.txt``."""
    if not fname.startswith(vid_id + "-"):
        return False
    stem = fname[len(vid_id) + 1:-4]  # strip "<vid>-" prefix and ".txt" suffix
    return bool(stem) and stem.isdigit()


def split_by_duration(snippets, max_duration=MAX_DURATION_SECONDS):
    """Phase 1: Group consecutive transcript snippets into duration-bounded segments.

    Returns list of segment dicts, each containing:
      - snippets: list of transcript snippets
      - start: start time in seconds
      - end: end time in seconds
      - duration: total duration in seconds
      - text: concatenated text
    """
    if not snippets:
        return []

    segments = []
    current_snippets = []
    current_start = None
    current_end = 0
    current_text = ""

    for snippet in snippets:
        start = snippet.start if hasattr(snippet, "start") else snippet["start"]
        duration = snippet.duration if hasattr(snippet, "duration") else snippet["duration"]
        end = start + duration
        text = snippet.text if hasattr(snippet, "text") else snippet["text"]

        new_start = current_start if current_start is not None else start
        new_end = max(current_end, end)
        new_duration = new_end - new_start

        if current_snippets and new_duration > max_duration:
            segments.append(
                {
                    "snippets": current_snippets,
                    "start": current_start,
                    "end": current_end,
                    "duration": current_end - current_start,
                    "text": current_text.strip(),
                }
            )
            current_snippets = [snippet]
            current_start = start
            current_end = end
            current_text = text
        else:
            current_snippets.append(snippet)
            current_start = new_start
            current_end = new_end
            current_text = current_text + " " + text if current_text else text

    if current_snippets:
        segments.append(
            {
                "snippets": current_snippets,
                "start": current_start,
                "end": current_end,
                "duration": current_end - current_start,
                "text": current_text.strip(),
            }
        )

    return segments


def split_segment_tokens(segment, enc, chunk_size=EFFECTIVE_TARGET):
    """Phase 2: Split a duration-bounded segment into token-bounded chunks
    using RecursiveCharacterTextSplitter for natural break points.

    Returns list of chunk dicts with 'text', 'start', 'end', 'tokens'.
    """
    text = segment["text"]
    if not text:
        return []

    start = segment["start"]
    end = segment["end"]

    tokens = len(enc.encode(text))

    if tokens <= chunk_size:
        return [
            {
                "text": text,
                "start": start,
                "end": end,
                "tokens": tokens,
            }
        ]

    splitter = RecursiveCharacterTextSplitter(
        chunk_size=chunk_size,
        chunk_overlap=0,
        length_function=lambda t: len(enc.encode(t)),
        separators=["\n\n", "\n", ". ", " ", ""],
    )
    parts = splitter.split_text(text)

    total_duration = end - start
    total_tokens = tokens

    chunks = []
    cumulative_tokens = 0
    for part in parts:
        part_tokens = len(enc.encode(part))
        part_duration = (part_tokens / total_tokens) * total_duration if total_tokens > 0 else 0
        part_start = start + (cumulative_tokens / total_tokens) * total_duration if total_tokens > 0 else start
        part_end = min(part_start + part_duration, end)
        cumulative_tokens += part_tokens

        chunks.append(
            {
                "text": part.strip(),
                "start": part_start,
                "end": part_end,
                "tokens": part_tokens,
            }
        )

    return chunks


def build_chunk_content(video_id, meta, chunk):
    """Build the final text content for a chunk file."""
    start_hms = seconds_to_hms(chunk["start"])
    end_hms = seconds_to_hms(chunk["end"])
    start_int = int(chunk["start"])
    url = f"{BASE_URL}{video_id}&t={start_int}s"

    return (
        f"Video Name: {meta['title']}\n"
        f"Video ID: {video_id}\n"
        f"Release Date: {meta['published_at']}\n"
        f"Views: {meta['view_count']}\n"
        f"Chunk Start: {start_hms}\n"
        f"Chunk End: {end_hms}\n"
        f"Video URL: {url}\n"
        f"Text: {chunk['text']}"
    )


def compute_metadata_overhead(video_id, meta, chunk, enc):
    """Compute actual token count of the metadata portion of a chunk."""
    content = build_chunk_content(video_id, meta, chunk)
    text_prefix = "Text: "
    meta_part = content[: content.rfind(text_prefix)]
    return len(enc.encode(meta_part))


class youTubeTranscriptionPlugin(BaseSubscription):
    """Downloads YouTube channel transcripts with chunking and reconciliation."""

    metadata = {
        "name": "youTubeTranscriptionPlugin",
        "display_name": "YouTube Transcriptions",
        "description": (
            "Downloads transcripts for all videos in a YouTube channel and "
            "optionally applies chunking with natural break points. Set cron to daily "
            "(0 0 * * *) to pick up new uploads. Due to very active measures by YouTube, this plugin may experience rate-limiting, and transcriptions might be skipped."
        ),
        "sub_type": "SCHEDULED",
    }


    def get_schema(self):
        return {
            "type": "object",
            "properties": {
                "channel_id": {
                    "type": "string",
                    "description": (
                        "YouTube channel ID (e.g. UCxxxxxxx), handle (e.g. @channelname), "
                        "or channel URL"
                    ),
                },
                "language": {
                    "type": "string",
                    "default": "en",
                    "description": (
                        "Transcript language code (e.g. en, es, fr). "
                        "Defaults to English."
                    ),
                },
                "api_key": {
                    "type": "string",
                    "description": (
                        "YouTube Data API v3 key (optional). "
                        "Enables metadata and faster channel enumeration."
                    ),
                },
                "max_videos": {
                    "type": "integer",
                    "default": 0,
                    "description": (
                        "Max recent videos to process (0 = all videos)."
                    ),
                },
                "chunking_enabled": {
                    "type": "boolean",
                    "default": False,
                    "description": "Chunk transcripts by token budget (~490 tokens per file). Disable to write each video's full transcript as a single document.",
                },
            },
            "required": ["channel_id"],
        }

    def _fetch_transcript_via_ytdlp(self, video_id, language="en"):
        """Fallback transcript fetcher using yt-dlp when youtube_transcript_api is blocked.

        Returns list of snippet-like dicts with 'text', 'start', 'duration' keys,
        or raises an exception if it fails.
        """
        import yt_dlp

        ydl_opts = {
            "quiet": True,
            "no_warnings": True,
            "skip_download": True,
            "writesubtitles": True,
            "writeautomaticsub": True,
            "subtitles langs": language,
            "subtitles format": "json3",
        }

        url = f"https://www.youtube.com/watch?v={video_id}"
        t0 = time.time()
        self.log.debug("ytdlp_fetch_start", video_id=video_id)
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)
        elapsed = time.time() - t0
        self.log.debug("ytdlp_fetch_done", video_id=video_id, elapsed=f"{elapsed:.1f}s")

        subs = info.get("requested_subtitles", {})
        if not subs:
            raise NoTranscriptFound(video_id, language)

        lang_key = list(subs.keys())[0]
        sub_url = subs[lang_key]["url"]

        resp = requests.get(sub_url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
        data = resp.json()

        snippets = []
        for event in data.get("events", []):
            segs = event.get("segs", [])
            text = "".join(seg.get("utf8", "") for seg in segs).strip()
            if not text or text == "\n":
                continue
            start = event.get("tStartMs", 0) / 1000.0
            duration = event.get("dDurationMs", 0) / 1000.0
            snippets.append({"text": text, "start": start, "duration": duration})

        return snippets

    def fetch_transcript(self, video_id, language="en", progress_callback=None, heartbeat_pct=0):
        """Fetch transcript for a video.

        Returns list of snippet dicts with 'text', 'start', 'duration' keys.
        Tries youtube_transcript_api first, falls back to yt-dlp on IP block.
        """
        if progress_callback:
            progress_callback(heartbeat_pct)

        self.log.debug("transcript_list_start", video_id=video_id)
        t0 = time.time()

        try:
            ytt_api = YouTubeTranscriptApi()
            transcript_list = ytt_api.list(video_id)
            elapsed = time.time() - t0
            self.log.debug("transcript_list_done", video_id=video_id, elapsed=f"{elapsed:.1f}s")

            if progress_callback:
                progress_callback(heartbeat_pct)

            t1 = time.time()
            try:
                transcript = transcript_list.find_transcript([language]).fetch()
            except NoTranscriptFound:
                transcript = transcript_list.find_transcript(
                    transcript_list.keys()
                ).fetch()
            fetch_elapsed = time.time() - t1
            self.log.debug("transcript_fetched", video_id=video_id, source="api", snippets=len(transcript.snippets), fetch_elapsed=f"{fetch_elapsed:.1f}s")
            return transcript.snippets
        except IpBlocked:
            elapsed = time.time() - t0
            self.log.warning("transcript_api_ip_blocked", video_id=video_id, fallback="ytdlp", elapsed=f"{elapsed:.1f}s")
            if progress_callback:
                progress_callback(heartbeat_pct)
            snippets = self._fetch_transcript_via_ytdlp(video_id, language=language)
            self.log.debug("transcript_fetched", video_id=video_id, source="ytdlp_fallback", snippets=len(snippets))
            return snippets
        except YouTubeRequestFailed:
            elapsed = time.time() - t0
            self.log.warning("transcript_api_request_failed", video_id=video_id, fallback="ytdlp", elapsed=f"{elapsed:.1f}s")
            if progress_callback:
                progress_callback(heartbeat_pct)
            snippets = self._fetch_transcript_via_ytdlp(video_id, language=language)
            self.log.debug("transcript_fetched", video_id=video_id, source="ytdlp_fallback", snippets=len(snippets))
            return snippets
        except (NoTranscriptFound, TranscriptsDisabled):
            raise
        except Exception as e:
            elapsed = time.time() - t0
            self.log.warning("transcript_api_error", video_id=video_id, error=f"{type(e).__name__}: {e}", fallback="ytdlp", elapsed=f"{elapsed:.1f}s")
            if progress_callback:
                progress_callback(heartbeat_pct)
            snippets = self._fetch_transcript_via_ytdlp(video_id, language=language)
            self.log.debug("transcript_fetched", video_id=video_id, source="ytdlp_fallback", snippets=len(snippets))
            return snippets

    def chunk_video(self, video_id, meta, enc, language="en", progress_callback=None, heartbeat_pct=0, chunking_enabled=True):
        """Full chunking pipeline for a single video.

        1. Fetch transcript
        2. Duration pre-split
        3. Token-based split with langchain
        4. Invariant enforcement

        When ``chunking_enabled`` is False, the entire transcript is written
        as a single document (no size-based splitting).

        Returns list of chunk dicts with 'filename', 'content', 'tokens'.
        """
        snippets = self.fetch_transcript(video_id, language=language, progress_callback=progress_callback, heartbeat_pct=heartbeat_pct)
        if not snippets:
            return []

        if not chunking_enabled:
            texts = []
            total_end = 0.0
            for snippet in snippets:
                start = snippet.start if hasattr(snippet, "start") else snippet["start"]
                duration = snippet.duration if hasattr(snippet, "duration") else snippet["duration"]
                text = snippet.text if hasattr(snippet, "text") else snippet["text"]
                texts.append(text)
                total_end = max(total_end, start + duration)
            full_text = " ".join(texts).strip()
            single_chunk = {
                "text": full_text,
                "start": 0,
                "end": total_end,
                "tokens": len(enc.encode(full_text)),
            }
            content = build_chunk_content(video_id, meta, single_chunk)
            return [
                {
                    "filename": f"{video_id}-full.txt",
                    "content": content,
                    "tokens": len(enc.encode(full_text)) + compute_metadata_overhead(video_id, meta, single_chunk, enc),
                }
            ]

        segments = split_by_duration(snippets, max_duration=MAX_DURATION_SECONDS)
        self.log.debug("split_done", video_id=video_id, segments=len(segments))

        if progress_callback:
            progress_callback(heartbeat_pct)
        self.log.debug("progress_updated", video_id=video_id, stage="after_split")

        all_chunks = []
        for seg_idx, seg in enumerate(segments):
            self.log.debug("segment_processing", video_id=video_id, seg_idx=seg_idx, seg_start=seg["start"], seg_end=seg["end"])
            if progress_callback:
                progress_callback(heartbeat_pct)
            seg_chunks = split_segment_tokens(seg, enc, chunk_size=EFFECTIVE_TARGET)
            self.log.debug("token_split_done", video_id=video_id, seg_idx=seg_idx, chunks=len(seg_chunks))

            chunk_size = EFFECTIVE_TARGET
            while any(c["tokens"] + compute_metadata_overhead(video_id, meta, c, enc) > MAX_CHUNK_TOKENS for c in seg_chunks):
                self.log.debug("metadata_check", video_id=video_id, seg_idx=seg_idx, chunk_size=chunk_size)
                chunk_size -= 10
                if chunk_size < 100:
                    break
                seg_chunks = split_segment_tokens(seg, enc, chunk_size=chunk_size)

            all_chunks.extend(seg_chunks)
            self.log.debug("chunks_accumulated", video_id=video_id, total_chunks=len(all_chunks))

        self.log.debug("chunk_video_build_start", video_id=video_id, total_chunks=len(all_chunks))
        results = []
        for i, chunk in enumerate(all_chunks):
            content = build_chunk_content(video_id, meta, chunk)
            filename = f"{video_id}-{i:03d}.txt"
            meta_tokens = compute_metadata_overhead(video_id, meta, chunk, enc)
            results.append(
                {
                    "filename": filename,
                    "content": content,
                    "tokens": len(enc.encode(chunk["text"])) + meta_tokens,
                }
            )
            self.log.debug("building_chunk", video_id=video_id, chunk_idx=i, filename=filename)

        self.log.debug("chunk_video_complete", video_id=video_id, results=len(results))
        return results

    def getData(self, config, progress_callback):
        channel_input = config.get("channel_id", "").strip()
        language = config.get("language", "en").strip() or "en"
        api_key = config.get("api_key", "").strip() or None
        max_videos = int(config.get("max_videos", 0))
        chunking_enabled = bool(config.get("chunking_enabled", False))

        if not channel_input:
            raise ValueError("channel_id is required")

        self.log.info("getData_started", channel=channel_input, language=language, max_videos=max_videos)

        enc = tiktoken.get_encoding("cl100k_base")

        # Resolve channel
        progress_callback(1)
        resolved = resolve_channel_id(channel_input, api_key)
        channel_id = resolved["channel_id"]
        channel_title = resolved.get("title") or channel_id
        self.log.info("channel_resolved", channel_id=channel_id, title=channel_title)

        # Enumerate videos
        progress_callback(5)
        if api_key:
            videos = enumerate_videos_api(channel_id, api_key, max_videos)
        else:
            videos = enumerate_videos_ytdlp(channel_id, max_videos)

        if not videos:
            raise RuntimeError(f"No videos found for channel {channel_id}")

        total_videos = len(videos)
        self.log.info("videos_enumerated", total=total_videos)
        progress_callback(5)

        # Reconciliation: find already-processed videos
        output_dir = self.get_destination_path()
        completed_videos = set()
        mode_files = {}
        if os.path.isdir(output_dir):
            for fname in os.listdir(output_dir):
                if not fname.endswith(".txt"):
                    continue
                vid_id = fname.rsplit("-", 1)[0]
                mode_files.setdefault(vid_id, set()).add(fname)
        # A video is "done" only when its on-disk output matches the CURRENT
        # chunking mode (chunking on → <vid>-NNN.txt; off → <vid>-full.txt).
        # Toggling chunking_enabled therefore reliably regenerates instead of
        # silently keeping stale, differently-chunked output.
        for vid_id, files in mode_files.items():
            if chunking_enabled:
                if any(_is_indexed_chunk(n, vid_id) for n in files):
                    completed_videos.add(vid_id)
            else:
                if f"{vid_id}-full.txt" in files:
                    completed_videos.add(vid_id)
        if completed_videos:
            self.log.info("reconciliation", already_processed=len(completed_videos))

        progress_callback(5, message=f"Found {total_videos} videos, {len(completed_videos)} already processed")

        # Process each video
        processed = 0
        skipped = 0
        current_progress = 5
        for idx, video in enumerate(videos):
            self.log.debug("loop_top", idx=idx, total=total_videos)
            vid_id = video["id"]
            self.log.debug("loop_vid_id", idx=idx, vid_id=vid_id)
            vid_title = video.get("title", vid_id)
            self.log.debug("loop_vid_title", idx=idx, vid_title=vid_title)

            current_progress = 5 + int(90 * idx / total_videos)
            self.log.debug("loop_progress_calc", idx=idx, current_progress=current_progress)
            progress_callback(current_progress, message=f"Processing {idx + 1}/{total_videos}")
            self.log.debug("loop_progress_done", idx=idx, current_progress=current_progress)

            if vid_id in completed_videos:
                self.log.debug("loop_already_done", idx=idx, vid_id=vid_id)
                continue

            # Clear any stale artifacts for this video (e.g. output produced
            # in the other chunking mode) so the regenerated files are not
            # mixed with old-format ones.
            for stale in mode_files.get(vid_id, ()):
                try:
                    os.remove(os.path.join(output_dir, stale))
                except OSError:
                    pass

            self.log.info("video_processing", video_id=vid_id, title=vid_title, idx=idx + 1, total=total_videos)

            meta = {
                "title": video.get("title", f"Video {vid_id}"),
                "published_at": video.get("published_at", "Unknown"),
                "view_count": video.get("view_count", "Unknown"),
            }

            try:
                t_video = time.time()
                chunks = self.chunk_video(vid_id, meta, enc, language=language, progress_callback=progress_callback, heartbeat_pct=current_progress, chunking_enabled=chunking_enabled)
            except (NoTranscriptFound, TranscriptsDisabled) as e:
                self.log.warning("video_skipped_no_transcript", video_id=vid_id, title=vid_title, error=f"{type(e).__name__}: {e}")
                skipped += 1
                continue
            except VideoUnavailable as e:
                self.log.warning("video_skipped_unavailable", video_id=vid_id, title=vid_title, error=f"{type(e).__name__}: {e}")
                skipped += 1
                continue
            except SubscriptionCancelledError:
                raise
            except requests.HTTPError as e:
                self.log.error("video_http_error", video_id=vid_id, title=vid_title, error=f"{type(e).__name__}: {e}")
                raise
            except Exception as e:
                self.log.error("video_skipped_error", video_id=vid_id, title=vid_title, error=f"{type(e).__name__}: {e}")
                skipped += 1
                continue

            if not chunks:
                self.log.warning("video_no_chunks", video_id=vid_id, title=vid_title)
                skipped += 1
                continue

            total_tokens = sum(c["tokens"] for c in chunks)
            self.log.debug("total_tokens", video_id=vid_id, tokens=total_tokens)
            for chunk_data in chunks:
                self.log.debug("writing_chunk", video_id=vid_id, filename=chunk_data["filename"])
                tmp_path = f"/tmp/{chunk_data['filename']}"
                with open(tmp_path, "w", encoding="utf-8") as f:
                    f.write(chunk_data["content"])
                self.move_to_destination(tmp_path)
                self.log.debug("chunk_moved", video_id=vid_id, filename=chunk_data["filename"])

            processed += 1
            self.log.debug("video_incremented", video_id=vid_id, processed=processed)
            elapsed_video = time.time() - t_video
            self.log.info("video_completed", video_id=vid_id, title=vid_title, chunks=len(chunks), tokens=total_tokens, elapsed=f"{elapsed_video:.1f}s")
            time.sleep(random.uniform(2, 5))
            self.log.debug("sleep_done", video_id=vid_id)

        self.log.info("getData_completed", processed=processed, skipped=skipped, total=total_videos)
        progress_callback(100, message=f"{processed} processed, {skipped} skipped out of {total_videos}")


if __name__ == "__main__":
    plugin = youTubeTranscriptionPlugin()
    plugin._subscription_name = "test"
    plugin.getData(
        {"channel_id": "@mkbhd", "language": "en", "max_videos": 2},
        lambda p: print(f"Progress: {p}%"),
    )
