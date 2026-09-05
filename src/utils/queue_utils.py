"""Redis queue/lock helpers and the QueueManager class."""

import json
import os
import secrets
import socket
import time
from contextlib import contextmanager
from typing import Any, Dict, Iterable, List, Optional, Set

import redis

from .constants import (
    ALL_OPERATIONS,
    LOCK_KEY_PREFIX,
    LOCK_TTL,
    OPERATION_FULL,
    OPERATION_SINK_ONLY,
    P_QUEUE_KEY,
    S_QUEUE_KEY,
    STARTUP_RETRY_SLEEP,
    MAX_STARTUP_RETRIES,
)


def _new_client(url: str) -> redis.Redis:
    client = redis.Redis.from_url(
        url,
        decode_responses=True,
        socket_connect_timeout=5,
        socket_timeout=5,
        health_check_interval=15,
        retry_on_timeout=True,
        socket_keepalive=True,
        max_connections=10,
    )
    return client


def _safe_url(url: str) -> str:
    from urllib.parse import urlparse, urlunparse
    parsed = urlparse(url)
    if parsed.password:
        netloc = f"{parsed.username}:****@{parsed.hostname}"
        if parsed.port:
            netloc += f":{parsed.port}"
        return urlunparse(parsed._replace(netloc=netloc))
    return url


def wait_for_redis(url: str, log_func=None) -> redis.Redis:
    """Block until Redis is reachable, with retry."""
    for i in range(MAX_STARTUP_RETRIES):
        try:
            client = _new_client(url)
            client.ping()
            if log_func:
                log_func("redis_connected", f"url={_safe_url(url)} attempt={i + 1}")
            return client
        except Exception as exc:  # noqa: BLE001
            if log_func:
                log_func("redis_retry", f"attempt={i + 1} error={_safe_url(str(exc))}")
            time.sleep(STARTUP_RETRY_SLEEP)
    raise RuntimeError(f"Could not connect to Redis at {url} after {MAX_STARTUP_RETRIES} retries")


def _encode_item(sub_id: str, operation: str) -> str:
    """Deterministic JSON encoding for a queue item."""
    return json.dumps({"sub_id": sub_id, "operation": operation}, sort_keys=True, separators=(",", ":"))


def _decode_item(raw: str) -> Optional[Dict[str, str]]:
    """Parse a queue item, returning None for malformed payloads (logged by caller)."""
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return None


# Atomically remove every occurrence of a sub_id from a queue list. Uses a Lua
# script so a concurrent producer push can neither be read-then-lost nor wipe
# out items added by another process mid-drain (the classic lrange→delete→
# re-push race). Items for OTHER subscriptions are preserved in order.
_REMOVE_FOR_SUB_SCRIPT = """
local removed = 0
local keep = {}
for _, raw in ipairs(redis.call('LRANGE', KEYS[1], 0, -1)) do
    local ok, parsed = pcall(cjson.decode, raw)
    if ok and parsed and parsed['sub_id'] == ARGV[1] then
        removed = removed + 1
    else
        table.insert(keep, raw)
    end
end
if removed > 0 then
    redis.call('DEL', KEYS[1])
    for _, raw in ipairs(keep) do
        redis.call('RPUSH', KEYS[1], raw)
    end
end
return removed
"""

# Compare-and-delete for the safety lock: only the process holding <token> may
# release the lock, so a stale/duplicate holder can never drop another holder's
# lock out from under it (lock fencing).
_RELEASE_LOCK_SCRIPT = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
"""


class QueueManager:
    """Wrapper around Redis for the two-tier queue and safety locks.

    Queue items are JSON ``{"sub_id": ..., "operation": "FULL"|"SINK_ONLY"}``.
    """

    def __init__(self, url: str):
        self.url = url
        self._client = _new_client(url)
        # Per-sub lock tokens for this process's acquisitions, used for
        # fenced (compare-and-delete) releases and tokenized refreshes.
        self._lock_tokens: Dict[str, str] = {}

    @property
    def client(self) -> redis.Redis:
        return self._client

    # ----- queue operations -----
    def push_primary(self, sub_id: str, operation: str = OPERATION_FULL) -> None:
        self._client.lpush(P_QUEUE_KEY, _encode_item(sub_id, operation))

    def push_secondary(self, sub_id: str, operation: str = OPERATION_FULL) -> None:
        self._client.lpush(S_QUEUE_KEY, _encode_item(sub_id, operation))

    def pop_primary(self, timeout: int = 5) -> Optional[Dict[str, str]]:
        try:
            item = self._client.brpop(P_QUEUE_KEY, timeout=timeout)
        except Exception:
            return None
        if not item:
            return None
        parsed = _decode_item(item[1])
        if parsed is None:
            return None
        return parsed

    def drain_all(self, sub_id: str) -> int:
        """Atomically remove all occurrences of ``sub_id`` from both queues.

        Returns the number of items removed. Atomic (Lua) so a concurrent push
        can never be dropped between a read and a delete, and never wipes out
        other subscriptions' items.
        """
        n = int(self._client.eval(_REMOVE_FOR_SUB_SCRIPT, 1, P_QUEUE_KEY, sub_id))
        n += int(self._client.eval(_REMOVE_FOR_SUB_SCRIPT, 1, S_QUEUE_KEY, sub_id))
        return n

    def promote_orphans(self) -> int:
        """Move S-queue items whose safety lock is no longer held back to the
        primary queue so some worker consumes them.

        The S-queue has no consumer of its own; items land there when the lock
        is busy, and a narrow race can leave one behind after the last run
        finished. ``promote_orphans`` is safe to call concurrently from every
        worker (``LREM`` is atomic, so each item is promoted exactly once).
        """
        promoted = 0
        for raw in self._client.lrange(S_QUEUE_KEY, 0, -1):
            parsed = _decode_item(raw)
            if parsed is None:
                self._client.lrem(S_QUEUE_KEY, 1, raw)
                promoted += 1
                continue
            sub_id = parsed.get("sub_id")
            if not sub_id:
                continue
            if not self._client.exists(self._lock_key(sub_id)):
                if self._client.lrem(S_QUEUE_KEY, 1, raw):
                    self._client.lpush(P_QUEUE_KEY, raw)
                    promoted += 1
        return promoted

    def any_full_for(self, sub_id: str) -> bool:
        """Return True if any queued item for *sub_id* has operation=FULL."""
        for key in (P_QUEUE_KEY, S_QUEUE_KEY):
            for raw in self._client.lrange(key, 0, -1):
                parsed = _decode_item(raw)
                if parsed and parsed.get("sub_id") == sub_id and parsed.get("operation") == OPERATION_FULL:
                    return True
        return False

    def has_in_queue(self, sub_id: str) -> bool:
        for key in (P_QUEUE_KEY, S_QUEUE_KEY):
            for raw in self._client.lrange(key, 0, -1):
                parsed = _decode_item(raw)
                if parsed and parsed.get("sub_id") == sub_id:
                    return True
        return False

    def queue_depth(self, key: str) -> int:
        return self._client.llen(key)

    # ----- lock operations -----
    def _lock_key(self, sub_id: str) -> str:
        return f"{LOCK_KEY_PREFIX}{sub_id}"

    def acquire_lock(self, sub_id: str, *, blocking: bool = True, ttl: int = LOCK_TTL) -> Optional[str]:
        """Acquire the safety lock for ``sub_id``.

        Returns the acquisition token on success (used for fenced release /
        refresh) or ``None`` if the lock could not be acquired. The value
        stored in Redis is the unique token, so an unreachable stale holder can
        never release a lock that a newer holder re-acquired.
        """
        key = self._lock_key(sub_id)
        token = secrets.token_hex(16)
        if blocking:
            for _ in range(50):
                if self._client.set(key, token, nx=True, ex=ttl):
                    self._lock_tokens[sub_id] = token
                    return token
                time.sleep(0.1)
            return None
        if self._client.set(key, token, nx=True, ex=ttl):
            self._lock_tokens[sub_id] = token
            return token
        return None

    def refresh_lock(self, sub_id: str, ttl: int = LOCK_TTL) -> bool:
        """Refresh the TTL of a lock this process owns (tokenized)."""
        token = self._lock_tokens.get(sub_id)
        if token is None:
            return False
        if self._client.get(self._lock_key(sub_id)) != token:
            return False
        return bool(self._client.expire(self._lock_key(sub_id), ttl))

    def release_lock(self, sub_id: str) -> bool:
        """Release the lock only if this process still holds it (fenced)."""
        token = self._lock_tokens.pop(sub_id, None)
        if token is None:
            return False
        return bool(self._client.eval(_RELEASE_LOCK_SCRIPT, 1, self._lock_key(sub_id), token))

    def lock_held(self, sub_id: str) -> bool:
        return bool(self._client.exists(self._lock_key(sub_id)))

    def force_release_lock(self, sub_id: str) -> None:
        """Escalate and drop a lock regardless of holder (watchdog path).

        Only used when the owning worker is believed dead; distinct from the
        fenced ``release_lock`` so normal shutdown can never clobber a lock
        re-acquired by someone else.
        """
        self._client.delete(self._lock_key(sub_id))


__all__ = ["QueueManager", "wait_for_redis"]
