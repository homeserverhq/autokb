"""Redis queue/lock helpers and the QueueManager class."""

import json
import os
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


class QueueManager:
    """Wrapper around Redis for the two-tier queue and safety locks.

    Queue items are JSON ``{"sub_id": ..., "operation": "FULL"|"SINK_ONLY"}``.
    """

    def __init__(self, url: str):
        self.url = url
        self._client = _new_client(url)

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
        """Remove all occurrences of a sub_id from both queues. Returns count removed."""
        n = self._remove_all_for_sub(P_QUEUE_KEY, sub_id)
        n += self._remove_all_for_sub(S_QUEUE_KEY, sub_id)
        return n

    def _remove_all_for_sub(self, key: str, sub_id: str) -> int:
        removed = 0
        items = self._client.lrange(key, 0, -1)
        keep = []
        for raw in items:
            parsed = _decode_item(raw)
            if parsed is None or parsed.get("sub_id") == sub_id:
                removed += 1
            else:
                keep.append(raw)
        if removed:
            self._client.delete(key)
            for raw in keep:
                self._client.lpush(key, raw)
        return removed

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

    def push_secondary_if_locked(self, sub_id: str, operation: str = OPERATION_FULL) -> bool:
        """Push to the S-Queue if the safety lock is held by someone else."""
        if not self.acquire_lock(sub_id, blocking=False):
            self.push_secondary(sub_id, operation=operation)
            return True
        return False

    # ----- lock operations -----
    def _lock_key(self, sub_id: str) -> str:
        return f"{LOCK_KEY_PREFIX}{sub_id}"

    def acquire_lock(self, sub_id: str, *, blocking: bool = True, ttl: int = LOCK_TTL) -> bool:
        key = self._lock_key(sub_id)
        if blocking:
            for _ in range(50):
                if self._client.set(key, "1", nx=True, ex=ttl):
                    return True
                time.sleep(0.1)
            return False
        return bool(self._client.set(key, "1", nx=True, ex=ttl))

    def refresh_lock(self, sub_id: str, ttl: int = LOCK_TTL) -> bool:
        return bool(self._client.expire(self._lock_key(sub_id), ttl))

    def release_lock(self, sub_id: str) -> None:
        self._client.delete(self._lock_key(sub_id))

    def lock_held(self, sub_id: str) -> bool:
        return bool(self._client.exists(self._lock_key(sub_id)))

    def force_release_lock(self, sub_id: str) -> None:
        self.release_lock(sub_id)


__all__ = ["QueueManager", "wait_for_redis"]
