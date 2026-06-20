"""Redis queue/lock helpers and the QueueManager class."""

import json
import os
import socket
import time
from contextlib import contextmanager
from typing import Iterable, List, Optional, Set

import redis

from .constants import (
    LOCK_KEY_PREFIX,
    LOCK_TTL,
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


class QueueManager:
    """Wrapper around Redis for the two-tier queue and safety locks."""

    def __init__(self, url: str):
        self.url = url
        self._client = _new_client(url)

    @property
    def client(self) -> redis.Redis:
        return self._client

    # ----- queue operations -----
    def push_primary(self, sub_id: str) -> None:
        self._client.lpush(P_QUEUE_KEY, sub_id)

    def push_secondary(self, sub_id: str) -> None:
        self._client.lpush(S_QUEUE_KEY, sub_id)

    def pop_primary(self, timeout: int = 5) -> Optional[str]:
        try:
            item = self._client.brpop(P_QUEUE_KEY, timeout=timeout)
        except Exception:
            # Timeout or transient connection issue — treat as "no item"
            return None
        return item[1] if item else None

    def drain_primary(self, sub_id: str) -> int:
        """Remove all occurrences of ``sub_id`` from the P-Queue. Returns count removed."""
        return self._remove_all(P_QUEUE_KEY, sub_id)

    def drain_both(self, sub_id: str) -> int:
        n = self._remove_all(P_QUEUE_KEY, sub_id)
        n += self._remove_all(S_QUEUE_KEY, sub_id)
        return n

    def _remove_all(self, key: str, sub_id: str) -> int:
        removed = 0
        while True:
            n = self._client.lrem(key, 1, sub_id)
            if not n:
                break
            removed += n
        return removed

    def queue_depth(self, key: str) -> int:
        return self._client.llen(key)

    def has_in_queue(self, sub_id: str) -> bool:
        return sub_id in self._client.lrange(P_QUEUE_KEY, 0, -1) or sub_id in self._client.lrange(S_QUEUE_KEY, 0, -1)

    def push_secondary_if_locked(self, sub_id: str) -> bool:
        """Push ``sub_id`` to the S-Queue if the safety lock is held by someone else."""
        if not self.acquire_lock(sub_id, blocking=False):
            self.push_secondary(sub_id)
            return True
        return False

    # ----- lock operations -----
    def _lock_key(self, sub_id: str) -> str:
        return f"{LOCK_KEY_PREFIX}{sub_id}"

    def acquire_lock(self, sub_id: str, *, blocking: bool = True, ttl: int = LOCK_TTL) -> bool:
        key = self._lock_key(sub_id)
        if blocking:
            # Loop briefly to acquire
            for _ in range(50):  # 50 * 0.1s = 5s
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
