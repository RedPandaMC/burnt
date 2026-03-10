"""Simple in-memory TTL cache with thread-safe operations."""

from dataclasses import dataclass
from threading import Lock
from time import monotonic
from typing import Any


@dataclass
class _CacheEntry:
    value: Any
    expires_at: float


class TTLCache:
    """Thread-safe in-memory cache with TTL expiration."""

    def __init__(self, ttl_seconds: float = 3600.0):
        self._cache: dict[str, _CacheEntry] = {}
        self._lock = Lock()
        self._ttl = ttl_seconds

    def get(self, key: str) -> Any | None:
        """Get value if exists and not expired."""
        with self._lock:
            entry = self._cache.get(key)
            if entry is None:
                return None
            if monotonic() > entry.expires_at:
                del self._cache[key]
                return None
            return entry.value

    def set(self, key: str, value: Any) -> None:
        """Set value with TTL expiration."""
        with self._lock:
            self._cache[key] = _CacheEntry(
                value=value, expires_at=monotonic() + self._ttl
            )

    def clear(self) -> None:
        """Clear all cached entries."""
        with self._lock:
            self._cache.clear()

    @property
    def ttl(self) -> float:
        """Get TTL in seconds."""
        return self._ttl
