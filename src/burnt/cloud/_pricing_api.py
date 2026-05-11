"""Shared pricing API infrastructure — TTL cache and client protocol."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Generic, NamedTuple, Protocol, TypeVar

if TYPE_CHECKING:
    from decimal import Decimal

T = TypeVar("T")


class _CacheEntry(NamedTuple):
    value: object
    fetched_at: float  # time.monotonic()


class PriceCache(Generic[T]):
    """Thread-safe TTL cache keyed by arbitrary hashable keys."""

    def __init__(self, ttl_seconds: float = 86_400) -> None:
        self._ttl = ttl_seconds
        self._store: dict[object, _CacheEntry] = {}

    def get(self, key: object) -> T | None:
        entry = self._store.get(key)
        if entry is None:
            return None
        if (time.monotonic() - entry.fetched_at) >= self._ttl:
            del self._store[key]
            return None
        return entry.value  # type: ignore[return-value]

    def set(self, key: object, value: T) -> None:
        self._store[key] = _CacheEntry(value=value, fetched_at=time.monotonic())

    def invalidate(self, key: object) -> None:
        self._store.pop(key, None)

    def clear(self) -> None:
        self._store.clear()


class LivePricingClient(Protocol):
    """Protocol for cloud-specific live pricing clients."""

    def get_instance_price_usd(self, instance_type: str, region: str) -> Decimal:
        """Return the on-demand hourly price in USD for an instance type/region."""
        ...
