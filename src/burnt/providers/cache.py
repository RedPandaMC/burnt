"""Disk-backed cache for provider pricing data.

Extends the in-memory TTLCache with a filesystem layer so bulk JSON
downloads (e.g. AWS EC2 pricing) survive process restarts.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from threading import Lock


class PricingCache:
    """Thread-safe pricing cache with TTL + disk persistence.

    Use this for API response data that is expensive to fetch and safe to
    reuse for a period.  Bulk pricing files (AWS) are written to disk; smaller
    payloads (Azure retail prices, exchange rates) stay in memory.
    """

    def __init__(
        self,
        ttl_seconds: float = 86400.0,
        cache_dir: Path | None = None,
    ):
        self._ttl = ttl_seconds
        self._cache_dir = cache_dir or (Path.home() / ".cache" / "burnt")
        self._mem: dict[str, tuple[float, object]] = {}
        self._lock = Lock()
        self._mem_lock = Lock()

    # ---- memory cache ----

    def get(self, key: str) -> object | None:
        with self._mem_lock:
            entry = self._mem.get(key)
            if entry is None:
                return None
            expires_at, value = entry
            if time.monotonic() > expires_at:
                del self._mem[key]
                return None
            return value

    def set(self, key: str, value: object) -> None:
        with self._mem_lock:
            self._mem[key] = (time.monotonic() + self._ttl, value)

    # ---- disk cache ----

    def get_disk(self, key: str) -> object | None:
        path = self._disk_path(key)
        if not path.exists():
            return None
        try:
            mtime = path.stat().st_mtime
            import time as _time

            if _time.time() > mtime + self._ttl:
                path.unlink(missing_ok=True)
                return None
            with path.open() as f:
                return json.load(f)
        except Exception:
            return None

    def set_disk(self, key: str, value: object) -> None:
        path = self._disk_path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        import json

        with path.open("w") as f:
            json.dump(value, f)

    def _disk_path(self, key: str) -> Path:
        safe = key.replace("/", "_").replace(":", "_")
        return self._cache_dir / f"{safe}.json"

    def clear(self) -> None:
        with self._mem_lock:
            self._mem.clear()
        for p in self._cache_dir.glob("*.json"):
            p.unlink(missing_ok=True)
