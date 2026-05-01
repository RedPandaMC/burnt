"""Session management — thin wrapper around runtime.spark_monitor."""

from __future__ import annotations

from .runtime.spark_monitor import SessionState, collect, start

__all__ = ["SessionState", "collect", "start"]
