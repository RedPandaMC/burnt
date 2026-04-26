"""Databricks-specific features for burnt.

Install with: pip install burnt[databricks]
"""

from __future__ import annotations

__all__ = [
    "DatabricksConnectionError",
    "DatabricksQueryError",
]


def __getattr__(name: str):
    if name in ("DatabricksConnectionError", "DatabricksQueryError"):
        from .exceptions import DatabricksConnectionError, DatabricksQueryError

        return locals()[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
