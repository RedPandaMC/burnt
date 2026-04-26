"""Runtime backends for executing queries in different contexts."""

from __future__ import annotations

from .auto import auto_backend, current_notebook_path
from .backend import Backend
from .spark_backend import SparkBackend

__all__ = [
    "Backend",
    "SparkBackend",
    "auto_backend",
    "current_notebook_path",
]


def _get_rest_backend():
    """Lazy import of RestBackend to avoid databricks-sdk dependency."""
    from .rest_backend import RestBackend

    return RestBackend
