"""Databricks-specific exceptions."""

from __future__ import annotations

from burnt.core.exceptions import BurntError


class DatabricksConnectionError(BurntError):
    """Raised when connection to Databricks workspace fails."""

    pass


class DatabricksQueryError(BurntError):
    """Raised when a SQL statement execution fails on Databricks."""
