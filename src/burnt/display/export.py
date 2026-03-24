"""Export to various formats."""

from __future__ import annotations

from typing import Any


def to_json(result: Any) -> str:
    """Export result as JSON.

    Args:
        result: CheckResult or WatchResult to export.

    Returns:
        JSON string.
    """
    raise NotImplementedError(
        "JSON export requires burnt-engine. Install with: pip install burnt[engine]"
    )


def to_markdown(result: Any) -> str:
    """Export result as Markdown.

    Args:
        result: CheckResult or WatchResult to export.

    Returns:
        Markdown string.
    """
    raise NotImplementedError(
        "Markdown export requires burnt-engine. Install with: pip install burnt[engine]"
    )
