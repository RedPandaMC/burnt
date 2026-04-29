"""Display outputs for different contexts."""

from __future__ import annotations

from .export import to_json, to_markdown
from .terminal import to_table


def auto_render(result) -> None:
    """Render to terminal using Rich."""
    to_table(result)


__all__ = [
    "auto_render",
    "to_json",
    "to_markdown",
    "to_table",
]
