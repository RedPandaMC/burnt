"""Display outputs for different contexts."""

from .export import to_json, to_markdown
from .notebook import to_html
from .terminal import to_table

__all__ = [
    "to_html",
    "to_json",
    "to_markdown",
    "to_table",
]
