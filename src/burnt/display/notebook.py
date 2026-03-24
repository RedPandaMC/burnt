"""HTML rendering for notebooks."""

from __future__ import annotations

from typing import Any


def to_html(result: Any) -> str:
    """Render result as HTML for notebooks.

    Args:
        result: CheckResult or WatchResult to render.

    Returns:
        HTML string.
    """
    raise NotImplementedError(
        "Notebook display requires burnt-engine. "
        "Install with: pip install burnt[engine]"
    )
