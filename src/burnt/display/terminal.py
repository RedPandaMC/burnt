"""Terminal rendering with Rich."""

from __future__ import annotations

from typing import Any


def to_table(result: Any) -> None:
    """Render result as a Rich table.

    Args:
        result: CheckResult or WatchResult to render.
    """
    raise NotImplementedError(
        "Terminal display requires burnt-engine. "
        "Install with: pip install burnt[engine]"
    )
