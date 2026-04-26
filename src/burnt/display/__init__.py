"""Display outputs for different contexts."""

from __future__ import annotations

from typing import Any

from .export import to_json, to_markdown
from .notebook import to_html
from .terminal import to_table


def auto_render(result: Any) -> None:
    """Auto-detect environment and render appropriately.

    Uses IPython display if in a notebook, otherwise falls back to
    Rich terminal output.
    """
    if _in_notebook():
        html = to_html(result)
        try:
            from IPython.display import HTML, display

            display(HTML(html))
        except ImportError:
            print(html)
    else:
        to_table(result)


def _in_notebook() -> bool:
    """Detect if running inside a Jupyter/IPython notebook."""
    try:
        from IPython import get_ipython

        shell = get_ipython()
        if shell is None:
            return False
        return shell.__class__.__name__ == "ZMQInteractiveShell"
    except ImportError:
        return False


__all__ = [
    "auto_render",
    "to_html",
    "to_json",
    "to_markdown",
    "to_table",
]
