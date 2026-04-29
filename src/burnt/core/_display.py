"""Shared display logic for burnt result types."""

from __future__ import annotations

from abc import abstractmethod
from typing import Any


class _DisplayMixin:
    """Shared display logic for burnt result types.

    Subclasses must implement:
      - comparison_table() -> str    # ASCII/rich table
      - to_markdown() -> str         # GFM markdown table
    """

    def display(self) -> None:
        """Render to terminal using Rich."""
        try:
            from rich.console import Console

            console = Console()
            console.print(self._render_rich())
        except ImportError:
            print(self.comparison_table())

    def _render_rich(self) -> Any:
        """Return rich-renderable object. Default: comparison_table() as string."""
        return self.comparison_table()

    @abstractmethod
    def comparison_table(self) -> str:
        """Generate ASCII comparison table."""
        raise NotImplementedError

    @abstractmethod
    def to_markdown(self) -> str:
        """Return a GFM markdown table suitable for pasting into Slack/GitHub/Confluence."""
        raise NotImplementedError
