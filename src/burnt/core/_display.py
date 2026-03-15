"""Shared display logic for burnt result types."""

from __future__ import annotations

import os
from abc import abstractmethod
from typing import Any


class _DisplayMixin:
    """Shared display logic for burnt result types.

    Subclasses must implement:
      - comparison_table() -> str    # ASCII/rich table
      - _to_html_table() -> str      # HTML for notebooks
      - to_markdown() -> str         # GFM markdown table
    """

    def display(self) -> None:
        """Render to terminal (rich) or Databricks notebook (HTML)."""
        if self._is_databricks_notebook():
            try:
                from IPython.display import HTML, display

                display(HTML(self._to_html_table()))
                return
            except ImportError:
                pass
        try:
            from rich.console import Console

            console = Console()
            console.print(self._render_rich())
        except ImportError:
            print(self.comparison_table())

    def _render_rich(self) -> Any:
        """Return rich-renderable object. Default: comparison_table() as string."""
        return self.comparison_table()

    def _is_databricks_notebook(self) -> bool:
        """Return True if running inside a Databricks notebook."""
        if not os.environ.get("DATABRICKS_RUNTIME_VERSION"):
            return False
        try:
            from pyspark.dbutils import DBUtils
            from pyspark.sql import SparkSession

            spark = SparkSession.getActiveSession()
            if spark is not None:
                DBUtils(spark)
                return True
        except ImportError:
            pass
        return False

    @abstractmethod
    def comparison_table(self) -> str:
        """Generate ASCII comparison table."""
        raise NotImplementedError

    @abstractmethod
    def _to_html_table(self) -> str:
        """Generate HTML table for notebooks."""
        raise NotImplementedError

    @abstractmethod
    def to_markdown(self) -> str:
        """Return a GFM markdown table suitable for pasting into Slack/GitHub/Confluence."""
        raise NotImplementedError
