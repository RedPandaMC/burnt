"""burnt.check() - Static code analysis."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from pathlib import Path


def run(
    path: str | Path | None = None,
    *,
    max_cost: float | None = None,
    severity: Literal["error", "warning", "info"] = "warning",
    skip: list[str] | None = None,
    only: list[str] | None = None,
    cluster: str | None = None,
    json: bool = False,
    markdown: bool = False,
) -> Any:
    """Run static analysis on a notebook, Python file, or SQL file."""
    raise NotImplementedError(
        "burnt.check() requires burnt-engine to be installed. "
        "See https://burnt.ai/docs/install for instructions."
    )


__all__ = ["run"]
