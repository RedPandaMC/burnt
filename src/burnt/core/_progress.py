"""Progress feedback utility for multi-tier estimation."""

from __future__ import annotations

import time
from contextlib import contextmanager
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Generator


class _ProgressTracker:
    """Tracks progress during multi-tier estimation.

    Shows tier-by-tier progress with timing information.
    Activated when estimation takes >200ms or when verbose=True.
    """

    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self.start_time: float | None = None
        self.tier_times: dict[str, float] = {}
        self._has_display = False
        self._display_handle = None

    def _should_display(self) -> bool:
        """Check if we should display progress (rich or IPython available)."""
        if self._has_display:
            return True

        import importlib.util

        # Check for rich
        if importlib.util.find_spec("rich") is not None:
            self._has_display = True
            return True
        # Check for IPython (Databricks)
        if importlib.util.find_spec("IPython") is not None:
            self._has_display = True
            return True
        return False

    def start(self, message: str = "Estimating query cost...") -> None:
        """Start progress tracking."""
        self.start_time = time.time()
        if not self._should_display():
            return

        if self.verbose:
            self._print(message)

    def _print(self, message: str) -> None:
        """Print message using available display method."""
        if not self._should_display():
            return
        try:
            from rich.console import Console

            console = Console()
            console.print(message)
        except ImportError:
            print(message)

    def tier_complete(
        self, tier_name: str, duration_ms: float, details: str = ""
    ) -> None:
        """Mark a tier as complete."""
        self.tier_times[tier_name] = duration_ms
        if not self._should_display():
            return
        if self.verbose or duration_ms > 200:
            check = "[✓]"
            detail_str = f" — {details}" if details else ""
            self._print(f"  {check} {tier_name}{detail_str} ({duration_ms:.0f}ms)")

    def tier_start(self, tier_name: str) -> None:
        """Mark a tier as started (for long-running operations)."""
        if not self._should_display():
            return
        if self.verbose:
            self._print(f"  [⟳] {tier_name}...")

    def done(self, confidence: str) -> None:
        """Mark estimation as complete."""
        if not self._should_display():
            return
        if self.verbose or (self.start_time and time.time() - self.start_time > 0.2):
            self._print(f"Done. Confidence: {confidence}")


@contextmanager
def _progress_context(verbose: bool = False) -> Generator[_ProgressTracker, None, None]:
    """Context manager for progress tracking during estimation.

    Usage:
        with _progress_context(verbose=True) as progress:
            progress.start()
            # ... do work ...
            progress.tier_complete("Static analysis", 2)
            progress.done("high")
    """
    tracker = _ProgressTracker(verbose=verbose)
    try:
        yield tracker
    finally:
        pass  # Cleanup if needed
