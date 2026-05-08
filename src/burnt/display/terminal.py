"""Terminal rendering with Rich."""

from __future__ import annotations

from typing import Any

from rich.console import Console
from rich.table import Table

console = Console()


def to_table(result: Any) -> None:
    """Render result as a Rich table.

    Args:
        result: CheckResult to render.
    """
    findings = getattr(result, "findings", [])
    file_path = getattr(result, "file_path", None) or "unknown"
    mode = getattr(result, "mode", "python")
    compute = getattr(result, "compute_seconds", None)

    console.print(f"\n[bold]burnt check:[/bold] {file_path}")
    console.print(f"[dim]mode:[/dim] {mode}")
    if compute is not None:
        console.print(f"[dim]compute:[/dim] {compute:.1f}s")
    console.print()

    if not findings:
        console.print("[green]✓ No cost anti-patterns found.[/green]\n")
        return

    table = Table(title=f"Findings ({len(findings)})")
    table.add_column("Severity", style="bold")
    table.add_column("Rule", style="cyan")
    table.add_column("Location")
    table.add_column("Message")

    for f in findings:
        sev_color = {
            "error": "red",
            "warning": "yellow",
            "info": "blue",
        }.get(f.severity, "white")

        location = f"line {f.line_number}" if f.line_number else "—"
        table.add_row(
            f"[{sev_color}]{f.severity.upper()}[/{sev_color}]",
            f.rule_id,
            location,
            f.message,
        )
        if f.suggestion:
            table.add_row("", "", "", f"[dim]→ {f.suggestion}[/dim]")

    console.print(table)
    console.print()


def to_table_multi(results: list[Any]) -> None:
    """Render multiple CheckResult objects as a single combined Rich table.

    Args:
        results: List of CheckResult to render.
    """
    all_findings = [(r, f) for r in results for f in getattr(r, "findings", [])]

    if not all_findings:
        console.print("[green]✓ No cost anti-patterns found.[/green]\n")
        return

    table = Table(title=f"Cost Anti-Patterns ({len(all_findings)})")
    table.add_column("File", style="cyan", no_wrap=True)
    table.add_column("Severity", style="bold")
    table.add_column("Rule", style="cyan")
    table.add_column("Location")
    table.add_column("Message")

    for result, f in all_findings:
        sev_color = {
            "error": "red",
            "warning": "yellow",
            "info": "blue",
        }.get(f.severity, "white")

        file_path = getattr(result, "file_path", None) or "unknown"
        location = f"line {f.line_number}" if f.line_number else "—"
        table.add_row(
            str(file_path),
            f"[{sev_color}]{f.severity.upper()}[/{sev_color}]",
            f.rule_id,
            location,
            f.message,
        )
        if getattr(f, "suggestion", None):
            table.add_row("", "", "", "", f"[dim]→ {f.suggestion}[/dim]")

    console.print(table)
    console.print()
