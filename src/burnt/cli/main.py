"""burnt CLI — static analysis tooling. Zero credentials required."""

from __future__ import annotations

import fnmatch
from pathlib import Path
from typing import TYPE_CHECKING

import typer

if TYPE_CHECKING:
    from ..parsers.antipatterns import AntiPattern
from rich.console import Console
from rich.table import Table

from ..core.config import Settings

app = typer.Typer(
    help="burnt - Pre-execution cost analysis for Databricks",
    no_args_is_help=True,
)
cache_app = typer.Typer(help="Manage the burnt local cache")
app.add_typer(cache_app, name="cache")

console = Console()

# ---------------------------------------------------------------------------
# --version eager flag
# ---------------------------------------------------------------------------


def _version_callback(value: bool) -> None:
    if value:
        from .. import __version__

        console.print(f"burnt v{__version__}")
        raise typer.Exit()


@app.callback()
def main(
    version: bool | None = typer.Option(
        None,
        "--version",
        "-V",
        callback=_version_callback,
        is_eager=True,
        help="Show version and exit.",
    ),
) -> None:
    pass


# ---------------------------------------------------------------------------
# burnt check
# ---------------------------------------------------------------------------

def _build_rule_severities() -> dict[str, str]:
    """Build the rule-id → severity dict from the REGISTRY."""
    from ..parsers.registry import REGISTRY

    return {rule_id: str(rule.severity) for rule_id, rule in REGISTRY.items()}


_RULE_SEVERITIES: dict[str, str] = _build_rule_severities()


@app.command()
def check(
    path: str = typer.Argument(..., help="File or directory to check"),
    fail_on: str = typer.Option(
        "error", "--fail-on", help="Exit with code 1 on severity: info|warning|error"
    ),
    output: str = typer.Option(
        "table", "--output", "-o", help="Output format: table|text|json"
    ),
    ignore_rule: list[str] = typer.Option(  # noqa: B008
        [], "--ignore-rule", help="Skip a specific rule ID (repeatable)"
    ),
) -> None:
    """Check SQL/PySpark files for cost anti-patterns."""
    from ..parsers.antipatterns import detect_antipatterns

    # Load config, merge with CLI overrides
    _config_path, settings = Settings.discover()
    effective_ignore = set(settings.lint.ignore) | set(ignore_rule)

    target = Path(path)
    if not target.exists():
        console.print(f"[red]Error:[/red] Path not found: {path}")
        raise typer.Exit(1)

    files_to_check: list[Path] = []
    if target.is_file():
        files_to_check.append(target)
    else:
        for ext in ("*.sql", "*.py"):
            for f in sorted(target.rglob(ext)):
                # Apply lint.exclude globs
                if not _is_excluded(f, settings.lint.exclude, target):
                    files_to_check.append(f)

    if not files_to_check:
        console.print("[yellow]No .sql or .py files found to check.[/yellow]")
        raise typer.Exit(0)

    severity_levels = {"info": 1, "warning": 2, "error": 3}
    fail_threshold = severity_levels.get(fail_on.lower(), 3)

    all_issues: list[tuple[Path, AntiPattern]] = []
    fail_build = False

    for file_path in files_to_check:
        source = file_path.read_text(encoding="utf-8")
        lang = "pyspark" if file_path.suffix == ".py" else "sql"

        # Determine per-file ignores
        file_ignores = set(effective_ignore)
        for glob_pattern, rule_ids in settings.lint.per_file_ignores.items():
            if fnmatch.fnmatch(str(file_path), glob_pattern) or fnmatch.fnmatch(
                file_path.name, glob_pattern
            ):
                file_ignores.update(rule_ids)

        # Determine active rules
        if settings.lint.select == ["ALL"]:
            active_rules = set(_RULE_SEVERITIES.keys())
        else:
            active_rules = set(settings.lint.select)
        active_rules -= file_ignores

        issues = detect_antipatterns(source, lang)
        # Filter to active rules
        issues = [i for i in issues if i.name not in file_ignores and i.name in active_rules]

        for issue in issues:
            all_issues.append((file_path, issue))
            if severity_levels.get(str(issue.severity), 0) >= fail_threshold:
                fail_build = True

    if not all_issues:
        console.print("[green]No cost anti-patterns found.[/green]")
        raise typer.Exit(0)

    if output == "json":
        import json

        data = [
            {
                "file": str(fp),
                "rule": issue.name,
                "severity": issue.severity,
                "description": issue.description,
                "suggestion": issue.suggestion,
            }
            for fp, issue in all_issues
        ]
        console.print(json.dumps(data, indent=2))
    elif output == "text":
        for file_path, issue in all_issues:
            color = "red" if issue.severity == "error" else "yellow" if issue.severity == "warning" else "blue"
            console.print(f"{file_path}: [{color}]{issue.severity.upper()}[/{color}] {issue.name}: {issue.description}")
            console.print(f"  [dim]Suggestion: {issue.suggestion}[/dim]")
    else:
        # table (default)
        table = Table(title="Cost Anti-Patterns")
        table.add_column("File", style="cyan", no_wrap=True)
        table.add_column("Rule", style="bold")
        table.add_column("Severity")
        table.add_column("Description")

        for file_path, issue in all_issues:
            sev_color = (
                "red" if issue.severity == "error"
                else "yellow" if issue.severity == "warning"
                else "blue"
            )
            table.add_row(
                str(file_path),
                issue.name,
                f"[{sev_color}]{issue.severity}[/{sev_color}]",
                issue.description,
            )
        console.print(table)

    if fail_build:
        raise typer.Exit(1)


def _is_excluded(file_path: Path, exclude_patterns: list[str], root: Path) -> bool:
    rel = str(file_path.relative_to(root))
    for pattern in exclude_patterns:
        if fnmatch.fnmatch(rel, pattern) or fnmatch.fnmatch(str(file_path), pattern):
            return True
    return False


# ---------------------------------------------------------------------------
# burnt advise
# ---------------------------------------------------------------------------


@app.command()
def advise(
    run_id: str = typer.Option(None, "--run-id", help="Databricks Job Run ID"),
    statement_id: str = typer.Option(None, "--statement-id", help="SQL statement ID"),
    job_id: str = typer.Option(None, "--job-id", help="Databricks Job ID"),
    job_name: str = typer.Option(None, "--job-name", help="Databricks Job name"),
    output: str = typer.Option("table", "--output", "-o", help="Output format: table|json|text"),
) -> None:
    """Analyze a historical run and recommend an optimized cluster configuration."""
    import burnt

    try:
        if job_id:
            console.print(f"[bold blue]Analyzing job {job_id}...[/bold blue]")
            advice = burnt.advise(job_id=job_id)
            if advice.num_runs_analyzed:
                console.print(
                    f"[dim]Based on {advice.num_runs_analyzed} runs — {advice.confidence_level} confidence[/dim]"
                )
        elif job_name:
            console.print(f"[dim]Looking up job '{job_name}'...[/dim]")
            advice = burnt.advise(job_name=job_name)
        elif run_id or statement_id:
            advice = burnt.advise(run_id=run_id, statement_id=statement_id)
        else:
            console.print("[red]Error:[/red] Provide --run-id, --statement-id, --job-id, or --job-name.")
            console.print("[dim]Hint: to analyze a notebook session, use burnt.advise() in Python.[/dim]")
            raise typer.Exit(1)

        if output == "json":
            import json
            console.print(json.dumps(advice.model_dump(), indent=2))
        elif output == "text":
            console.print(advice.comparison_table())
            if advice.insights:
                for insight in advice.insights:
                    console.print(f"• {insight}")
            console.print("\nRecommended Cluster:")
            console.print(advice.recommended.model_dump_json(indent=2))
        else:
            advice.display()

    except NotImplementedError as e:
        console.print(f"[red]Not implemented:[/red] {e}")
        raise typer.Exit(1) from e
    except RuntimeError as e:
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1) from e
    except Exception as e:
        console.print(f"[red]Unexpected error:[/red] {e}")
        raise typer.Exit(1) from e


# ---------------------------------------------------------------------------
# burnt init
# ---------------------------------------------------------------------------

_BURNT_TOML_TEMPLATE = """\
[lint]
select = ["ALL"]
ignore = []
fail-on = "error"
exclude = []

[cache]
enabled = true
ttl-seconds = 3600
"""

_PYPROJECT_BURNT_SECTION = """
[tool.burnt]

[tool.burnt.lint]
select = ["ALL"]
ignore = []
fail-on = "error"
exclude = []

[tool.burnt.cache]
enabled = true
ttl-seconds = 3600
"""


@app.command()
def init() -> None:
    """Interactive project setup — creates config and updates .gitignore."""
    cwd = Path.cwd()

    # Determine default format
    has_pyproject = (cwd / "pyproject.toml").exists()
    default_format = "pyproject.toml" if has_pyproject else ".burnt.toml"

    fmt = typer.prompt(
        "Config format? [pyproject.toml / .burnt.toml]",
        default=default_format,
    ).strip()

    if fmt == "pyproject.toml":
        target = cwd / "pyproject.toml"
        if target.exists():
            # Check if [tool.burnt] already there
            try:
                import tomllib
                with open(target, "rb") as f:
                    data = tomllib.load(f)
                if data.get("tool", {}).get("burnt") and not typer.confirm(
                    "[tool.burnt] already exists. Overwrite?", default=False
                ):
                    console.print("[yellow]Skipped config.[/yellow]")
                    target = None
            except Exception:
                pass

            if target:
                with open(target, "a") as f:
                    f.write(_PYPROJECT_BURNT_SECTION)
                console.print(f"[green]✓[/green] Added [tool.burnt] to {target}")
        else:
            target.write_text(f"[tool.burnt]{_PYPROJECT_BURNT_SECTION}")
            console.print(f"[green]✓[/green] Created {target}")

    else:
        target = cwd / ".burnt.toml"
        if target.exists() and not typer.confirm(
            ".burnt.toml already exists. Overwrite?", default=False
        ):
            console.print("[yellow]Skipped config.[/yellow]")
            target = None
        if target:
            target.write_text(_BURNT_TOML_TEMPLATE)
            console.print(f"[green]✓[/green] Created {target}")

    # Add .burnt/cache/ to .gitignore
    gitignore = cwd / ".gitignore"
    cache_entry = ".burnt/cache/"
    if gitignore.exists():
        content = gitignore.read_text()
        if cache_entry not in content:
            with open(gitignore, "a") as f:
                f.write(f"\n# burnt cache\n{cache_entry}\n")
            console.print(f"[green]✓[/green] Added {cache_entry} to .gitignore")
    else:
        gitignore.write_text(f"# burnt cache\n{cache_entry}\n")
        console.print(f"[green]✓[/green] Created .gitignore with {cache_entry}")

    # Optionally generate examples
    if typer.confirm("Generate examples/?", default=True):
        _run_tutorial()


# ---------------------------------------------------------------------------
# burnt tutorial
# ---------------------------------------------------------------------------

_NOTEBOOK_TEMPLATE = """\
{{
 "nbformat": 4,
 "nbformat_minor": 5,
 "metadata": {{"kernelspec": {{"display_name": "Python 3", "language": "python", "name": "python3"}}}},
 "cells": [
  {{
   "cell_type": "markdown",
   "metadata": {{}},
   "source": ["# {title}\\n"]
  }},
  {{
   "cell_type": "code",
   "execution_count": null,
   "metadata": {{}},
   "outputs": [],
   "source": ["{code}"]
  }}
 ]
}}
"""

_TUTORIAL_NOTEBOOKS = [
    (
        "01_estimate_cost.ipynb",
        "Cost Estimation",
        "import burnt\\ne = burnt.estimate('SELECT * FROM orders o JOIN customers c ON o.id = c.id')\\nprint(e)",
    ),
    (
        "02_simulate_scenarios.ipynb",
        "Simulation Scenarios",
        "import burnt\\ne = burnt.estimate('SELECT COUNT(*) FROM events')\\nr = e.simulate().scenario('Photon').cluster().enable_photon().scenario('Serverless').cluster().to_serverless().compare()\\nprint(r.comparison_table())",
    ),
    (
        "03_advise.ipynb",
        "Cluster Advisor",
        "import burnt\\nreport = burnt.advise(run_id='your-run-id')\\nreport.display()",
    ),
    (
        "04_check_antipatterns.ipynb",
        "Anti-Pattern Check",
        "# Run from terminal:\\n# burnt check src/ --output table",
    ),
]


def _run_tutorial() -> None:
    examples_dir = Path.cwd() / "examples"
    examples_dir.mkdir(exist_ok=True)

    for filename, title, code in _TUTORIAL_NOTEBOOKS:
        notebook_path = examples_dir / filename
        content = _NOTEBOOK_TEMPLATE.format(title=title, code=code)
        notebook_path.write_text(content)
        console.print(f"[green]✓[/green] Created {notebook_path}")

    console.print(f"\n[bold]Examples written to {examples_dir}/[/bold]")


@app.command()
def tutorial() -> None:
    """Generate example notebooks in examples/."""
    _run_tutorial()


# ---------------------------------------------------------------------------
# burnt cache show / cache clear
# ---------------------------------------------------------------------------

_CACHE_DIR = Path(".burnt") / "cache"


@cache_app.command("show")
def cache_show() -> None:
    """List cached files and their sizes."""
    cache_dir = Path.cwd() / _CACHE_DIR
    if not cache_dir.exists() or not any(cache_dir.iterdir()):
        console.print("[dim]Cache is empty.[/dim]")
        return

    table = Table(title=f"Cache ({cache_dir})")
    table.add_column("File")
    table.add_column("Size", justify="right")

    total = 0
    for f in sorted(cache_dir.iterdir()):
        if f.is_file():
            size = f.stat().st_size
            total += size
            table.add_row(f.name, _human_bytes(size))

    console.print(table)
    console.print(f"[dim]Total: {_human_bytes(total)}[/dim]")


@cache_app.command("clear")
def cache_clear(
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
) -> None:
    """Remove all cached files."""
    cache_dir = Path.cwd() / _CACHE_DIR
    if not cache_dir.exists() or not any(cache_dir.iterdir()):
        console.print("[dim]Cache is already empty.[/dim]")
        return

    files = [f for f in cache_dir.iterdir() if f.is_file()]
    if not yes and not typer.confirm(
        f"Remove {len(files)} cached file(s) from {cache_dir}?"
    ):
        raise typer.Exit(0)

    for f in files:
        f.unlink()

    console.print(f"[green]✓[/green] Cleared {len(files)} file(s) from cache.")


def _human_bytes(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


# ---------------------------------------------------------------------------
# burnt rules (TUI)
# ---------------------------------------------------------------------------


@app.command()
def rules() -> None:
    """Interactive TUI to toggle lint rules in the active config."""
    from rich.prompt import Prompt

    config_path, settings = Settings.discover()

    if config_path is None:
        console.print(
            '[red]Error:[/red] No config found. Run [bold]burnt init[/bold] first.'
        )
        raise typer.Exit(1)

    console.print(f"[dim]Active config: {config_path}[/dim]\n")

    # Show rules with their current state
    ignored = set(settings.lint.ignore)
    rule_ids = list(_RULE_SEVERITIES.keys())

    table = Table(title="Lint Rules")
    table.add_column("#", style="dim")
    table.add_column("Rule ID")
    table.add_column("Default Severity")
    table.add_column("Status")

    for i, rule_id in enumerate(rule_ids, 1):
        status = "[red]disabled[/red]" if rule_id in ignored else "[green]enabled[/green]"
        table.add_row(str(i), rule_id, _RULE_SEVERITIES[rule_id], status)

    console.print(table)
    console.print(
        "\nEnter rule number(s) to toggle (space-separated), or [bold]q[/bold] to quit:"
    )

    while True:
        raw = Prompt.ask(">", default="q")
        if raw.strip().lower() == "q":
            break

        changed = False
        for token in raw.split():
            try:
                idx = int(token) - 1
                if 0 <= idx < len(rule_ids):
                    rule_id = rule_ids[idx]
                    if rule_id in ignored:
                        ignored.discard(rule_id)
                        console.print(f"  [green]Enabled[/green] {rule_id}")
                    else:
                        ignored.add(rule_id)
                        console.print(f"  [red]Disabled[/red] {rule_id}")
                    changed = True
                else:
                    console.print(f"  [yellow]Invalid number: {token}[/yellow]")
            except ValueError:
                console.print(f"  [yellow]Not a number: {token}[/yellow]")

        if changed:
            _write_ignore_list(config_path, sorted(ignored))
            console.print(f"[dim]Saved to {config_path}[/dim]")

    console.print("Done.")


def _write_ignore_list(config_path: Path, ignore: list[str]) -> None:
    """Persist the ignore list to the active config file."""

    raw_text = config_path.read_text()

    # Determine section path
    if config_path.name == "pyproject.toml":
        section_key = "[tool.burnt.lint]"
        ignore_key = "ignore"
    else:
        section_key = "[lint]"
        ignore_key = "ignore"

    ignore_value = "[" + ", ".join(f'"{r}"' for r in ignore) + "]"

    lines = raw_text.splitlines(keepends=True)
    in_section = False
    found_key = False
    new_lines: list[str] = []

    for line in lines:
        stripped = line.strip()
        if stripped == section_key:
            in_section = True
        elif stripped.startswith("[") and stripped != section_key:
            in_section = False

        if in_section and stripped.startswith(f"{ignore_key} ="):
            new_lines.append(f"{ignore_key} = {ignore_value}\n")
            found_key = True
        else:
            new_lines.append(line)

    if not found_key:
        # Append ignore line into section
        result = []
        in_section = False
        for line in new_lines:
            result.append(line)
            if line.strip() == section_key:
                in_section = True
        if in_section:
            result.append(f"{ignore_key} = {ignore_value}\n")
        new_lines = result

    config_path.write_text("".join(new_lines))


# ---------------------------------------------------------------------------
# burnt doctor (stub — implemented in s2-08)
# ---------------------------------------------------------------------------


@app.command()
def doctor() -> None:
    """Diagnose burnt setup and Databricks connectivity. (Implemented in s2-08)"""
    raise NotImplementedError("doctor command will be implemented in s2-08")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    app()
