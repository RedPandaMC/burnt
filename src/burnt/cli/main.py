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
    """Build the rule-id → severity dict from the Rust engine."""
    try:
        from burnt._engine import list_rules

        rules = list_rules()
        return {r.code: str(r.severity) for r in rules}
    except ImportError:
        return {}


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
    select: list[str] = typer.Option(  # noqa: B008
        [],
        "--select",
        help="Enable rules: exact ID (BP008), prefix (BP), tag (performance), or ALL",
    ),
    ignore: list[str] = typer.Option(  # noqa: B008
        [],
        "--ignore",
        help="Disable rules: exact ID, prefix, or tag (repeatable)",
    ),
    extend_select: list[str] = typer.Option(  # noqa: B008
        [], "--extend-select", help="Add rules on top of config select"
    ),
    extend_ignore: list[str] = typer.Option(  # noqa: B008
        [], "--extend-ignore", help="Add rules to ignore on top of config ignore"
    ),
) -> None:
    """Check SQL/PySpark files for cost anti-patterns."""
    from ..core.rule_filter import RuleIndex
    from ..core.suppression import apply_suppressions, parse_suppressions
    from ..parsers.antipatterns import detect_antipatterns

    _config_path, settings = Settings.discover()

    try:
        index = RuleIndex.build()
    except ImportError:
        index = None

    target = Path(path)
    if not target.exists():
        _check_inline_sql(path, console)
        raise typer.Exit(0)

    files_to_check: list[Path] = []
    if target.is_file():
        files_to_check.append(target)
    else:
        for ext in ("*.sql", "*.py"):
            for f in sorted(target.rglob(ext)):
                if not _is_excluded(f, settings.lint.exclude, target):
                    files_to_check.append(f)

    if not files_to_check:
        console.print("[yellow]No .sql or .py files found to check.[/yellow]")
        raise typer.Exit(0)

    severity_levels = {"info": 1, "warning": 2, "error": 3}
    fail_threshold = severity_levels.get(fail_on.lower(), 3)

    # Resolve active rule set: config + CLI overrides
    effective_select = settings.lint.select
    effective_extend_select = settings.lint.extend_select + list(extend_select)
    effective_ignore = settings.lint.ignore
    effective_extend_ignore = settings.lint.extend_ignore + list(ignore) + list(extend_ignore)

    if index is not None:
        # CLI --select overrides config select when provided
        if select:
            effective_select = list(select)
            effective_extend_select = list(extend_select)
        active_rules = index.resolve_active(
            effective_select,
            effective_extend_select,
            effective_ignore,
            effective_extend_ignore,
        )
    else:
        # Fallback: exact-ID matching only
        if settings.lint.select == ["ALL"]:
            active_rules = frozenset(_RULE_SEVERITIES.keys())
        else:
            active_rules = frozenset(settings.lint.select)
        active_rules -= frozenset(effective_extend_ignore) | frozenset(effective_ignore)

    all_issues: list[tuple[Path, AntiPattern]] = []
    fail_build = False

    for file_path in files_to_check:
        source = file_path.read_text(encoding="utf-8")
        lang = "pyspark" if file_path.suffix == ".py" else "sql"

        # Per-file ignores from config
        file_active = set(active_rules)
        if index is not None:
            for glob_pattern, patterns in settings.lint.per_file_ignores.items():
                if fnmatch.fnmatch(str(file_path), glob_pattern) or fnmatch.fnmatch(
                    file_path.name, glob_pattern
                ):
                    for p in patterns:
                        file_active -= index.resolve_pattern(p)
        else:
            for glob_pattern, rule_ids in settings.lint.per_file_ignores.items():
                if fnmatch.fnmatch(str(file_path), glob_pattern) or fnmatch.fnmatch(
                    file_path.name, glob_pattern
                ):
                    file_active -= set(rule_ids)

        issues = detect_antipatterns(source, lang)
        issues = [i for i in issues if i.name in file_active]

        # Apply comment-based suppressions
        if index is not None:
            file_sup, line_sup, standalone = parse_suppressions(source, index)
            issues = apply_suppressions(issues, file_sup, line_sup, standalone)

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
            color = (
                "red"
                if issue.severity == "error"
                else "yellow"
                if issue.severity == "warning"
                else "blue"
            )
            console.print(
                f"{file_path}: [{color}]{issue.severity.upper()}[/{color}] {issue.name}: {issue.description}"
            )
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
                "red"
                if issue.severity == "error"
                else "yellow"
                if issue.severity == "warning"
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


def _check_inline_sql(sql: str, console: Console) -> None:
    """Print anti-pattern warnings for an inline SQL string."""
    from ..parsers.antipatterns import detect_antipatterns

    issues = detect_antipatterns(sql, "sql")
    if issues:
        console.print("Warnings:")
        for issue in issues:
            console.print(f"  ⚠ {issue.name} — {issue.description}")

    console.print("\nConnect to a workspace for cost estimates: burnt doctor")


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
    output: str = typer.Option(
        "table", "--output", "-o", help="Output format: table|json|text"
    ),
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
            console.print(
                "[red]Error:[/red] Provide --run-id, --statement-id, --job-id, or --job-name."
            )
            console.print(
                "[dim]Hint: to analyze a notebook session, use burnt.advise() in Python.[/dim]"
            )
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
ignore = []           # exact ID (BP008), prefix (BP), or tag (performance)
extend-ignore = []
fail-on = "error"
exclude = []

# [lint.per-file-ignores]
# "migrations/*.sql" = ["BQ*"]
# "notebooks/*.py" = ["style"]

[cache]
enabled = true
ttl-seconds = 3600
"""

_PYPROJECT_BURNT_SECTION = """
[tool.burnt]

[tool.burnt.lint]
select = ["ALL"]
ignore = []           # exact ID (BP008), prefix (BP), or tag (performance)
extend-ignore = []
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
            "[red]Error:[/red] No config found. Run [bold]burnt init[/bold] first."
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
        status = (
            "[red]disabled[/red]" if rule_id in ignored else "[green]enabled[/green]"
        )
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
# burnt doctor
# ---------------------------------------------------------------------------

_SYSTEM_TABLES = [
    ("system.billing.usage", "cost attribution / anomaly detection"),
    ("system.billing.list_prices", "dollar amount calculation"),
    ("system.query.history", "historical estimation / fingerprint lookup"),
    ("system.compute.node_types", "instance catalog refresh"),
    ("system.compute.node_timeline", "idle cluster detection"),
    ("system.lakeflow.jobs", "job analysis"),
    ("system.lakeflow.job_run_timeline", "job run cost attribution"),
]


def _check_table_access(
    host: str,
    token: str,
    warehouse_id: str,
    table: str,
) -> tuple[str, str]:
    """Check SELECT access on a system table via the SQL Statement API.

    Returns (status, message) where status is one of: OK, NO ACCESS, TIMEOUT, ERROR.
    """
    import requests

    try:
        resp = requests.post(
            f"{host}/api/2.0/sql/statements",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            json={
                "statement": f"SELECT 1 FROM {table} LIMIT 1",
                "warehouse_id": warehouse_id,
                "wait_timeout": "30s",
                "disposition": "INLINE",
            },
            timeout=35,
        )
        data = resp.json()
        state = data.get("status", {}).get("state", "UNKNOWN")
        if state == "SUCCEEDED":
            return "OK", ""
        if state in ("PENDING", "RUNNING"):
            return "TIMEOUT", "query still running after 30s"
        error = data.get("status", {}).get("error", {})
        msg = error.get("message", "unknown error")
        msg_lower = msg.lower()
        if (
            "permission_denied" in msg_lower
            or "does not have privilege" in msg_lower
            or "insufficient privileges" in msg_lower
        ):
            return "NO ACCESS", msg
        return "ERROR", msg
    except requests.Timeout:
        return "TIMEOUT", "request timed out after 35s"
    except Exception as exc:
        return "ERROR", str(exc)


@app.command()
def doctor(
    warehouse_id: str | None = typer.Option(
        None,
        "--warehouse-id",
        help="SQL warehouse ID for system table permission checks",
    ),
) -> None:
    """Diagnose burnt setup and Databricks connectivity."""
    import importlib.metadata
    import os
    import sys

    import requests

    from .. import __version__

    SEP = "─" * 48

    # ── Header ───────────────────────────────────────────────────────────────
    console.print(f"burnt v{__version__} environment check")
    console.print(SEP)

    # ── Python + dependencies ─────────────────────────────────────────────────
    vi = sys.version_info
    py_ver = f"{vi.major}.{vi.minor}.{vi.micro}"
    console.print(f"  {'Python':<22} {py_ver:<14} [green]OK[/green]")

    _PACKAGES = [
        ("sqlglot", "sqlglot"),
        ("pydantic", "pydantic"),
        ("pydantic-settings", "pydantic_settings"),
        ("rich", "rich"),
    ]
    for pkg_name, import_name in _PACKAGES:
        try:
            ver = importlib.metadata.version(import_name)
            console.print(f"  {pkg_name:<22} {ver:<14} [green]OK[/green]")
        except importlib.metadata.PackageNotFoundError:
            console.print(f"  {pkg_name:<22} {'':14} [red]MISSING[/red]")

    console.print(SEP)

    # ── Credentials ───────────────────────────────────────────────────────────
    host = (
        os.environ.get("DATABRICKS_HOST")
        or os.environ.get("DATABRICKS_WORKSPACE_URL")
        or os.environ.get("BURNT_WORKSPACE_URL")
    )
    token = os.environ.get("DATABRICKS_TOKEN") or os.environ.get("BURNT_TOKEN")
    creds_ok = bool(host and token)

    if host:
        console.print(f"  {'DATABRICKS_HOST':<22} {'SET':<14} {host}")
    else:
        console.print(
            f"  {'DATABRICKS_HOST':<22} [yellow]NOT SET ⚠[/yellow]"
            "       advise() and live features unavailable"
        )

    if token:
        redacted = (token[:6] + "...") if len(token) > 6 else token
        console.print(f"  {'DATABRICKS_TOKEN':<22} {'SET':<14} {redacted}")
    else:
        console.print(
            f"  {'DATABRICKS_TOKEN':<22} [yellow]NOT SET ⚠[/yellow]"
            "       advise() and live features unavailable"
        )

    # ── Connection test ───────────────────────────────────────────────────────
    if not creds_ok:
        console.print(f"  {'Connection test':<38} SKIP  (credentials not configured)")
    else:
        try:
            resp = requests.get(
                f"{host}/api/2.0/clusters/list",
                params={"limit": 1},
                headers={"Authorization": f"Bearer {token}"},
                timeout=5,
            )
            if resp.status_code == 200:
                console.print(
                    f"  {'Connection test':<38} [green]OK[/green]  workspace reachable"
                )
            elif resp.status_code in (401, 403):
                console.print(
                    f"  {'Connection test':<38} [red]AUTH ERROR[/red]  check token"
                )
            else:
                console.print(
                    f"  {'Connection test':<38} [red]ERROR[/red]  {resp.status_code}"
                )
        except requests.Timeout:
            console.print(
                f"  {'Connection test':<38} [yellow]TIMEOUT[/yellow]  (5s)  check firewall/network"
            )
        except Exception as exc:
            console.print(f"  {'Connection test':<38} [red]ERROR[/red]  {exc}")

    # ── System table permission checks ────────────────────────────────────────
    if not creds_ok:
        for tbl, _ in _SYSTEM_TABLES:
            console.print(f"  {tbl:<38} SKIP  (credentials not configured)")
    else:
        wh_id = warehouse_id
        if not wh_id:
            try:
                wh_resp = requests.get(
                    f"{host}/api/2.1/warehouses",
                    headers={"Authorization": f"Bearer {token}"},
                    timeout=5,
                )
                if wh_resp.status_code == 200:
                    for wh in wh_resp.json().get("warehouses", []):
                        if wh.get("state") != "DELETED":
                            wh_id = wh["id"]
                            break
            except Exception:
                pass

        if not wh_id:
            for tbl, _ in _SYSTEM_TABLES:
                console.print(
                    f"  {tbl:<38} [yellow]SKIP[/yellow]  (no SQL warehouse; use --warehouse-id)"
                )
        else:
            missing_features: list[str] = []
            for tbl, feature in _SYSTEM_TABLES:
                status, msg = _check_table_access(host, token, wh_id, tbl)
                if status == "OK":
                    console.print(f"  {tbl:<38} [green]OK[/green]")
                elif status == "NO ACCESS":
                    console.print(
                        f"  {tbl:<38} [red]NO ACCESS ⚠[/red]  required for {feature}"
                    )
                    missing_features.append(feature)
                else:
                    console.print(f"  {tbl:<38} [red]{status}[/red]  {msg}")

            if missing_features:
                console.print(
                    f"\n  [yellow]Missing permissions affect:[/yellow] {', '.join(missing_features)}"
                )
                console.print(
                    "  Contact your workspace admin to grant SELECT on system catalog tables."
                )

    console.print(SEP)

    # ── Config ────────────────────────────────────────────────────────────────
    config_path, settings = Settings.discover(cwd=Path.cwd())

    if config_path is None:
        console.print(
            f"  {'Config':<22} [yellow]NOT FOUND ⚠[/yellow]  "
            "Run 'burnt init' to create .burnt.toml"
        )
    else:
        console.print(f"  {'Config':<22} {config_path}")
        url_val = settings.workspace_url or "(not set)"
        console.print(f"    {'workspace-url':<16} {url_val}")
        console.print(f"    {'lint.fail-on':<16} {settings.lint.fail_on}")

        try:
            from burnt._engine import get_registry_count

            total_rules = get_registry_count()
        except ImportError:
            total_rules = 0

        ignored_count = len(settings.lint.ignore)
        if settings.lint.select == ["ALL"]:
            rules_str = f"ALL  ({total_rules} rules, {ignored_count} ignored)"
        else:
            selected = len(settings.lint.select)
            rules_str = f"{selected} rules selected, {ignored_count} ignored"
        console.print(f"    {'lint.select':<16} {rules_str}")
        console.print(f"    {'cache.ttl':<16} {int(settings.cache.ttl_seconds)}s")

        # Check for secondary config in the same directory
        parent = config_path.parent
        if config_path.name == ".burnt.toml":
            secondary = parent / "pyproject.toml"
            if secondary.exists() and Settings._has_tool_burnt(secondary):
                console.print(
                    f"  {'Also found':<22} {secondary} [tool.burnt]  (lower priority)"
                )
        else:
            secondary = parent / ".burnt.toml"
            if secondary.exists():
                console.print(f"  {'Also found':<22} {secondary}  (lower priority)")

    console.print(SEP)

    # ── Cache ─────────────────────────────────────────────────────────────────
    cache_dir = Path.cwd() / ".burnt" / "cache"
    if cache_dir.exists():
        files = [f for f in cache_dir.iterdir() if f.is_file()]
        total_size = sum(f.stat().st_size for f in files)
        console.print(
            f"  {'Cache':<22} {cache_dir}  {len(files)} files  {_human_bytes(total_size)}"
        )
    else:
        console.print(
            f"  {'Cache':<22} {cache_dir}  [dim]not found[/dim]  "
            "(run 'burnt check' to populate)"
        )

    console.print(SEP)
    raise typer.Exit(0)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    app()
