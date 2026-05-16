"""burnt CLI — static analysis tooling. Zero credentials required."""

from __future__ import annotations

import fnmatch
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

from .._check import run as check_run
from ..core.config import Settings

app = typer.Typer(
    help="burnt - Static cost analyzer for Spark",
    no_args_is_help=True,
)
cache_app = typer.Typer(help="Manage the burnt local cache")
app.add_typer(cache_app, name="cache")
pricing_app = typer.Typer(help="Pricing backend management")
app.add_typer(pricing_app, name="pricing")

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
        "table", "--output", "-o", help="Output format: table|text|json|sarif"
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
    backend: str | None = typer.Option(
        None,
        "--backend",
        help="Pricing backend override: azure-databricks, aws-databricks, gcp-databricks, onprem-spark",
    ),
    currency: str = typer.Option(
        "USD", "--currency", help="Output currency code (USD, EUR, GBP, ...)"
    ),
) -> None:
    """Check SQL/PySpark files for cost anti-patterns."""
    import json as json_mod

    from burnt import _SESSION
    from burnt._check import CheckResult
    from burnt.display.export import report_to_sarif  # type: ignore[import]

    from ..core.rule_filter import RuleIndex
    from ..core.suppression import apply_suppressions, parse_suppressions
    from ..display import to_table_multi

    try:
        _config_path, settings = Settings.discover()
    except Exception as exc:
        console.print(f"[red]Config error: {exc}[/red]")
        raise typer.Exit(2) from exc

    try:
        index = RuleIndex.build()
    except ImportError:
        index = None

    target = Path(path)
    if not target.exists():
        # Treat as inline source
        result = check_run(path=path, severity=fail_on, session=_SESSION)
        _render_inline(result, output, console)
        raise typer.Exit(0 if not result.findings else 1)

    files_to_check: list[Path] = []
    if target.is_file():
        files_to_check.append(target)
    else:
        for ext in ("*.sql", "*.py"):
            files_to_check.extend(
                f
                for f in sorted(target.rglob(ext))
                if not _is_excluded(f, settings.lint.exclude, target)
            )

    if not files_to_check:
        console.print("[yellow]No .sql or .py files found to check.[/yellow]")
        raise typer.Exit(0)

    # Resolve active rule set: config + CLI overrides
    effective_select = settings.lint.select
    effective_extend_select = settings.lint.extend_select + list(extend_select)
    effective_ignore = settings.lint.ignore
    effective_extend_ignore = (
        settings.lint.extend_ignore + list(ignore) + list(extend_ignore)
    )

    if index is not None:
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
        if settings.lint.select == ["ALL"]:
            active_rules = frozenset(_RULE_SEVERITIES.keys())
        else:
            active_rules = frozenset(settings.lint.select)
        active_rules -= frozenset(effective_extend_ignore) | frozenset(effective_ignore)

    results: list = []
    fail_build = False

    for file_path in files_to_check:
        source = file_path.read_text(encoding="utf-8")

        # Per-file ignores → skip list
        file_skip: set[str] = set()
        if index is not None:
            for glob_pattern, patterns in settings.lint.per_file_ignores.items():
                if fnmatch.fnmatch(str(file_path), glob_pattern) or fnmatch.fnmatch(
                    file_path.name, glob_pattern
                ):
                    for p in patterns:
                        file_skip |= index.resolve_pattern(p)
        else:
            for glob_pattern, rule_ids in settings.lint.per_file_ignores.items():
                if fnmatch.fnmatch(str(file_path), glob_pattern) or fnmatch.fnmatch(
                    file_path.name, glob_pattern
                ):
                    file_skip |= set(rule_ids)

        # Build only/skip for _check.run
        only = list(active_rules) if active_rules else None
        skip = list(file_skip) if file_skip else None

        result = check_run(
            path=str(file_path),
            severity=fail_on,
            skip=skip,
            only=only,
            session=_SESSION,
        )

        # Apply comment-based suppressions
        if index is not None:
            file_sup, line_sup, standalone = parse_suppressions(source, index)
            result.findings = apply_suppressions(
                result.findings, file_sup, line_sup, standalone
            )

        # Post-filter by active rules (per-file ignores may have removed some)
        if active_rules:
            result.findings = [f for f in result.findings if f.rule_id in active_rules]

        if result.findings:
            fail_build = True

        results.append(result)

    # Apply pricing backend if configured
    effective_backend = backend or settings.pricing.backend
    if effective_backend:
        _apply_pricing(results, effective_backend, currency)

    if not fail_build:
        console.print("[green]No cost anti-patterns found.[/green]")
        raise typer.Exit(0)

    if output == "json":
        data = [r.to_json() for r in results]
        # Use plain print so soft-wrapping doesn't corrupt JSON strings.
        print(json_mod.dumps(data, indent=2))
    elif output == "sarif":
        flat_result = CheckResult(
            file_path=None,
            mode="aggregate",
            findings=[f for r in results for f in r.findings],
        )
        sarif = report_to_sarif(flat_result)
        print(json_mod.dumps(sarif, indent=2))
    elif output == "text":
        for result in results:
            for f in result.findings:
                color = (
                    "red"
                    if f.severity == "error"
                    else "yellow"
                    if f.severity == "warning"
                    else "blue"
                )
                fp = getattr(result, "file_path", "unknown")
                console.print(
                    f"{fp}: [{color}]{f.severity.upper()}[/{color}] {f.rule_id}: {f.message}"
                )
                if f.suggestion:
                    console.print(f"  [dim]Suggestion: {f.suggestion}[/dim]")
    else:
        to_table_multi(results)

    raise typer.Exit(1)


def _render_inline(result, output: str, console: Console) -> None:
    """Render a single-file CheckResult for inline source."""
    import json as json_mod

    from ..display import to_table

    if output == "json":
        console.print(json_mod.dumps(result.to_json(), indent=2))
    elif output == "text":
        for f in result.findings:
            color = (
                "red"
                if f.severity == "error"
                else "yellow"
                if f.severity == "warning"
                else "blue"
            )
            console.print(
                f"[{color}]{f.severity.upper()}[/{color}] {f.rule_id}: {f.message}"
            )
    else:
        to_table(result)


def _is_excluded(file_path: Path, exclude_patterns: list[str], root: Path) -> bool:
    rel = str(file_path.relative_to(root))
    for pattern in exclude_patterns:
        if fnmatch.fnmatch(rel, pattern) or fnmatch.fnmatch(str(file_path), pattern):
            return True
    return False


def _apply_pricing(results: list, backend_name: str, currency: str) -> None:
    """Apply a pricing backend to all check results that have compute_seconds."""
    from burnt.providers import get_backend

    provider = get_backend(backend_name)
    if provider is None:
        console.print(
            f"[yellow]Pricing backend '{backend_name}' not available.[/yellow] "
            "Install the matching extra: pip install burnt[onprem-spark]"
        )
        return

    for result in results:
        if result.compute_seconds is None or result.compute_seconds <= 0:
            continue
        try:
            cost = provider.estimate(
                result.compute_seconds,
                currency=currency,
            )
            result.cost_estimate = cost
        except Exception as e:
            console.print(f"[dim]Pricing error: {e}[/dim]")


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

                with target.open("rb") as f:
                    data = tomllib.load(f)
                if data.get("tool", {}).get("burnt") and not typer.confirm(
                    "[tool.burnt] already exists. Overwrite?", default=False
                ):
                    console.print("[yellow]Skipped config.[/yellow]")
                    target = None
            except Exception:
                pass

            if target:
                with target.open("a") as f:
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
            with gitignore.open("a") as f:
                f.write(f"\n# burnt cache\n{cache_entry}\n")
            console.print(f"[green]✓[/green] Added {cache_entry} to .gitignore")
    else:
        gitignore.write_text(f"# burnt cache\n{cache_entry}\n")
        console.print(f"[green]✓[/green] Created .gitignore with {cache_entry}")


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
            except ValueError:  # noqa: PERF203
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
        ("pydantic", "pydantic"),
        ("pydantic-settings", "pydantic_settings"),
        ("rich", "rich"),
    ]
    for pkg_name, import_name in _PACKAGES:
        try:
            ver = importlib.metadata.version(import_name)
            console.print(f"  {pkg_name:<22} {ver:<14} [green]OK[/green]")
        except importlib.metadata.PackageNotFoundError:  # noqa: PERF203
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
            "       live Databricks features unavailable"
        )

    if token:
        redacted = (token[:6] + "...") if len(token) > 6 else token
        console.print(f"  {'DATABRICKS_TOKEN':<22} {'SET':<14} {redacted}")
    else:
        console.print(
            f"  {'DATABRICKS_TOKEN':<22} [yellow]NOT SET ⚠[/yellow]"
            "       live Databricks features unavailable"
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
        console.print(f"    {'lint.fail-on':<16} {settings.lint.fail_on.value}")

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
# burnt pricing *
# ---------------------------------------------------------------------------


@pricing_app.command("list-backends")
def pricing_list_backends() -> None:
    """List all available pricing backends."""
    from burnt.providers import list_backends

    backends = list_backends()
    if not backends:
        console.print("[yellow]No pricing backends installed.[/yellow]")
        console.print(
            "  Install one: pip install burnt[azure-databricks]  "
            "| burnt[aws-databricks] | burnt[gcp-databricks] | burnt[onprem-spark]"
        )
        raise typer.Exit(0)

    table = Table(title="Available Pricing Backends")
    table.add_column("Name", style="cyan")
    table.add_column("Status")
    for name in backends:
        from burnt.providers import get_backend

        p = get_backend(name)
        status = (
            "[green]available[/green]"
            if (p and p.is_available())
            else "[dim]unavailable[/dim]"
        )
        table.add_row(name, status)
    console.print(table)


@pricing_app.command("refresh")
def pricing_refresh(
    backend: str | None = typer.Option(
        None, "--backend", help="Refresh only this backend (default: all)"
    ),
) -> None:
    """Force-refresh pricing data from APIs."""
    from burnt.providers import get_backend, list_backends

    to_refresh = [backend] if backend else list_backends()
    for name in to_refresh:
        p = get_backend(name)
        if p is None:
            console.print(f"[red]Backend '{name}' not found[/red]")
            continue
        try:
            p.refresh_cache()
            console.print(f"[green]Refreshed[/green] {name}")
        except Exception as e:
            console.print(f"[red]Failed[/red] {name}: {e}")


@pricing_app.command("list-instances")
def pricing_list_instances(
    backend: str = typer.Option(
        "azure-databricks", "--backend", help="Backend to list instances for"
    ),
    category: str | None = typer.Option(None, "--category", help="Filter by category"),
    limit: int = typer.Option(20, "--limit", help="Max results"),
) -> None:
    """List cached instance types and their DBU rates."""
    from burnt.providers import get_backend

    p = get_backend(backend)
    if p is None:
        console.print(f"[red]Backend '{backend}' not found[/red]")
        raise typer.Exit(1)

    if not p.is_available():
        console.print(f"[yellow]Backend '{backend}' is not available.[/yellow]")
        raise typer.Exit(1)

    try:
        catalog = _get_instance_catalog(p)
    except Exception as e:
        console.print(f"[red]Error loading catalog: {e}[/red]")
        raise typer.Exit(1) from e

    if category:
        catalog = [c for c in catalog if c.category == category]

    table = Table(title=f"Instance Catalog — {backend}")
    table.add_column("Instance Type", style="cyan")
    table.add_column("vCPUs")
    table.add_column("Memory GB")
    table.add_column("DBU/hr")
    table.add_column("VM$/hr")
    table.add_column("Category")

    for spec in catalog[:limit]:
        table.add_row(
            spec.instance_type,
            str(spec.vcpus),
            f"{spec.memory_gb:.0f}",
            f"{spec.dbu_rate:.2f}",
            f"${spec.vm_cost_per_hour:.3f}",
            spec.category,
        )

    console.print(table)
    console.print(f"[dim]{len(catalog)} total instance types[/dim]")


def _get_instance_catalog(p) -> list:
    """Extract instance catalog from a provider backend."""
    from burnt.providers.aws_databricks import load_catalog as load_aws
    from burnt.providers.azure_databricks import load_catalog as load_azure
    from burnt.providers.gcp_databricks import load_catalog as load_gcp

    name = p.name
    if name == "azure-databricks":
        return list(load_azure().values())
    if name == "aws-databricks":
        return list(load_aws().values())
    if name == "gcp-databricks":
        return list(load_gcp().values())
    return []


@pricing_app.command("estimate")
def pricing_estimate(
    compute_seconds: float = typer.Argument(..., help="Compute seconds"),
    instance_type: str = typer.Option("Standard_DS3_v2", "--instance-type", "-i"),
    num_workers: int = typer.Option(2, "--workers", "-w"),
    backend: str = typer.Option(
        "azure-databricks", "--backend", "-b", help="Backend to use"
    ),
    currency: str = typer.Option("USD", "--currency", "-c"),
    region: str | None = typer.Option(None, "--region"),
) -> None:
    """Estimate cost for a given compute workload."""
    from burnt.providers import get_backend

    p = get_backend(backend)
    if p is None:
        console.print(f"[red]Backend '{backend}' not found[/red]")
        raise typer.Exit(1)

    try:
        estimate = p.estimate(
            compute_seconds,
            instance_type=instance_type,
            num_workers=num_workers,
            region=region,
            currency=currency,
        )
    except Exception as e:
        console.print(f"[red]Estimation failed: {e}[/red]")
        raise typer.Exit(1) from e

    console.print(f"\n[bold]Cost Estimate — {backend}[/bold]")
    console.print(f"  Compute seconds:  {compute_seconds:,.1f}s")
    console.print(f"  Instance type:   {instance_type} x {num_workers} workers")
    target_cost = estimate.cost_in(currency) or estimate.cost_in("USD")
    if target_cost is not None:
        console.print(f"  Estimated cost:  [green]{currency} {target_cost:.4f}[/green]")
        if currency != "USD" and estimate.cost_in("USD") is not None:
            console.print(
                f"                   [dim](USD {estimate.cost_in('USD'):.4f})[/dim]"
            )
        if estimate.breakdown:
            console.print("  Breakdown:")
            for k, v in estimate.breakdown.items():
                console.print(f"    {k}: {v}")
    else:
        console.print("  Estimated cost:  [dim]unavailable[/dim]")
    if estimate.warnings:
        for w in estimate.warnings:
            console.print(f"  [yellow]Warning: {w}[/yellow]")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    app()
