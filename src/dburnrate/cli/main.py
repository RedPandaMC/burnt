from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

from ..core.config import Settings
from ..core.models import VALID_SKUS, ClusterConfig
from ..core.pricing import AZURE_INSTANCE_DBU, get_dbu_rate
from ..estimators.pipeline import EstimationPipeline
from ..estimators.static import CostEstimator
from ..estimators.whatif import apply_photon_scenario, apply_serverless_migration
from ..parsers.notebooks import parse_dbc, parse_notebook

app = typer.Typer(help="dburnrate - Pre-execution cost estimation for Databricks")
console = Console()


@app.command()
def estimate(
    query: str = typer.Argument(..., help="SQL query or path to .sql/.ipynb/.dbc file"),
    cluster_type: str = typer.Option(
        "Standard_DS3_v2", "--cluster", "-c", help="Instance type"
    ),
    workers: int = typer.Option(2, "--workers", "-w", help="Number of workers"),
    photon: bool = typer.Option(False, "--photon", help="Enable Photon"),
    sku: str = typer.Option(
        "ALL_PURPOSE",
        "--sku",
        "-s",
        help=f"SKU type: {', '.join(sorted(VALID_SKUS))}",
    ),
    currency: str = typer.Option("USD", "--currency", help="Output currency (USD/EUR)"),
    output: str = typer.Option(
        "table", "--output", "-o", help="Output format: table, json, text"
    ),
    warehouse_id: str = typer.Option(
        None, "--warehouse-id", help="SQL warehouse ID for EXPLAIN COST"
    ),
    workspace_url: str = typer.Option(
        None,
        "--workspace-url",
        help="Databricks workspace URL (overrides DBURNRATE_WORKSPACE_URL)",
    ),
):
    query_path = Path(query)
    if query_path.exists():
        if query_path.suffix == ".sql":
            query = query_path.read_text()
        elif query_path.suffix == ".ipynb":
            cells = parse_notebook(query_path)
            query = "\n\n".join(c.source for c in cells if c.language == "sql")
        elif query_path.suffix == ".dbc":
            cells = parse_dbc(query_path)
            query = "\n\n".join(c.source for c in cells if c.language == "sql")

    dbu_rate = AZURE_INSTANCE_DBU.get(cluster_type, 0.75)
    cluster = ClusterConfig(
        instance_type=cluster_type,
        num_workers=workers,
        dbu_per_hour=dbu_rate,
        photon_enabled=photon,
        sku=sku,
    )

    settings = Settings()
    if workspace_url:
        settings.workspace_url = workspace_url

    pipeline = EstimationPipeline(
        backend=None,
        warehouse_id=warehouse_id,
    )

    if warehouse_id and settings.workspace_url and settings.token:
        try:
            from ..tables.connection import DatabricksClient

            pipeline = EstimationPipeline(
                backend=DatabricksClient(settings),
                warehouse_id=warehouse_id,
            )
        except Exception as e:
            console.print(
                f"[yellow]Warning: Failed to connect to Databricks ({e}). Using offline estimation.[/yellow]"
            )
            pipeline = EstimationPipeline(backend=None, warehouse_id=None)

    try:
        result = pipeline.estimate(query, cluster)
    except ImportError as e:
        if "sqlglot" in str(e):
            console.print(
                "[red]Error: sqlglot is required for SQL parsing.[/red]\n"
                "Install with: [yellow]uv sync --extra sql[/yellow]\n"
                "or: [yellow]pip install dburnrate[sql][/yellow]"
            )
            raise typer.Exit(1) from e
        raise

    signal = "static"
    for warning in result.warnings:
        if warning.startswith("Signal:"):
            signal = warning.replace("Signal:", "").strip()
            break

    if currency != "USD" and result.estimated_cost_usd:
        estimator = CostEstimator(cluster=cluster, target_currency=currency)
        result = estimator.estimate(query)

    dbu_rate_decimal = get_dbu_rate(cluster.sku)
    estimated_cost_usd = round(float(result.estimated_dbu) * float(dbu_rate_decimal), 4)

    if output == "json":
        import json

        console.print(json.dumps(result.model_dump(), indent=2))
    elif output == "text":
        console.print(f"Estimated DBU: {result.estimated_dbu}")
        console.print(f"Estimated Cost ({currency}): ${estimated_cost_usd}")
        console.print(f"Confidence: {result.confidence}")
        console.print(f"Signal: {signal}")
    else:
        table = Table(title="Cost Estimate")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="green")
        table.add_row("Estimated DBU", str(result.estimated_dbu))
        table.add_row(f"Estimated Cost ({currency})", f"${estimated_cost_usd}")
        table.add_row("Confidence", result.confidence)
        table.add_row("Signal", signal)
        console.print(table)


@app.command()
def whatif(
    query: str = typer.Argument(..., help="SQL query to model scenarios for"),
    scenario: str = typer.Option(
        ..., "--scenario", "-s", help="Scenario: photon, serverless, resize"
    ),
    cluster_type: str = typer.Option("Standard_DS3_v2", "--cluster", "-c"),
    workers: int = typer.Option(2, "--workers", "-w"),
    utilization: float = typer.Option(
        50.0, "--utilization", help="Cluster utilization %"
    ),
):
    cluster = ClusterConfig(
        instance_type=cluster_type,
        num_workers=workers,
        dbu_per_hour=AZURE_INSTANCE_DBU.get(cluster_type, 0.75),
    )

    estimator = CostEstimator(cluster=cluster)
    base = estimator.estimate(query)

    if scenario == "photon":
        result = apply_photon_scenario(base, "complex_join")
    elif scenario == "serverless":
        result = apply_serverless_migration(base, "ALL_PURPOSE", utilization)
    else:
        console.print(f"[red]Unknown scenario: {scenario}[/red]")
        raise typer.Exit(1)

    console.print(f"[bold]Base Cost:[/bold] ${base.estimated_cost_usd:.4f}")
    console.print(f"[bold]With {scenario}:[/bold] ${result.estimated_cost_usd:.4f}")

    if result.warnings:
        for w in result.warnings:
            console.print(f"[yellow]Warning:[/yellow] {w}")


@app.command()
def lint(
    path: str = typer.Argument(..., help="Path to file or directory to lint"),
    fail_on: str = typer.Option(
        "error", "--fail-on", help="Exit code 1 on severity (info, warning, error)"
    ),
):
    """Lint SQL and PySpark code for cost anti-patterns."""
    from pathlib import Path

    from dburnrate import lint_file

    target = Path(path)
    if not target.exists():
        console.print(f"[red]Error:[/red] Path not found: {path}")
        raise typer.Exit(1)

    files_to_lint = []
    if target.is_file():
        files_to_lint.append(target)
    else:
        files_to_lint.extend(target.rglob("*.sql"))
        files_to_lint.extend(target.rglob("*.py"))

    if not files_to_lint:
        console.print("[yellow]No .sql or .py files found to lint.[/yellow]")
        raise typer.Exit(0)

    severity_levels = {"info": 1, "warning": 2, "error": 3}
    fail_threshold = severity_levels.get(fail_on.lower(), 3)

    found_issues = False
    fail_build = False

    for file_path in files_to_lint:
        issues = lint_file(file_path)
        if issues:
            found_issues = True
            console.print(f"\n[bold]{file_path}[/bold]")
            for issue in issues:
                color = (
                    "red"
                    if issue.severity == "error"
                    else "yellow"
                    if issue.severity == "warning"
                    else "blue"
                )
                console.print(
                    f"  [{color}]{issue.severity.upper()}[/{color}] {issue.name}: {issue.description}"
                )
                console.print(f"  [dim]Suggestion: {issue.suggestion}[/dim]")

                if severity_levels.get(str(issue.severity), 0) >= fail_threshold:
                    fail_build = True

    if not found_issues:
        console.print("[green]No cost anti-patterns found![/green]")

    if fail_build:
        raise typer.Exit(1)


@app.command()
def advise(
    run_id: str = typer.Option(
        ..., "--run-id", help="Databricks Job Run ID or Statement ID to analyze"
    ),
):
    """Analyze a recent interactive test run and recommend an optimized Jobs Cluster configuration."""
    console.print(
        f"[bold blue]Analyzing execution metrics for run: {run_id}[/bold blue]"
    )
    console.print(
        "[yellow]Note: The Interactive Advisor is currently in active development.[/yellow]"
    )
    console.print(
        "This command will soon connect to system.lakeflow.job_run_timeline to fetch actual memory/CPU metrics."
    )
    raise typer.Exit(0)


@app.command()
def version():
    """Print version info."""
    from .. import __version__

    console.print(f"dburnrate v{__version__}")


if __name__ == "__main__":
    app()
