# Task: Display Mixin — Rich/HTML Rendering for All Result Types

---

## Metadata

```yaml
id: s2-06-display-mixin
status: todo
phase: 2
priority: high
agent: ~
blocked_by: [s2-05a-cli-api-redesign]
created_by: planner
```

---

## Context

### Goal

Extract the display logic from `AdvisoryReport` into a shared `_DisplayMixin` base class,
then apply it to `CostEstimate`, `SimulationResult`, `MultiSimulationResult`, and
`ClusterRecommendation`. All result types should render correctly in both terminal (rich)
and Databricks notebook (IPython HTML) environments without the user needing to know
which context they're in.

### Files to read

```
# Required
src/burnt/advisor/report.py          ← display() and _is_databricks_notebook() already exist here
src/burnt/core/models.py             ← CostEstimate, SimulationResult, MultiSimulationResult, ClusterRecommendation
src/burnt/estimators/simulation.py   ← SimulationResult, MultiSimulationResult (post s2-05a)

# Reference
tasks/s2-05a-cli-api-redesign.md
```

### Background

`AdvisoryReport` already has:
- `display()` — detects environment, delegates to rich Console or IPython HTML
- `_is_databricks_notebook()` — checks `DATABRICKS_RUNTIME_VERSION` + DBUtils
- `_to_html_table()` — generates HTML table for notebooks
- `comparison_table()` — ASCII table for terminal

This logic should live in a shared mixin so it can be applied to other types without
duplicating the environment detection code.

The mixin must handle `ImportError` gracefully: if `rich` is not installed, fall back to
`print()` + `comparison_table()`. If `IPython` is not installed in a Databricks env,
fall back to print.

---

## Part 1: `_DisplayMixin` base class

Location: `src/burnt/core/_display.py` (new file)

```python
class _DisplayMixin:
    """Shared display logic for burnt result types.

    Subclasses must implement:
      - comparison_table() -> str    # ASCII/rich table
      - _to_html_table() -> str      # HTML for notebooks
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

    def _render_rich(self):
        """Return rich-renderable object. Default: comparison_table() as string."""
        return self.comparison_table()

    def _is_databricks_notebook(self) -> bool:
        """Return True if running inside a Databricks notebook."""
        import os
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

    def comparison_table(self) -> str:
        raise NotImplementedError

    def _to_html_table(self) -> str:
        raise NotImplementedError
```

---

## Part 2: Apply to each type

### `CostEstimate`

Implement `comparison_table()` and `_to_html_table()`. The table should show:

| Field | Value |
|-------|-------|
| Estimated DBU | 100.0 |
| Estimated Cost | $5.50 |
| Confidence | high |
| Breakdown | key: value per entry |
| Warnings | each on its own row |

### `SimulationResult`

Already has `summary()` and `comparison_table()`. Add `_to_html_table()`. The table
compares original vs projected:

| | Original | Projected | Δ |
|---|---|---|---|
| Cost (USD) | $5.50 | $3.85 | -30.0% |
| DBU | 100.0 | 70.0 | -30.0% |

Below the table: list each `SimulationModification` (name, multiplier, rationale).

### `MultiSimulationResult`

Implements `comparison_table()` already (per plan). Add `_to_html_table()`. The table
shows one row per named scenario:

| Scenario | Cost (USD) | vs Baseline | Modifications |
|----------|-----------|-------------|---------------|
| Baseline | $5.50 | — | — |
| Photon | $3.85 | -30.0% | enable_photon |
| Serverless | $4.40 | -20.0% | to_serverless |

Add `best() -> tuple[str, SimulationResult]` — returns the scenario with the lowest
`projected.estimated_cost_usd`.

### `ClusterRecommendation`

Add `display()` method using `_DisplayMixin`. The table shows three tiers side by side:

| Tier | Instance | Workers | DBU/hr | Est. Monthly |
|------|----------|---------|--------|-------------|
| Economy | Standard_D4s_v3 | 2 | 0.75 | $120 |
| Balanced | Standard_D8s_v3 | 4 | 0.75 | $240 |
| Performance | Standard_D16s_v3 | 8 | 0.75 | $480 |

`to_api_json()` is a shortcut for `self.balanced.to_api_json()`.

### `AdvisoryReport`

- Extract `_is_databricks_notebook()` from `AdvisoryReport` — remove duplicated method,
  inherit from `_DisplayMixin` instead.
- Keep existing `display()`, `comparison_table()`, `_to_html_table()` behavior unchanged
  (just refactor to use mixin).

---

## Acceptance Criteria

- [ ] `_DisplayMixin` exists in `src/burnt/core/_display.py`
- [ ] `CostEstimate` inherits from `_DisplayMixin` and has working `display()`
- [ ] `SimulationResult` inherits from `_DisplayMixin` and has working `display()`
- [ ] `MultiSimulationResult` inherits from `_DisplayMixin` and has working `display()`
- [ ] `MultiSimulationResult.best()` returns the lowest-cost named scenario
- [ ] `ClusterRecommendation` inherits from `_DisplayMixin` and has working `display()`
- [ ] `ClusterRecommendation.to_api_json()` returns `self.balanced.to_api_json()`
- [ ] `AdvisoryReport` uses `_DisplayMixin` (no duplicated `_is_databricks_notebook()`)
- [ ] All `display()` calls fall back to `print()` when neither `rich` nor `IPython` available
- [ ] No `ImportError` raised in any environment

---

## Verification

```bash
uv run pytest -m unit -v
uv run ruff check src/ tests/

# Terminal display smoke test
python -c "
import burnt
e = burnt.estimate('SELECT customer_id, SUM(amount) FROM orders GROUP BY 1')
e.display()
r = e.simulate().cluster().enable_photon().compare()
r.display()
mr = (
    e.simulate()
    .scenario('Photon').cluster().enable_photon()
    .scenario('Serverless').cluster().to_serverless()
    .compare()
)
mr.display()
print('best:', mr.best()[0])
"
```

### Integration Check

- [ ] `e.display()` prints a formatted table without raising errors
- [ ] `result.display()` prints comparison table with modification details
- [ ] `multi_result.display()` prints scenario comparison table
- [ ] `multi_result.best()` returns the correct scenario name

---

## Handoff

### Result

```yaml
status: todo
```

### Blocked reason

Blocked on s2-05a (requires renamed classes: `SimulationResult`, `MultiSimulationResult`).
