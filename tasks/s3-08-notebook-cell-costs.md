# Task: Notebook Cell-Level Cost Breakdown

---

## Metadata

```yaml
id: s3-08-notebook-cell-costs
status: todo
phase: 3
priority: medium
agent: ~
blocked_by: [s3-01-delta-scan-integration]
created_by: planner
```

---

## Context

### Goal

Extend notebook cost estimation to break down costs per cell, identifying which cell is
the expensive one (the "hotspot"). Support all Databricks notebook formats: `.ipynb`,
`.dbc`, and the source-format `.py`/`.sql`/`.scala` files produced by Databricks Repos
and Git integration. For PySpark notebooks, attribute cost to the cell where an action
fires (`.write`, `.count`, `.collect`), not to the transformation cells.

### Files to read

```
# Required
src/burnt/parsers/notebooks.py     ← existing .ipynb and .dbc parsing
src/burnt/estimators/pipeline.py
src/burnt/core/models.py           ← CostEstimate
src/burnt/__init__.py              ← estimate() public API

# Reference
DESIGN.md
tasks/s3-01-delta-scan-integration.md
```

### Background

**Supported formats:**

| Format | Extension | Cell Delimiter | Language Detection |
|--------|-----------|---------------|--------------------|
| Source (Python) | `.py` | `# COMMAND ----------` | `# MAGIC %sql`, `# MAGIC %python` |
| Source (SQL) | `.sql` | `-- COMMAND ----------` | `-- MAGIC %python` |
| Source (Scala) | `.scala` | `// COMMAND ----------` | `// MAGIC %sql` |
| Jupyter | `.ipynb` | JSON cell array | `cell_type` + magic commands |
| DBC archive | `.dbc` | ZIP → JSON → cell array | `language` field per cell |

The current parser handles `.ipynb` and `.dbc`. This task adds source-format variants
(`.py` with `COMMAND` delimiters).

**New data models:**

```python
@dataclass
class CellEstimate:
    cell_index: int
    source_preview: str          # first 80 chars of cell source
    language: str                # "sql", "python", "scala", "markdown"
    estimate: CostEstimate | None  # None for markdown/comment-only cells
    pct_of_total: float          # 0.0–1.0
    is_action: bool              # True if PySpark action fires in this cell
    triggered_by: list[int]      # cell indices of lazy transformations this action materializes

@dataclass
class NotebookEstimate:
    notebook_path: str
    cells: list[CellEstimate]
    total: CostEstimate
    hotspot: CellEstimate        # cell with highest cost
```

**PySpark attribution model:**

Lazy transformations (`.filter()`, `.select()`, `.groupBy()`, `.join()`) produce no
Spark jobs — cost is $0 for those cells. Only actions trigger execution:
`.write.*`, `.count()`, `.collect()`, `.toPandas()`, `.display()`, `spark.sql()` with DML.

Attribution strategy: concatenate all Python cells into a single scope for AST analysis.
For each action cell, list all preceding transformation cells that contribute to it via
DataFrame lineage (simple name tracking — not full dataflow analysis).

**`estimate()` API extension:**

```python
# File path input
estimate = burnt.estimate("./notebook.py")   # returns NotebookEstimate if path ends in notebook extension
estimate.display()  # shows per-cell breakdown table

# Alternatively explicit
estimate = burnt.estimate_notebook("./notebook.py")
```

---

## Acceptance Criteria

- [ ] `src/burnt/parsers/notebooks.py` extended to parse `.py` with `# COMMAND ----------` delimiters
- [ ] Language detection from `# MAGIC %sql` and `# MAGIC %python` magic prefixes
- [ ] `NotebookEstimate` and `CellEstimate` models exist in `src/burnt/core/models.py`
- [ ] `burnt.estimate("path/to/notebook.py")` returns `NotebookEstimate` when path ends in `.py`, `.ipynb`, or `.dbc`
- [ ] Per-cell cost breakdown: each SQL cell gets its own `CostEstimate`
- [ ] PySpark action cells are marked `is_action=True`; transformation-only cells have `estimate=None` or cost=$0
- [ ] `NotebookEstimate.hotspot` returns the most expensive cell
- [ ] `NotebookEstimate.display()` renders a table: cell index, preview, cost, % of total, hotspot flag
- [ ] Markdown cells are skipped (not estimated)
- [ ] Unit tests cover: `.py` parsing, `.ipynb` cell breakdown, PySpark action attribution, hotspot detection
- [ ] All existing tests still pass
- [ ] Lint passes: `uv run ruff check src/ tests/`

---

## Verification

### Commands

```bash
uv run pytest -m unit -v -k "notebook_cell or NotebookEstimate"
uv run ruff check src/ tests/
```

### Integration Check

- [ ] Create a test `.py` file with 3 cells: one SQL SELECT, one PySpark transformation, one PySpark write action. `burnt.estimate("test.py")` returns a `NotebookEstimate` with `hotspot` pointing to the write action cell.

---

## Handoff

### Result

[Executor fills this in when done.]

```yaml
status: todo
```

### Blocked reason

Blocked on s3-01 (connected-mode estimation needed for meaningful per-cell cost numbers).
