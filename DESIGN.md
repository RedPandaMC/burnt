# burnt — Technical Specification

> Performance coach for Spark data engineers. Watches your practice runs, learns from Spark metrics, and tells you how to ship cheaper code.

---

## 1. Product

burnt sits between **development** and **production**. You run your notebook, execute your Spark code, then ask burnt for a review. It combines:

- **Static analysis** (Rust engine) — parses Python/SQL code, builds a cost graph, finds expensive patterns
- **Runtime analysis** — listens to your Spark session, captures actual stage metrics, shuffle, spill, and query history
- **Actionable advice** — specific, data-backed recommendations: "60% of your cost is in this crossJoin on line 42. Add a salt column."

### Output

```
┌─ burnt check report ───────────────────────────────┐
│ 3 expensive operations found in 12 cells          │
│ Estimated compute: 14.2 executor-hours            │
│                                                    │
│ ⚠ Line 42: crossJoin without salt                 │
│   → Cost: 8.3 hr (58% of total)                   │
│   → Fix: Add salted join key to avoid skew        │
│                                                    │
│ ⚠ Line 67: collect() without limit()              │
│   → Cost: 2.1 hr (driver-bound, high risk)        │
│   → Fix: Use .limit(1000).collect()               │
│                                                    │
│ ℹ Line 89: repartition(1) before write            │
│   → Cost: 0.8 hr (single-task bottleneck)         │
│   → Fix: Remove or increase to repartition(200)   │
└────────────────────────────────────────────────────┘
```

### Metrics

| Metric | Target |
|--------|--------|
| Static analysis latency (50-cell notebook) | < 3 seconds |
| Driver memory overhead | < 50 MB |
| Lint rules | ~30 (cost-focused only) |
| Runtime capture overhead | < 5% CPU, negligible memory |

---

## 2. Philosophy

**Spark-first, not Databricks-first.** burnt works on any Spark cluster — local `pyspark`, EMR, Dataproc, Databricks, etc. Databricks-specific features (DBU pricing, system tables, workspace monitoring) live in an optional module.

**Practice-run coaching, not crystal-ball estimation.** We don't predict what your code *will* cost. We observe what it *did* cost during development and tell you how to improve it.

**Actionable over precise.** "8.3 executor-hours" is more useful than "$12.47" because the dollar amount depends on your cloud pricing. The engineer can act on "reduce shuffle" regardless of SKU.

---

## 3. Environments

### In-Notebook (Primary)

```python
import burnt

# Start listening to your Spark session
burnt.start_session()

# ... write and run your Spark code ...

# Get the review
report = burnt.check()
report.display()
```

Captures: SparkListener metrics, Query History (if available), cell execution times, stage-level shuffle/spill.

### Post-Session CLI

```bash
# Analyze a saved notebook + Spark event log
burnt check ./notebook.ipynb --event-log ./eventlog

# Or just static analysis (no runtime data)
burnt check ./pipeline.py
```

### Connected Mode (Optional)

With `pip install burnt[databricks]`:

```python
import burnt
report = burnt.check()  # Also queries Databricks query history, DESCRIBE DETAIL
```

---

## 4. Architecture

```
┌─ User Code ────────────────────────────────────────┐
│  burnt.start_session()  →  SparkListener registered │
│  ... your Spark code runs ...                      │
│  burnt.check()  →  Report                          │
└────────────────────────────────────────────────────┘
                          │
        ┌─────────────────┼─────────────────┐
        ▼                 ▼                 ▼
   ┌─────────┐     ┌─────────────┐   ┌──────────┐
   │  Rust   │     │   Runtime   │   │  Backend │
   │ Engine  │     │   Listener  │   │  (opt)   │
   │(static) │     │  (runtime)  │   │          │
   └────┬────┘     └──────┬──────┘   └────┬─────┘
        │                 │               │
        └─────────────────┼───────────────┘
                          ▼
                   ┌─────────────┐
                   │   Merger    │
                   │ (hybrid     │
                   │  analysis)  │
                   └──────┬──────┘
                          ▼
                   ┌─────────────┐
                   │   Report    │
                   │  + Display  │
                   └─────────────┘
```

### Components

**Rust engine** (`burnt-engine` via PyO3): tree-sitter Python/SQL parsing, CostGraph construction, ~30 cost-focused lint rules. Always installed (compiled wheel).

**Runtime listener** (`burnt.spark.listener`): `burnt.start_session()` registers a `SparkListener` that captures stage metrics, task durations, shuffle read/write bytes, spill to disk. Lightweight, no external dependencies.

**Backend** (`burnt.runtime`): Optional protocol for enriching with cloud-specific data.
- `SparkBackend`: Generic SparkSession introspection
- `DatabricksBackend`: `pip install burnt[databricks]` — adds query history, DESCRIBE DETAIL, DBU pricing

---

## 5. Session Lifecycle

### 1. Start

```python
burnt.start_session(
    capture_sql=True,        # Capture SQL query text + duration
    capture_stages=True,     # Capture stage metrics
    capture_cells=True,      # Capture cell execution times
)
```

- Registers `SparkListener` if `SparkSession` exists
- Starts a background thread to poll `SparkContext.statusTracker()`
- Records notebook cell boundaries (if in Jupyter/Databricks)

### 2. Run

User executes Spark code normally. burnt silently records:

| Signal | Source | Use |
|--------|--------|-----|
| Stage metrics | SparkListener | Shuffle, spill, input/output bytes |
| SQL query text | `SparkListener.onOtherEvent` (SQLExecution) | Correlate with static analysis |
| Cell timing | Jupyter kernel hooks / Databricks context | Idle vs execution time |
| Spark conf | `SparkSession.getActiveSession().conf` | Cluster topology |

### 3. Check

```python
report = burnt.check(path="./notebook.ipynb")  # optional path for static analysis
```

1. **Static pass**: Rust engine parses code → `CostGraph` + `Findings`
2. **Runtime pass**: Correlate graph nodes with captured stage metrics
   - CrossJoin node → stage with high shuffle bytes? High confidence match.
   - Read node → stage with large input bytes? Enrich estimated_input_bytes.
3. **Merge**: Tag each graph node with runtime data. Re-sort findings by actual cost impact.
4. **Recommend**: Generic Spark advice (add salt, enable AQE, reduce partitions, etc.)

### 4. Report

```python
report.display()       # Rich table (terminal) or HTML (notebook)
report.to_json()       # Machine-readable for CI
report.to_markdown()   # For PR descriptions
```

---

## 6. Cost Graph

Generic Spark model. No Databricks concepts in core.

### CostNode

| Field | Description |
|-------|-------------|
| kind | read, transform, shuffle, action, write, udf_call, maintenance |
| scaling_type | linear, linear_with_cliff, quadratic, step_failure, maintenance |
| shuffle_required | Triggers shuffle? |
| driver_bound | Materializes on driver? |
| tables_referenced | Tables this node touches |
| estimated_input_bytes | Filled by runtime or backend enrichment |
| actual_compute_seconds | Filled by runtime listener |
| actual_shuffle_bytes | Filled by runtime listener |
| line_number | Source location |

### SQL Statement → Nodes

| Statement | Nodes |
|-----------|-------|
| `SELECT ... GROUP BY` | Read → Shuffle |
| `CREATE TABLE AS SELECT ... JOIN` | Read + Read → Shuffle → Write |
| `MERGE INTO ... USING` | Read + Read → Shuffle → Write |
| `OPTIMIZE ... ZORDER BY` | Maintenance |
| Final `SELECT` | Read → Action |

Cross-cell: Write in cell 1 → Read in cell 2 creates an edge.

### Scaling Behaviors

| Type | Behavior |
|------|----------|
| Linear | ∝ input bytes |
| LinearWithCliff | Linear until memory exceeded, ~3× after (spill) |
| Quadratic | ∝ left × right (cross join) |
| StepFailure | Works until threshold, then OOM |
| Maintenance | ∝ table size + file count |

Rust assigns type. Runtime listener fills actual bytes/times.

---

## 7. Rules

**~30 rules, all cost-focused.** No style rules, no generic notebook hygiene.

| Category | Count | Examples |
|----------|-------|----------|
| Performance | 12 | collect() without limit, crossJoin without salt, repartition(1), explode() in select |
| SQL | 8 | SELECT *, missing predicate pushdown, cartesian product, nested subqueries |
| Spark Config | 5 | AQE disabled, shuffle partitions too high/low, broadcast threshold missed |
| Delta / Lake | 5 | Missing ZORDER, VACUUM, too many small files, no partition pruning |

Rules are ranked by **actual cost impact** when runtime data is available. A crossJoin that shuffled 50GB is shown before a missing ZORDER on a 1MB table.

---

## 8. Estimation

### Core Unit: Compute Seconds

burnt reports in **compute seconds** (or executor-hours). This is generic and actionable:

```
Operation          Compute   %Total   Action
─────────────────────────────────────────────
crossJoin (L42)    8.3 hr    58%      Add salt
collect() (L67)    2.1 hr    15%      Add limit
repartition(L89)   0.8 hr    6%       Remove or increase
```

### Backend Mapping (Optional)

With a backend installed, compute seconds can be mapped to dollar estimates:

```python
report.cost_estimate  # CostEstimate with USD if backend available
```

| Backend | Input | Output |
|---------|-------|--------|
| None | compute seconds | compute seconds only |
| Databricks | DBU rate + SKU | USD estimate |
| AWS EMR | EC2 pricing API | USD estimate |
| Custom | User-provided price sheet | USD estimate |

---

## 9. Display

### Terminal (`burnt check` CLI)

```
┌─ burnt check: pipeline.py ─────────────────────────┐
│ 3 issues found (1 error, 2 warnings)              │
│ Compute: 14.2 executor-hours                        │
│                                                     │
│ error  BP008  collect() without limit()            │
│        pipeline.py:67                                │
│        → Add .limit(n) or use .take(n)             │
│        → Estimated: 2.1 hr driver-bound             │
│                                                     │
│ warning BP003  crossJoin without salt               │
│        pipeline.py:42                                │
│        → Add salted join key to avoid skew         │
│        → Estimated: 8.3 hr (58% of total)           │
│        → Actual: 12.4 hr (observed 50GB shuffle)    │
└─────────────────────────────────────────────────────┘
```

### Notebook (HTML)

Collapsible sections per finding. Bar chart of compute by operation. Click to jump to source cell.

### Export

- `report.to_json()` — structured data for programmatic use
- `report.to_markdown()` — for PR descriptions or documentation

---

## 10. Databricks Optional Module

`pip install burnt[databricks]`

Adds:
- `DatabricksBackend` — query history, DESCRIBE DETAIL, system tables
- `burnt.watch()` — workspace cost monitoring (tags, idle clusters, drift)
- DBU pricing and dollar estimates
- DLT pipeline analysis

```python
import burnt  # core works immediately
# With databricks extra, check() is automatically enriched
report = burnt.check()
report.cost_estimate.estimated_cost_usd  # available if backend connected
```

---

## 11. Configuration

### Discovery

Same as ruff/black: walk up from target path looking for:

1. `burnt.toml`
2. `.burnt.toml`
3. `pyproject.toml` → `[tool.burnt]`
4. `~/.config/burnt/burnt.toml`

### `burnt.toml`

```toml
[check]
skip = ["BP008"]          # Skip specific rules
severity = "warning"       # Minimum severity to report

[session]
capture_sql = true
capture_stages = true
capture_cells = true

[display]
format = "auto"            # "auto" | "terminal" | "notebook"
show_runtime = true        # Include actual metrics when available

# Only used when burnt[databricks] is installed:
[connection]
warehouse_id = "abc123"

[watch]
tag_key = "team"
drift_threshold = 0.25
budget = 5000.0
```

---

## 12. CLI

```bash
# In-notebook style (capture session, then check)
burnt check                          # Current directory / notebook
burnt check ./pipeline.py            # Specific file
burnt check ./notebooks/ --json      # Directory, JSON output

# Post-hoc analysis (static + event log)
burnt check ./notebook.ipynb --event-log ./app-20260101-eventlog

# Configuration
burnt init                           # Generate burnt.toml
burnt rules                          # List all rules
burnt doctor                         # Check setup (Databricks-aware if extra installed)
```

---

## 13. Stack

### Rust: `burnt-engine`

```toml
pyo3 = { version = "0.22", features = ["extension-module"] }
tree-sitter = "0.24"
tree-sitter-python = "0.23"
tree-sitter-sql = "0.3"
sqlparser = { version = "0.60", features = ["visitor"] }
rayon = "1.10"
```

### Python: `burnt`

Core dependencies (always installed):
```toml
dependencies = [
    "pydantic>=2.0,<3",
    "pydantic-settings>=2.0",
    "rich>=13.0",
    "typer>=0.15",
    "pyyaml>=6.0",
]

[project.optional-dependencies]
databricks = ["databricks-sdk>=0.50.0"]
alerts = ["slack-sdk>=3.0"]
all = ["burnt[databricks,alerts]"]
```

---

## 14. Package Structure

```
src/burnt/
├── __init__.py            # start_session(), check(), config(), version()
├── _check.py              # Hybrid check() implementation
├── _config.py             # Config loading: burnt.toml + env + args
├── _session.py            # Session listener + metric storage
├── _display.py            # Auto-detect terminal vs notebook
├── core/
│   ├── __init__.py
│   ├── cache.py           # Generic TTL cache
│   ├── config.py          # Settings model (generic)
│   ├── exceptions.py      # BurntError, ConfigError, etc.
│   ├── models.py          # CostEstimate, ClusterConfig (generic)
│   └── protocols.py       # Backend protocol
├── graph/
│   ├── __init__.py
│   ├── model.py           # CostGraph, CostNode (generic)
│   ├── scaling.py         # Scaling functions
│   └── estimate.py        # Graph walk → compute seconds
├── runtime/
│   ├── __init__.py        # auto_backend()
│   ├── backend.py         # Backend protocol
│   └── spark_backend.py   # Generic SparkSession backend
├── spark/
│   └── listener.py        # SparkListener implementation
├── parsers/
│   ├── __init__.py
│   ├── explain.py         # Spark EXPLAIN parsing
│   ├── notebooks.py       # Jupyter notebook parsing
│   └── antipatterns.py    # AntiPattern dataclass + rust bridge
├── display/
│   ├── __init__.py
│   ├── terminal.py        # Rich table output
│   ├── notebook.py        # HTML output
│   └── export.py          # JSON, Markdown export
├── cli/
│   └── main.py            # CLI commands
└── databricks/            # Optional namespace
    ├── __init__.py
    ├── backend.py         # DatabricksBackend
    ├── watch/             # Workspace monitoring
    ├── pricing/           # DBU rates, instance catalog
    └── cli.py             # Databricks-specific CLI commands
```

---

## 15. Python API

```python
import burnt

# ── Session ─────────────────────────────────────────
burnt.start_session(capture_stages=True, capture_sql=True)

# ... run your Spark code ...

# ── Check ───────────────────────────────────────────
report = burnt.check()                      # Current notebook
report = burnt.check("./pipeline.py")       # File or directory

report.display()          # Auto-detects terminal vs notebook
report.findings           # list[Finding] (sorted by cost impact)
report.graph              # CostGraph
report.compute_seconds    # Total compute time from runtime
report.to_json()          # dict
report.to_markdown()      # str

# ── Config ──────────────────────────────────────────
burnt.config(
    warehouse_id="abc123",  # only used if databricks extra installed
)
```

---

## 16. Design Principles

1. **Spark-first.** Generic Spark is the default. Databricks is an enhancement.
2. **Post-development coaching.** We observe practice runs, not predict the future.
3. **Compute seconds over dollars.** Dollars vary by cloud; compute time is actionable everywhere.
4. **Hybrid analysis.** Static + runtime together is stronger than either alone.
5. **Actionable findings.** Every warning includes a specific fix.
6. **Honest confidence.** "Observed 50GB shuffle" > "Estimated $12.47".
7. **Graceful always.** Works without Spark, works without Databricks, degrades transparently.
8. **Optional extras.** Core is lightweight. Cloud-specific features are install-time opt-in.

---

## 17. Phases

| Phase | Duration | Deliverable |
|-------|----------|-------------|
| P1 Rust Engine | Done | Parsing, CostGraph, ~30 rules, PyO3 |
| P2 Session Listener | 2 wks | SparkListener, metric storage, cell tracking |
| P3 Hybrid Check | 2 wks | Merge static + runtime, ranking by actual cost |
| P4 Display & CLI | 2 wks | Terminal, notebook, JSON, markdown export |
| P5 Databricks Module | 2 wks | `burnt[databricks]`: backend, watch, pricing |
| P6 Integration | 1 wk | Edge cases, config, docs, wheels |

**Total: 9 weeks from current state.**
