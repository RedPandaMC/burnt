# burnt — Technical Specification

> Cost compiler for Databricks. Parses Python, SQL, and DLT notebooks. Builds a cost graph. Shows where the money goes.

---

## 1. Product

burnt parses Databricks notebooks, builds a structural model of every data operation, and produces:

- **Cost Graph** (Python/SQL) or **Pipeline Graph** (DLT) — per-operation or per-table dollar estimates
- **84 lint rules** — expensive patterns linked to graph nodes with cost impact
- **Recommendations** — right-sized cluster configs with Databricks API JSON
- **Session cost** — execution cost vs idle cluster cost
- **Monitoring** — tag attribution, idle detection, cost drift alerts
- **Alerts** — Slack, Teams, webhook, Delta table
- **Feedback loop** — predicted vs actual billing, auto-calibrating coefficients

Requires a Databricks connection. No offline mode.

### Metrics

| Metric | Target |
|--------|--------|
| Accuracy (notebook, full enrichment) | 2× of actual |
| Accuracy (REST-only) | 3× of actual |
| Latency (50-cell notebook) | < 3 seconds |
| Driver memory overhead | < 50 MB |
| Rules | 84 |

---

## 2. Environments

**Databricks Notebook.** SparkSession. Full: DESCRIBE DETAIL, EXPLAIN COST, Spark UI metrics, system tables, cluster config.

**Connected Laptop / CI.** REST API via `databricks-sdk`: Statement Execution API, Jobs/Clusters/Pipelines API, DABs YAML. No EXPLAIN COST.

**Scheduled Job.** Single-node. System tables for monitoring. Sends alerts.

### Access Levels

| Level | Available | Output |
|-------|-----------|--------|
| Full | System tables + SparkSession | Graph + estimates + monitoring + alerts |
| Session | SparkSession, no system tables | Graph + DESCRIBE enrichment + structural findings |
| REST | REST API, no SparkSession | Graph + Delta enrichment + medium confidence |
| Auth-only | Authenticated, no permissions | 84 lint rules |

Never crashes on missing permissions. Explains what to enable for more.

---

## 3. Analysis Modes

Auto-detected. User never chooses.

| Mode | Detection | Output |
|------|-----------|--------|
| Python | Default cells, no DLT/SDP signal | CostGraph (operations) |
| SQL | All cells SQL, no Python | CostGraph (SQL statements) |
| DLT/SDP | `import dlt` / `import dp` / `CREATE STREAMING TABLE` / `LIVE.ref` | PipelineGraph (tables) |

Priority: DLT signals → DLT. All cells SQL → SQL. Otherwise → Python.

---

## 4. Architecture

```
CLI: burnt check          Python API: burnt.check() / burnt.watch()
      │                          │                  │
      │     ┌────────────────────┘                  │
      ▼     ▼                                       ▼
  Code Analysis                              Cost Monitoring
  (Rust engine → graph → enrich              (system table SQL
   → estimate → recommend)                    → alerts)
```

**Rust engine** (`burnt-engine` via PyO3): tree-sitter Python + SQL, `%run` resolution, mode detection, semantic model, CostGraph / PipelineGraph construction, 84 rules.

**Python layer** (`burnt`): enrichment (Delta, EXPLAIN, DLT pipeline), estimation, session cost, recommendations, feedback calibration, monitoring, alerts.

---

## 5. Cost Graph

Python and SQL modes.

### CostNode

| Field | Description |
|-------|-------------|
| kind | read, transform, shuffle, action, write, udf_call, maintenance |
| scaling_type | linear, linear_with_cliff, quadratic, step_failure, maintenance |
| photon_eligible | Can Photon accelerate this? |
| shuffle_required | Triggers shuffle? |
| driver_bound | Materializes on driver? |
| tables_referenced | Tables this node touches |
| estimated_input_bytes | Filled by enrichment |
| estimated_cost_usd | Filled by estimation |

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

Rust assigns type. Python fills thresholds from cluster config.

---

## 6. Pipeline Graph

DLT/SDP mode.

### PipelineTable

| Field | Description |
|-------|-------------|
| kind | streaming, materialized_view, temporary_view |
| source_type | cloud_files, kafka, dlt_read, live_ref |
| inner_nodes | CostNodes inside the table definition |
| expectations | Data quality constraints |
| is_incremental | Streaming = true, MV = false |

### DLT Detection

AST-based: `import dlt`, `from dlt import`, `@dlt.table`, `@dp.table`, `@dp.materialized_view`, `CREATE STREAMING TABLE`, `CREATE MATERIALIZED VIEW`, `LIVE.ref`.

### DLT Cost

| Kind | Formula |
|------|---------|
| Streaming | `batch_bytes × coefficient` per batch |
| Materialized view | `full_source_bytes × coefficient` per run |
| Pipeline overhead | `sum × 1.12` |

DLT tiers: CORE ~1×, PRO ~1.5×, ADVANCED ~2.5×.

---

## 7. Parsers

**tree-sitter** for Python and SQL. Stable API, semver grammars, unified types, S-expression queries, error recovery.

**sqlparser-rs** with `DatabricksDialect` for typed SQL AST: MERGE INTO, CREATE TABLE AS SELECT, correlated subqueries.

```toml
tree-sitter = "0.24"
tree-sitter-python = "0.23"
tree-sitter-sql = "0.3"
sqlparser = { version = "0.60", features = ["visitor"] }
```

---

## 8. Rules

### Rule Categories

Rules are organized by **category** (not tiers) for user-facing clarity:

| Category | Prefix | Description |
|----------|--------|-------------|
| **Performance** | BP | PySpark/SQL performance issues (collect without limit, cross joins, etc.) |
| **SQL** | BQ, SQ | SQL anti-patterns (NOT IN with NULLs, correlated subqueries, etc.) |
| **SDP** | SDP | Spark Declarative Pipelines (DLT) - missing expectations, no schema, etc. |
| **Style** | BNT | Code style & naming conventions |
| **Notebook** | BN, BB | Notebook structure and metadata |
| **Delta** | BD | Delta Lake optimizations (ZORDER, vacuum, etc.) |

### Implementation Tiers (Internal)

| Tier | Implementation | Description |
|------|----------------|-------------|
| **Tier 1** | TOML + tree-sitter S-expressions | Simple pattern matching, no Rust required |
| **Tier 2** | TOML + Rust context | Loop detection, naming patterns, chain context |
| **Tier 3** | TOML + Rust dataflow | Cross-cell binding analysis, cache lifecycle |

### Rule File Format

Rules are defined in TOML files organized by category:

```toml
[rule]
id = "collect_without_limit"
code = "BP008"
severity = "error"
language = "python"
description = "collect() without limit() can OOM the driver"
suggestion = "Add .limit(n).collect() or use .take(n)"
category = "Performance"
tier = 1

[query]
detect = """
(call
  function: (attribute
    object: (_) @df
    attribute: (identifier) @method)
  (#eq? @method "collect"))
"""
```

The `detect` field is a raw tree-sitter S-expression query.  An optional
`exclude` field acts as a negative pattern — if it matches anywhere in the same
source the rule is suppressed.

> **CPL (Cinder Pattern Language)** — a proposed human-readable rule DSL —
> is a post-v2.0 item.  TOML rules currently use tree-sitter S-expressions
> directly.

### Execution

Phase 0: source text → 1: tree-sitter parse (parallel) → 2: Tier 1 queries → 3: Tier 2 context → 4: Tier 3 semantic → 5: sqlparser-rs deep → 6: post-process (escalation, suppression, sort).

Suppression: `# burnt: ignore[BP001]` (line), `# burnt: ignore-file[BP001]` (file).

---

## 9. Estimation

### Python/SQL

Topological graph walk. `estimated_input_bytes × coefficient → dbu → cost_usd`. Un-enriched → heuristic, `confidence: low`. Infrastructure: `vm_cost × (workers + 1) × hours`. TCO = dbu + infra.

### DLT

Per-table walk. Streaming: `batch × coeff × batches`. MV: `source × coeff`. Overhead ×1.12. DLT tier rates.

### Session Cost (Notebook)

```
execution_cost + idle_cost = total_session_cost
utilization = execution_time / total_time
```

### Recommendations

Python/SQL: ALL_PURPOSE → JOBS_COMPUTE, instance right-sizing, Photon (>60% eligible), spot, API JSON.
DLT: MV → streaming conversion, tier optimization.
Session: serverless when utilization < 30%, auto-termination.

---

## 10. Monitoring

Python API only. Not in CLI.

| Function | Source |
|----------|--------|
| Tag attribution | system.billing.usage |
| Idle clusters | system.compute.node_timeline |
| Cost drift | system.billing + lakeflow |
| Job report | system.lakeflow.job_run_timeline |
| Pipeline report | system.lakeflow.pipeline_event_log |

All accessible through `burnt.watch()`. Output: `.display()`, `.json()`, `.alert()`.

---

## 11. Feedback

`result.calibrate(job_id, run_id)` — actual from billing, per-node comparison.
`result.calibrate(pipeline_id, update_id)` — actual from pipeline_event_log, per-table.
EMA: `0.3 × observed + 0.7 × old`. Stored per config file or Delta table.

---

## 12. Configuration

### Discovery

burnt searches for config in this order, using the first file found:

1. `burnt.toml` (project root, walking up from target path)
2. `.burnt.toml` (same search)
3. `pyproject.toml` → `[tool.burnt]` section (same search)
4. `~/.config/burnt/burnt.toml` (user home)

This matches how ruff, black, and pytest discover configuration. A single project has one config location. burnt walks upward from the file being checked until it finds one of these files or hits the filesystem root.

### `burnt.toml` / `.burnt.toml`

Standalone config. All sections at top level.

```toml
[connection]
warehouse_id = "abc123def456"

[tables]
billing_usage = "governance.cost_management.v_billing_usage"
query_history = "governance.cost_management.v_query_history"
list_prices = "governance.cost_management.v_list_prices"

[check]
skip = ["BNT-A01", "BNT-A02"]
max_cost = 50.0
severity = "warning"

[watch]
tag_key = "team"
drift_threshold = 0.25
idle_threshold = 0.10
budget = 5000.0
days = 30

[alert]
slack = "https://hooks.slack.com/services/T00/B00/xxx"

[calibration]
store = "local"
```

### `pyproject.toml`

Same schema, nested under `[tool.burnt]`. Coexists with ruff, pytest, hatch, etc.

```toml
[project]
name = "my-databricks-project"

[tool.ruff]
line-length = 88

[tool.burnt]
# Flat keys go under [tool.burnt]

[tool.burnt.connection]
warehouse_id = "abc123def456"

[tool.burnt.tables]
billing_usage = "governance.cost_management.v_billing_usage"

[tool.burnt.check]
skip = ["BNT-A01", "BNT-A02"]
max_cost = 50.0
severity = "warning"

[tool.burnt.watch]
tag_key = "team"
drift_threshold = 0.25
idle_threshold = 0.10
budget = 5000.0

[tool.burnt.alert]
slack = "https://hooks.slack.com/services/T00/B00/xxx"

[tool.burnt.calibration]
store = "local"
```

When burnt finds `pyproject.toml`, it reads the `[tool.burnt]` section and strips the prefix. The resulting config is identical to `burnt.toml` — the schema is the same, just the nesting differs.

If `pyproject.toml` exists but has no `[tool.burnt]` section, burnt ignores it and continues searching (user home).

### Environment Variables

Every config key maps to `BURNT_` prefix with `__` for nesting:

```bash
BURNT_CONNECTION__WAREHOUSE_ID=abc123def456
BURNT_TABLES__BILLING_USAGE=governance.v_billing_usage
BURNT_CHECK__MAX_COST=50.0
BURNT_ALERT__SLACK=https://hooks.slack.com/...
```

### Programmatic Override

```python
burnt.config(
    warehouse_id="abc123",
    billing_table="governance.v_billing_usage",
)
```

### Priority

Highest wins:

```
Function arguments / CLI flags
  > burnt.config()
    > burnt.toml / .burnt.toml / pyproject.toml [tool.burnt]  (first found)
      > ~/.config/burnt/burnt.toml
        > BURNT_* env vars
          > defaults
```

### Config Sections

| Section | Used By | Keys |
|---------|---------|------|
| `[connection]` | All | `warehouse_id` |
| `[tables]` | Enrichment, monitoring | `billing_usage`, `query_history`, `list_prices` |
| `[check]` | `burnt.check()`, `burnt check` | `skip`, `only`, `max_cost`, `severity` |
| `[watch]` | `burnt.watch()` | `tag_key`, `drift_threshold`, `idle_threshold`, `budget`, `days` |
| `[alert]` | `.alert()` | `slack`, `teams`, `webhook`, `delta` |
| `[calibration]` | `.calibrate()` | `store` (`"local"` or `"delta:catalog.schema.table"`) |

### CLI

CLI reads the discovered config file. Flags override:

```bash
# Uses max_cost from config
burnt check ./notebooks/

# Overrides
burnt check ./notebooks/ --max-cost 100
```

`burnt check --init` generates a starter `burnt.toml` in the current directory. If `pyproject.toml` exists, asks whether to add `[tool.burnt]` there instead.

---

## 13. Stack

### Rust: `burnt-engine`

```toml
pyo3 = { version = "0.22", features = ["extension-module"] }
tree-sitter = "0.24"
tree-sitter-python = "0.23"
tree-sitter-sql = "0.3"
sqlparser = { version = "0.60", features = ["visitor"] }
toml = "0.8"
rayon = "1.10"
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
```

### Python: `burnt`

```toml
dependencies = [
    "burnt-engine>=0.1.0",
    "pydantic>=2.0,<3",
    "pydantic-settings>=2.0",
    "databricks-sdk>=0.50.0",
    "rich>=13.0",
    "typer>=0.15",
    "pyyaml>=6.0",
]

[project.optional-dependencies]
alerts = ["slack-sdk>=3.0"]
```

---

## 14. Package Structure

```
src/burnt-engine/
├── Cargo.toml
├── build.rs
├── rules/
│   ├── registry.toml
│   ├── tier1/{pyspark,sql,dlt}/
│   ├── tier2/
│   └── tier3/
└── src/
    ├── lib.rs
    ├── ingestion/
    ├── detect.rs
    ├── parse/
    ├── semantic/
    ├── graph/{cost_graph,pipeline_graph,model,serialize}.rs
    ├── rules/
    └── types.rs

src/burnt/
├── __init__.py            # check(), watch(), config()
├── _connection.py         # Access level detection
├── _config.py             # Config loading: burnt.toml + env + args
├── graph/
│   ├── model.py           # Pydantic: CostGraph, PipelineGraph
│   ├── enrich.py          # Delta + EXPLAIN
│   ├── enrich_dlt.py      # Pipeline config + event log
│   ├── estimate.py        # Graph walk → CostEstimate
│   └── scaling.py         # 7 scaling functions
├── intelligence/
│   ├── recommend.py
│   ├── feedback.py
│   └── session.py
├── watch/
│   ├── tags.py
│   ├── idle.py
│   ├── drift.py
│   ├── job.py
│   ├── pipeline.py
│   └── core.py            # burnt.watch() orchestration
├── alerts/
│   └── dispatch.py        # .alert(slack=, teams=, webhook=, delta=)
├── runtime/
│   ├── detect.py
│   ├── spark.py
│   ├── rest.py
│   └── dabs.py
├── catalog/
│   ├── instances.py
│   └── pricing.py
├── display/
│   ├── notebook.py
│   ├── terminal.py
│   └── export.py
├── result.py              # CheckResult class
├── cli.py                 # burnt check (< 200 lines)
└── config.py              # Config model + loading

templates/burnt_monitor.py
```

---

## 15. Python API

```python
import burnt

# ── Code analysis ─────────────────────────────────
result = burnt.check()                      # Current notebook
result = burnt.check("./pipeline.py")       # File or directory

result.display()          # Notebook HTML or terminal table
result.cost               # CostEstimate
result.findings           # list[Finding]
result.graph              # CostGraph or PipelineGraph
result.mode               # "python" | "sql" | "dlt"
result.session            # SessionCost (notebook only)
result.api_json()         # Recommended cluster JSON
result.json()             # Full result as dict
result.markdown()         # Markdown string
result.calibrate(job_id=12345, run_id=67890)

# ── Monitoring (notebook or scheduled job) ────────
audit = burnt.watch(
    tag_key="team",
    drift_threshold=0.25,
    idle_threshold=0.10,
    budget=5000.0,
    days=30,
)
audit = burnt.watch(job_id=12345)           # Single job
audit = burnt.watch(pipeline_id=67890)      # Single pipeline

audit.display()
audit.json()
audit.alert(slack="https://hooks.slack.com/...")
# or: alert(teams=, webhook=, delta=)
# or: no args → uses [alert] from burnt.toml

# ── Config ────────────────────────────────────────
burnt.config(
    warehouse_id="abc123",
    billing_table="governance.v_billing_usage",
)
```

---

## 16. CLI

One command. Code analysis only. Monitoring is Python API.

```bash
burnt check <path>                          # File, directory, or glob
burnt check <path> --cluster yaml:target    # DABs cluster config
burnt check <path> --json                   # JSON output
burnt check <path> --markdown               # Markdown output
burnt check <path> --strict                 # Exit 1 on any error
burnt check <path> --max-cost 25            # Exit 1 if cost > $25/run
burnt check <path> --only BP001,DLT001      # Only these rules
burnt check <path> --skip BNT-A01           # Skip these rules

burnt check --explain                       # List all 84 rules
burnt check --explain BP007                 # Explain one rule with examples
burnt check --init                          # Generate burnt.toml

burnt version
```

Exit codes: 0 clean, 1 threshold exceeded, 2 internal error.

CLI reads `burnt.toml` for defaults. Flags override config.

---

## 17. Graceful Degradation

| Failure | Behavior |
|---------|----------|
| DESCRIBE DETAIL fails | Un-enriched, heuristic, low confidence |
| System tables inaccessible | Monitoring disabled, analysis works |
| Pipelines API unavailable | Default DLT tier rates |
| EXPLAIN COST fails | Skip, use DESCRIBE + heuristic |
| Dynamic SQL (`f"SELECT FROM {var}"`) | BN002 + partial graph |
| Widget table name | Try default value, else unknown |
| `%run` target missing | BN001, continue |
| Parse error | Finding, partial tree |
| No credentials | `ConnectionRequired` with instructions |

---

## 18. System Tables

| Table | Used By |
|---|---|
| `system.billing.usage` | watch (tags, drift), feedback |
| `system.billing.list_prices` | DBU → USD (all SKUs) |
| `system.query.history` | Feedback per-node |
| `system.compute.node_types` | Instance catalog |
| `system.compute.node_timeline` | watch (idle) |
| `system.lakeflow.jobs` | watch (job_report) |
| `system.lakeflow.job_run_timeline` | Per-run cost |
| `system.lakeflow.pipeline_event_log` | DLT metrics, feedback |
| `INFORMATION_SCHEMA.TABLES` | Delta enrichment |

Override via `[tables]` in `burnt.toml` or `burnt.config()`.

---

## 19. Design Principles

1. Databricks-native. Requires connection.
2. One graph, every view.
3. Three modes, auto-detected.
4. CLI checks code. Python API monitors costs. Right tool for the job.
5. Honest confidence.
6. Code-aware. Per-operation cost.
7. Existence-aware. Idle cost alongside execution cost.
8. Pipeline-aware. DLT table-level cost.
9. Graceful always.
10. Config file for defaults, flags for overrides, env vars for CI.

---

## 20. Phases

| Phase | Duration | Deliverable |
|-------|----------|-------------|
| P1 Rust Engine | 6 wks | Parsing, 3 graphs, 84 rules, PyO3 |
| P2 Python Intelligence | 4 wks | Enrichment, estimation, recs, feedback, session cost |
| P3 Display & CLI | 3 wks | `check()`, `burnt check`, 3 layouts, degradation |
| P4 Monitoring & Alerts | 3 wks | `watch()`, `.alert()`, monitoring template |
| P5 Integration | 2 wks | Edge cases, config system, docs, wheels |
| P6 Validation | 1 wk | Dogfood, security, performance |

**Total: 19 weeks.**

---

## 21. Post v2.0

- Graph-aware simulation (`result.simulate().enable_photon().compare()`)
- Structural forecasting (threshold alerts)
- Custom TOML rules from user directories
- Multi-workspace monitoring
