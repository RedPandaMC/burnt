# burnt — Technical Specification

> Notebook quality and cost analysis for Databricks. Like ruff, but for your notebooks.

---

## 1. Product

burnt is a static analysis and runtime enrichment tool for Databricks notebooks. Its three modes in priority order:

1. **CLI static lint** — `burnt check ./notebook.py` finds expensive patterns without running any code
2. **Interactive coaching** — attach to a Spark session, run your code, call `burnt.check()` for runtime-enriched findings
3. **CI gate** — block PRs with cost or quality regressions using `--output sarif` or `--max-cost`

It combines:

- **Static analysis** (Rust engine) — parses Python/SQL code, builds a cost graph, 84 lint rules across 6 categories
- **Runtime enrichment** — sparkMeasure captures actual stage metrics (shuffle, spill, CPU) and correlates them to graph nodes
- **Actionable findings** — every warning includes a specific fix

### Output

```
$ burnt check ./notebooks/pipeline.py

  error   BP008  collect() without limit()
          pipeline.py:67
          → Materialises entire dataset on driver — use .limit(n) or .take(n)

  warning BP003  crossJoin without salt
          pipeline.py:42
          → O(n*m) row explosion. Add a salt column to the join key.

  warning BD002  OPTIMIZE without ZORDER
          pipeline.py:88
          → Add ZORDER BY your most-filtered column to speed up file skipping.

3 findings  (1 error, 2 warnings)
```

### Targets

| Metric | Target |
|--------|--------|
| Static analysis latency (50-cell notebook) | < 3 seconds |
| Cold start (`import burnt; burnt.check("x.py")`) | < 1 second |
| Driver memory overhead (static only) | < 50 MB RSS |
| Lint rules | 84 across 6 categories |
| Runtime capture overhead | < 5% CPU, negligible memory |

---

## 2. Philosophy

1. **Databricks-first.** The lint rules work without credentials. Cost intelligence requires Databricks. Be honest about it.
2. **CLI-first.** `burnt check` is the product. The Python API is a second mode.
3. **Full notebook hygiene.** Cost rules + style rules + structure rules. "ruff for Databricks notebooks."
4. **Static + runtime.** Static rules always run. sparkMeasure enriches them when a Spark session is active.
5. **Compute seconds over dollars.** "8.3 executor-hours" is actionable everywhere. Backends optionally map to USD.
6. **Honest confidence.** "Observed 50GB shuffle" is shown differently from "Estimated 50GB shuffle."
7. **Graceful degradation.** No credentials → 84 lint rules. Spark session → runtime enrichment. `pip install burnt[databricks]` → dollar estimates.

---

## 3. Environments

### CLI (Primary)

```bash
# Static lint — no credentials, no Spark needed
burnt check ./notebook.py
burnt check ./notebooks/ --select BP* --ignore BNT_*

# SARIF output for GitHub Code Scanning
burnt check ./notebook.py --output sarif > burnt.sarif

# Enrich with a saved Spark event log
burnt check ./notebook.py --event-log ./app-20260427-eventlog
```

### In-Notebook (Coaching Mode)

```python
import burnt

burnt.start_session()     # attaches sparkMeasure to the active SparkSession

# ... write and run your Spark code ...

report = burnt.check()    # static analysis + runtime correlation
report.display()          # Rich table (terminal) or HTML (notebook)
```

Captures via sparkMeasure: per-stage executorRunTime, shuffleReadBytes, shuffleWriteBytes, memoryBytesSpilled, diskBytesSpilled.

### CI/CD (Gate Mode)

```yaml
# GitHub Actions
- run: burnt check ./notebooks/ --output sarif > burnt.sarif
- uses: github/codeql-action/upload-sarif@v3
  with: { sarif_file: burnt.sarif }
```

### Connected (`pip install burnt[databricks]`)

Adds dollar estimates, system table queries, and DESCRIBE DETAIL enrichment. Lint rules still run without this extra.

---

## 4. Architecture

```
┌─ CLI / Notebook ───────────────────────────────────┐
│  burnt check ./notebook.py         (CLI)            │
│  burnt.start_session(); burnt.check()  (notebook)   │
└────────────────────────────────────────────────────┘
                          │
        ┌─────────────────┼─────────────────┐
        ▼                 ▼                 ▼
   ┌─────────┐     ┌─────────────┐   ┌──────────┐
   │  Rust   │     │sparkMeasure │   │  Backend │
   │ Engine  │     │  (runtime)  │   │  (opt)   │
   │ 84 rules│     │  StageMetrics│   │          │
   └────┬────┘     └──────┬──────┘   └────┬─────┘
        │                 │               │
        └─────────────────┼───────────────┘
                          ▼
                   ┌─────────────┐
                   │   Merger    │
                   │ correlate   │
                   │ nodes↔stages│
                   └──────┬──────┘
                          ▼
                   ┌─────────────┐
                   │  CheckResult│
                   │  + Display  │
                   └─────────────┘
```

### Components

**Rust engine** (`burnt-engine` via PyO3): tree-sitter Python/SQL parsing, CostGraph construction, 84 lint rules across 6 categories. Always installed (compiled wheel).

**Runtime capture** (`runtime/sparkmeasure.py`): `burnt.start_session()` initialises `StageMetrics(spark)`. At `check()` time, calls `metrics.end()` then `metrics.create_df().collect()` to get per-stage data. Falls back to Spark REST API (`localhost:4040`) if sparkMeasure is not installed.

**Backend** (`burnt.runtime`): Optional protocol for enriching with cloud-specific data.
- `SparkBackend`: generic SparkSession introspection (REST API, event log)
- `DatabricksBackend`: `pip install burnt[databricks]` — adds DESCRIBE DETAIL, system tables, DBU pricing

---

## 5. Session Lifecycle

### 1. Start

```python
burnt.start_session()
```

Internally:

```python
from sparkmeasure import StageMetrics   # pip install burnt[spark]

spark = SparkSession.getActiveSession()
_SESSION = StageMetrics(spark)
_SESSION.begin()
```

If sparkMeasure is not installed, falls back to polling the Spark REST API at `spark.sparkContext.uiWebUrl + "/api/v1/..."`. If no Spark session is active, `start_session()` returns silently — static analysis still works.

**Fallback hierarchy:**
1. sparkMeasure (`pip install burnt[spark]`) — native Scala listener, full per-stage data *(primary)*
2. Spark REST API (`localhost:4040`) — post-execution, no install needed *(fallback)*
3. Event log (`--event-log <path>`) — fully offline, batch use *(CLI flag)*

### 2. Run

User executes Spark code normally. sparkMeasure records per stage:

| Field | Description |
|-------|-------------|
| `executorRunTime` | Total executor CPU+wall time (ms) |
| `executorCpuTime` | CPU time only (ms) |
| `shuffleReadBytes` | Bytes read from shuffle |
| `shuffleWriteBytes` | Bytes written to shuffle |
| `memoryBytesSpilled` | Memory spill (indicates OOM pressure) |
| `diskBytesSpilled` | Disk spill |
| `inputBytes` | Bytes read from storage |
| `outputBytes` | Bytes written to storage |

### 3. Check

```python
report = burnt.check(path="./notebook.py")  # path is optional
```

1. **Static pass**: Rust engine parses code → `CostGraph` + static `Findings`
2. **Runtime pass**:
   ```python
   _SESSION.end()
   stages = _SESSION.create_df().collect()   # list of Row objects
   ```
   Each stage `name` field encodes the source call site (e.g. `"crossJoin at pipeline.py:42"`). Stages are matched to graph nodes by comparing the encoded file/line against `node.line_number ± 5`.
3. **Enrich**: matched nodes get `actual_compute_seconds = sum(executorRunTime) / 1000` and `actual_shuffle_bytes`.
4. **Re-rank**: findings sorted by `actual_compute_seconds` descending (observed cost beats estimated).

### 4. Report

```python
report.display()         # Rich table (terminal) or HTML (notebook)
report.to_json()         # machine-readable for CI
report.to_markdown()     # for PR descriptions
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

84 rules across 6 categories. Three tiers: TOML query (tree-sitter pattern, no Rust required) → context-aware (Rust) → dataflow semantic (Rust).

| Category | Prefix | Count | Examples |
|----------|--------|-------|---------|
| Performance | `BP*` | ~18 | `collect()` without limit, `crossJoin`, `repartition(1)`, `withColumn` in loop, `explode` in select |
| SQL quality | `SQ*`, `BQ*` | ~10 | `SELECT *`, `NOT IN` with NULLs, correlated subquery, missing predicate pushdown |
| Delta / Lake | `BD*` | ~5 | missing `ZORDER`, `VACUUM` frequency, too many small files |
| DLT / SDP | `SDP*` | ~5 | missing expectation, streaming without key, non-idempotent pipeline |
| Notebook style | `BNT_*` | ~3 | generic `df` name, star import, bare `df` reference without action |
| Notebook structure | `BB*`, `BN*` | varies | magic commands in plain Python, deprecated `%python` syntax |

Rules are ranked by **actual cost impact** when runtime data is available. A `crossJoin` that shuffled 50GB is surfaced before a missing `ZORDER` on a 1MB table.

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

With `pip install burnt[databricks]`, compute seconds are mapped to dollar estimates via DBU rates.

```python
result.cost_estimate  # CostEstimate with USD if DatabricksBackend is active
```

| Setup | Output |
|-------|--------|
| Core only | compute seconds |
| `burnt[databricks]` | compute seconds + USD estimate |

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

- `result.to_json()` — structured data for programmatic use
- `result.to_markdown()` — for PR descriptions or documentation
- `result.to_sarif()` / `--output sarif` — SARIF 2.1.0 for GitHub Code Scanning

---

## 10. Databricks Optional Module

`pip install burnt[databricks]`

Adds:
- `DatabricksBackend` — DESCRIBE DETAIL enrichment, system table queries
- DBU pricing and dollar estimates
- DLT pipeline graph analysis

```python
import burnt  # core 84 rules work immediately
# With databricks extra, check() is enriched with dollar estimates
result = burnt.check()
result.cost_estimate.estimated_cost_usd  # available if DatabricksBackend is active
```

Works without credentials — lint rules run regardless. Credentials unlock cost estimates.

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
select = ["BP*", "BD*"]   # only run these rule prefixes/IDs (default: all)
ignore = ["BNT_001"]      # skip specific rules
severity = "warning"       # minimum severity to report (error|warning|note)
max_cost = 50.0            # fail if total compute exceeds N executor-hours

[display]
format = "auto"            # "auto" | "terminal" | "notebook"

# Only used when burnt[databricks] is installed:
[connection]
warehouse_id = "abc123"
```

---

## 12. CLI

```bash
# ── check ──────────────────────────────────────────────────────────
burnt check ./notebook.py            # single file
burnt check ./notebooks/             # directory (recursive)
burnt check ./pipeline.sql           # inline SQL

# Output formats
burnt check ./notebook.py --output table    # default Rich table
burnt check ./notebook.py --output json     # machine-readable
burnt check ./notebook.py --output text     # plain text (no colours)
burnt check ./notebook.py --output sarif    # SARIF 2.1.0 for GitHub Code Scanning

# Rule filtering
burnt check ./notebook.py --select BP*        # only performance rules
burnt check ./notebook.py --ignore BNT_*      # skip style rules
burnt check ./notebook.py --select BP014,BD*  # mix of IDs and prefixes

# CI gates
burnt check ./notebook.py --fail-on error     # exit 1 only on errors
burnt check ./notebook.py --max-cost 25       # exit 1 if total compute > 25 hrs

# Event log enrichment (offline runtime data)
burnt check ./notebook.py --event-log ./app-20260427-eventlog

# ── rules ──────────────────────────────────────────────────────────
burnt rules                     # interactive TUI — browse, filter, toggle rules
burnt rules --output json       # dump all rules as JSON

# ── init ───────────────────────────────────────────────────────────
burnt init                      # create .burnt.toml interactively

# ── doctor ─────────────────────────────────────────────────────────
burnt doctor                    # check config, Spark availability, Databricks connectivity

# ── cache ──────────────────────────────────────────────────────────
burnt cache clear               # remove cached analysis results
burnt cache show                # print cache path and entry count
```

**Exit codes:**

| Code | Meaning |
|------|---------|
| 0 | No findings above the configured threshold |
| 1 | One or more findings at or above threshold |
| 2 | Parse or configuration error |

---

## 13. CI Integration

### Pre-commit hook

```yaml
# .pre-commit-config.yaml
repos:
  - repo: local
    hooks:
      - id: burnt-check
        name: burnt check
        entry: burnt check
        language: system
        types: [python, sql]
        pass_filenames: true
```

### GitHub Actions — lint gate with SARIF

```yaml
# .github/workflows/burnt-check.yml
name: burnt lint
on: [push, pull_request]
jobs:
  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: pip install burnt
      - run: burnt check ./notebooks/ --output sarif > burnt.sarif
      - uses: github/codeql-action/upload-sarif@v3
        if: always()
        with:
          sarif_file: burnt.sarif
```

Findings appear as inline annotations on PR diffs. No token required beyond the default `GITHUB_TOKEN`.

### GitHub Actions — cost gate

```yaml
# .github/workflows/burnt-cost-gate.yml
- run: pip install burnt
- run: burnt check ./notebooks/ --max-cost 50 --fail-on error
```

Blocks the PR if total estimated compute exceeds 50 executor-hours or any error-severity finding is present.

### Databricks Asset Bundles — pre-deploy hook

```yaml
# databricks.yml
bundle:
  name: my-pipeline
  hooks:
    pre-deploy:
      - burnt check ./src/ --fail-on error
```

---

## 14. Stack

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

## 15. Package Structure

```
src/burnt/
├── __init__.py            # start_session(), check(), version()
├── _check/
│   └── __init__.py        # check() orchestration: static → runtime → merge
├── _config.py             # Config loading: burnt.toml + env + args
├── _session.py            # sparkMeasure wrapper + REST fallback
├── core/
│   ├── cache.py           # TTL cache
│   ├── config.py          # BurntConfig (Pydantic settings)
│   ├── exceptions.py      # BurntError, ConfigError
│   ├── models.py          # Finding, CheckResult, CostEstimate
│   └── protocols.py       # Backend protocol
├── graph/
│   ├── model.py           # CostGraph, CostNode
│   ├── scaling.py         # LinearScaling, QuadraticScaling, etc.
│   └── estimate.py        # Graph walk → compute seconds
├── runtime/
│   ├── backend.py         # Backend protocol
│   ├── spark_backend.py   # Generic SparkSession + REST API backend
│   ├── sparkmeasure.py    # StageMetrics wrapper (pip install burnt[spark])
│   └── event_log.py       # Event log parser (--event-log flag)
├── parsers/
│   ├── explain.py         # EXPLAIN EXTENDED output parsing
│   ├── notebooks.py       # Jupyter / Databricks notebook parsing
│   └── antipatterns.py    # AntiPattern dataclass + Rust bridge
├── display/
│   ├── terminal.py        # Rich table output
│   ├── notebook.py        # HTML output
│   └── export.py          # JSON, Markdown, SARIF 2.1.0 export
├── cli/
│   └── main.py            # check, rules, init, doctor, cache
└── databricks/            # pip install burnt[databricks]
    ├── backend.py         # DatabricksBackend (system tables, DESCRIBE DETAIL)
    ├── pricing/           # DBU rates, dollar estimates
    └── cli.py             # Databricks-specific CLI commands
```

---

## 16. Python API

```python
import burnt

# ── Session (optional — enables runtime enrichment) ──
burnt.start_session()    # requires active SparkSession + pip install burnt[spark]

# ... run your Spark code ...

# ── Check ───────────────────────────────────────────
result = burnt.check()                 # current notebook / working directory
result = burnt.check("./pipeline.py")  # specific file or directory

result.display()          # Rich table (terminal) or HTML (notebook)
result.findings           # list[Finding], sorted by cost impact
result.graph              # CostGraph
result.compute_seconds    # total observed compute time (None if no session)
result.to_json()          # dict
result.to_markdown()      # str
result.to_sarif()         # SARIF 2.1.0 dict
```

---

## 17. Design Principles

1. **Databricks-first.** Lint rules work without credentials. Cost intelligence requires Databricks. No pretending otherwise.
2. **CLI-first.** `burnt check` is the product. The Python API is a second mode.
3. **Full notebook hygiene.** Cost + style + structure rules. No artificial scope limit.
4. **Static + runtime.** Static rules always run. sparkMeasure enriches when a session is active.
5. **Compute seconds over dollars.** Actionable everywhere. Backends map to USD.
6. **Honest confidence.** Mark observed data differently from estimates.
7. **Graceful degradation.** No creds → 84 rules. Spark session → enrichment. Databricks → dollar estimates. Never a hard failure.

---

## 18. Phases

| Phase | Status | Deliverable |
|-------|--------|-------------|
| P0 Base Rework | done | Cleanup, new package structure |
| P1 Rust Engine | done | tree-sitter, CostGraph, 84 rules, PyO3 bridge |
| PX Design Alignment | **in progress** | Dead code removal, sparkMeasure session, docs |
| P2 CLI Completion | todo | Rewire `check` to `_check.run()`, SARIF, event log |
| P3 Databricks Module | todo | `burnt[databricks]`: dollar estimates, system tables |
| P4 CI Integration | todo | Pre-commit, GitHub Actions, DABs examples |
| P5 Hardening | todo | E2E tests, error audit, packaging |
| P6 Validation | todo | Dogfood, security audit, ship v0.2.0 |
