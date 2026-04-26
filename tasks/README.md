# tasks/ — Phase-Based Task Queue

This directory is the **handoff protocol** between the Planner agent and the Executor agent.

---

## Roadmap

```
Phase 0: Base Rework ─────────────── Cleanup, new architecture setup, test adaptation [done]
Phase 1: Rust Engine ─────────────── tree-sitter, SQL/Python parsing, CostGraph, rules [done]
Phase 2: Session & Intelligence ──── Spark listener, hybrid check, display, config [in-progress]
Phase 3: Optional Databricks ─────── watch(), DatabricksBackend, DBU pricing [todo]
Phase 4: Integration & Hardening ─── E2E tests, error handling, CI/ packaging [todo]
Phase 5: Validation ──────────────── Dogfood, security audit, ship v0.2.0 [todo]
```

> **Architecture Pivot (April 2026):** Redesigned from "pre-execution cost estimation" (crystal ball) to "post-development performance coach" (practice-run reviewer). See DESIGN.md §1-2.
> - Spark-first, not Databricks-first
> - Compute seconds over dollars
> - `burnt.start_session()` → run code → `burnt.check()` workflow

---

### Phase 0: Base Rework

| Task | Status | What |
|------|--------|------|
| `P0/01-remove-unneeded-code` | done | Cleanup old estimators, advisor, etc. |
| `P0/02-setup-new-package-structure` | done | Create burnt-engine/ and scaffold src/burnt/ |
| `P0/03-adapt-existing-tests` | done | Refactor current tests for new architecture |

### Phase 1: Rust Engine

| Task | Status | What |
|------|--------|-------|
| `P1/01-cargo-setup` | done | Basic Cargo and core Rust types |
| `P1/02-format-parsers` | done | .py, .ipynb, and .sql formats |
| `P1/03-run-resolution` | done | Handle %run directives in Rust engine |
| `P1/04-tree-sitter-python` | done | tree-sitter for Python and SQL fragments |
| `P1/05-tree-sitter-sql` | done | tree-sitter and sqlparser-rs for SQL |
| `P1/06-mode-detection` | done | Detect if Python, SQL, or DLT/SDP mode |
| `P1/07-semantic-model` | done | Scope, bindings, and call chains |
| `P1/08-python-cost-graph` | done | Build CostGraph for Python code |
| `P1/09-sql-cost-graph` | done | Build CostGraph for SQL statements |
| `P1/10-dlt-pipeline-graph` | done | Build PipelineGraph for DLT/SDP |
| `P1/11-tier1-rules` | done | ~48 TOML-based rules |
| `P1/12-tier2-rules` | done | ~25 Rust context-aware rules |
| `P1/13-tier3-rules` | done | ~11 Rust semantic rules |
| `P1/14-rule-pipeline` | done | Rule execution and suppression |
| `P1/15-pyo3-bridge` | done | Expose engine to Python via PyO3 |
| `P1/16-parity-validation` | done | Ensure parity with v1.0 |
| `P1/17-better-rule-creation-system` | done | Improved rule creation framework |
| `P1/18-refactor-code-base` | done | Code refactoring and optimizations |
| `P1/19-rework-rules-to-use-cpl` | done | CPL pattern language integration |
| `P1/20-rework-burnt-engine` | done | Engine cleanup and optimization |

### Phase 2: Session & Intelligence

| Task | Status | What |
|------|--------|-------|
| `P2/01-pydantic-models` | done | Core models (CostEstimate, CheckResult, Finding) |
| `P2/02-env-detection` | done | Generic Spark detection (not just Databricks) |
| `P2/03-spark-integration` | done | SparkListener for stage metrics, SQL executions |
| `P2/04-rest-backend` | done | `databricks-sdk` moved to optional extra |
| `P2/05-dabs-parser` | cancelled | Databricks Asset Bundle — Databricks-only, not core |
| `P2/06-delta-enrichment` | todo | Move to `burnt[databricks]` optional module |
| `P2/07-dlt-enrichment` | cancelled | DLT Pipelines API — Databricks-only |
| `P2/08-explain-enrichment` | todo | Verify standard Spark EXPLAIN works |
| `P2/09-scaling-functions` | done | 5 scaling models (Linear, Quadratic, etc.) |
| `P2/10-cost-estimation` | todo | Merge runtime metrics with graph nodes |
| `P2/11-session-cost` | done | Session cost analysis (idle vs execution) |
| `P2/12-recommendations` | cancelled | Replaced by generic Spark advice patterns |
| `P2/13-feedback-loop` | cancelled | Calibrate removed from roadmap (bad design choice) |
| `P2/14-instance-catalog` | cancelled | DBU pricing moved to `burnt[databricks]` |

### Phase 3: Display & CLI

| Task | Status | What |
|------|--------|------|
| `P3/01-notebook-renderer` | done | HTML output for Jupyter/Databricks notebooks |
| `P3/02-terminal-renderer` | done | Rich table output for CLI |
| `P3/03-export` | done | JSON and Markdown export |
| `P3/04-check-wiring` | done | `burnt.check()` orchestrates Rust + runtime merge |
| `P3/05-config-system` | done | `burnt.toml` / `pyproject.toml` loading |
| `P3/06-cli-implementation` | todo | Update `burnt check` for new architecture |
| `P3/07-graceful-degradation` | done | Static-only when Spark/Databricks unavailable |
| `P3/08-performance-tuning` | todo | Profile and optimize latency/memory |

### Phase 4: Optional Databricks Module

| Task | Status | What |
|------|--------|-------|
| `P4/01-databricks-namespace` | todo | Move watch/, tables/, pricing/ to `burnt/databricks/` |
| `P4/02-rest-backend-move` | todo | Move `RestBackend` to `burnt/databricks/runtime/` |
| `P4/03-databricks-cli` | todo | `burnt advise`, `burnt doctor` in optional module |
| `P4/04-watch-orchestration` | done | `burnt.watch()` (moved to optional module) |
| `P4/05-alert-dispatch` | done | Slack, Teams, webhook alerts (optional) |
| `P4/06-delta-enrichment` | todo | DESCRIBE DETAIL via DatabricksBackend |
| `P4/07-dlt-analysis` | todo | PipelineGraph enrichment for DLT |
| `P4/08-monitoring-template` | done | Deployable monitoring notebook |

### Phase 5: Integration & Hardening

| Task | Status | What |
|------|--------|------|
| `P5/01-e2e-tests` | todo | Fixtures for static + hybrid analysis |
| `P5/02-dynamic-sql` | todo | Variable resolution in SQL strings |
| `P5/03-error-handling-audit` | todo | Eliminate tracebacks on failure |
| `P5/04-access-level-tests` | cancelled | Old model replaced by optional extras |
| `P5/05-config-validation` | todo | Catch invalid configs with clear errors |
| `P5/06-ci-examples` | todo | GitHub Actions for `burnt check` |
| `P5/07-packaging` | todo | Verify wheels work without databricks-sdk |
| `P5/08-documentation` | todo | README, docs, CHANGELOG |

### Phase 6: Validation

| Task | Status | What |
|------|--------|------|
| `P6/01-dogfood` | todo | Test on 5+ real-world notebooks |
| `P6/02-performance-validation` | todo | Profile latency and memory |
| `P6/03-security-audit` | todo | cargo audit, pip-audit |
| `P6/04-edge-case-testing` | todo | Empty, large, syntax-error notebooks |
| `P6/05-version-pins` | todo | Finalize Python dependency bounds |
| `P6/06-ship` | todo | Tag v0.2.0 and publish |

---

## How It Works

```
Planner  → creates task file (status: todo)
           ↓
Executor → claims (status: in-progress, agent: <name>)
         → implements code, runs tests/lint, validates
         → updates task file (status: done, checks off criteria)
         → updates this README — marks row as done in the sprint table
         → renames task file to <id>.md.completed
```

## Cancelled Tasks

The following tasks were cancelled due to architecture redesign:
- `P2/05-dabs-parser` — Databricks Asset Bundle parsing is Databricks-only
- `P2/07-dlt-enrichment` — DLT Pipelines API is Databricks-only
- `P2/12-recommendations` — Replaced by simpler generic Spark advice
- `P2/13-feedback-loop` — Removed due to temporal mismatch and telemetry burden
- `P2/14-instance-catalog` — DBU pricing moved to optional Databricks module
- `P5/04-access-level-tests` — Old access-level model replaced by optional extras

## Archived Tasks

Sprint-based tasks (S1-S5) have been superseded by the Phase-based restructure (P0-P6).
