# tasks/ — Phase-Based Task Queue

This directory is the **handoff protocol** between the Planner agent and the Executor agent.

---

## Roadmap

```
Phase 0: Base Rework ──────────────── Cleanup, new architecture setup [done]
Phase 1: Rust Engine ──────────────── tree-sitter, CostGraph, 84 rules [done]
Phase X: Design Alignment ─────────── Dead code removal, docs, sparkMeasure session [in-progress]
Phase 2: Session & Intelligence ───── Cost estimation, EXPLAIN enrichment [todo, unblocked after PX]
Phase 3: CLI Completion ───────────── Rewire check, SARIF output, event log [todo]
Phase 4: Databricks Module ────────── DatabricksBackend, dollar estimates, Delta enrichment [todo]
Phase 5: Integration & Hardening ──── E2E tests, CI examples, packaging [todo]
Phase 6: Validation ───────────────── Dogfood, security audit, ship v0.2.0 [todo]
```

> **Strategic Position (April 2026):**
> - **Databricks-first** — lint rules work without credentials; cost intelligence requires Databricks
> - **CLI-first** — `burnt check` is the product; notebook API is a second mode
> - **Full notebook hygiene** — cost + style + structure rules ("ruff for Databricks notebooks")
> - **sparkMeasure** replaces the broken SparkListener/statusTracker session implementation

---

## Phase X: Design Alignment *(do these first)*

| Task | Status | What |
|------|--------|------|
| `PX/01-remove-dead-code` | todo | Remove advise, tutorial, estimate, simulate — pre-pivot debris |
| `PX/02-sparkmeasure-session` | todo | Replace broken SparkListener with sparkMeasure |
| `PX/03-cli-rewire` | todo | Wire `burnt check` to `_check.run()` instead of old antipatterns path |
| `PX/04-sarif-output` | todo | Add SARIF 2.1.0 output format for GitHub Code Scanning |
| `PX/05-design-doc-update` | done | Update DESIGN.md, AGENTS.md, README.md, pyproject.toml |
| `PX/06-tasks-cleanup` | done | Archive old P4 tasks, rewrite thin tasks, update this README |

---

## Phase 0: Base Rework

| Task | Status | What |
|------|--------|------|
| `P0/01-remove-unneeded-code` | done | Cleanup old estimators, advisor, etc. |
| `P0/02-setup-new-package-structure` | done | Create burnt-engine/ and scaffold src/burnt/ |
| `P0/03-adapt-existing-tests` | done | Refactor current tests for new architecture |

## Phase 1: Rust Engine

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

## Phase 2: Session & Intelligence

| Task | Status | What |
|------|--------|-------|
| `P2/01-pydantic-models` | done | Core models (CostEstimate, CheckResult, Finding) |
| `P2/02-env-detection` | done | Spark detection (not just Databricks) |
| `P2/03-spark-integration` | done | Session listener (⚠ being replaced by PX/02) |
| `P2/04-rest-backend` | done | databricks-sdk moved to optional extra |
| `P2/05-dabs-parser` | cancelled | Databricks-only, not core |
| `P2/06-delta-enrichment` | todo | DESCRIBE DETAIL via DatabricksBackend |
| `P2/07-dlt-enrichment` | cancelled | DLT Pipelines API — Databricks-only |
| `P2/08-explain-enrichment` | todo | EXPLAIN EXTENDED parsing and enrichment |
| `P2/09-scaling-functions` | done | 5 scaling models (Linear, Quadratic, etc.) |
| `P2/10-cost-estimation` | todo | Merge sparkMeasure stage data with graph nodes |
| `P2/11-session-cost` | done | Session cost analysis (idle vs execution) |
| `P2/12-recommendations` | cancelled | Replaced by generic Spark advice |
| `P2/13-feedback-loop` | cancelled | Removed (bad design choice) |
| `P2/14-instance-catalog` | cancelled | DBU pricing moved to burnt[databricks] |

## Phase 3: CLI Completion

| Task | Status | What |
|------|--------|------|
| `P3/01-notebook-renderer` | done | HTML output for Jupyter/Databricks notebooks |
| `P3/02-terminal-renderer` | done | Rich table output for CLI |
| `P3/03-export` | done | JSON and Markdown export |
| `P3/04-check-wiring` | done | `burnt.check()` orchestrates Rust + runtime merge |
| `P3/05-config-system` | done | `burnt.toml` / `pyproject.toml` loading |
| `P3/06-cli-implementation` | todo | Rewire check, add SARIF + event-log, remove dead commands |
| `P3/07-graceful-degradation` | done | Static-only when Spark/Databricks unavailable |
| `P3/08-performance-tuning` | todo | Benchmark script, latency and memory targets |

## Phase 4: Databricks Module

| Task | Status | What |
|------|--------|-------|
| `P4/01-databricks-namespace` | todo | Consolidate Databricks code under `burnt/databricks/` |
| `P4/02-rest-backend-move` | todo | Move RestBackend to `burnt/databricks/runtime/` |
| `P4/03-databricks-cli` | todo | Databricks-specific CLI commands |
| `P4/06-delta-enrichment` | todo | DESCRIBE DETAIL via DatabricksBackend |
| `P4/07-dlt-analysis` | todo | PipelineGraph enrichment for DLT |

## Phase 5: Integration & Hardening

| Task | Status | What |
|------|--------|------|
| `P5/01-e2e-tests` | todo | Fixtures and E2E tests for full pipeline |
| `P5/02-dynamic-sql` | todo | Variable resolution in SQL strings |
| `P5/03-error-handling-audit` | todo | Eliminate tracebacks on failure |
| `P5/05-config-validation` | todo | Catch invalid configs with clear errors |
| `P5/06-ci-examples` | todo | Pre-commit, GitHub Actions (SARIF + cost gate), DABs |
| `P5/07-packaging` | todo | Verify wheels work without databricks-sdk |
| `P5/08-documentation` | todo | CHANGELOG, docs/ site |

## Phase 6: Validation

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
Planner  → creates task file (status: todo, with acceptance criteria checkboxes)
           ↓
Executor → claims (status: in-progress, agent: <model-id>)
         → implements code, runs tests/lint, validates
         → checks off acceptance criteria
         → updates task file (status: done, completed_by: <model-id>)
         → updates this README — marks row as done
         → renames task file to <id>.md.completed
```

## Cancelled Tasks

- `P2/05-dabs-parser` — Databricks Asset Bundle parsing is Databricks-only
- `P2/07-dlt-enrichment` — DLT Pipelines API is Databricks-only
- `P2/12-recommendations` — Replaced by simpler generic Spark advice
- `P2/13-feedback-loop` — Removed (temporal mismatch, telemetry burden)
- `P2/14-instance-catalog` — DBU pricing moved to optional Databricks module
- `P5/04-access-level-tests` — Old access-level model replaced by optional extras

## Archived Tasks

See `tasks/archive/` — pre-pivot Databricks watch features (tag attribution, idle cluster detection, cost drift, job/pipeline reports, monitoring template). These completed the old crystal-ball design and do not apply to the current architecture.
