# tasks/ — Phase-Based Task Queue

This directory is the **handoff protocol** between the Planner agent and the Executor agent.

---

## Roadmap

```
Phase 0: Base Rework ──────────────── Cleanup, new architecture setup [done]
Phase 1: Rust Engine ──────────────── tree-sitter, CostGraph, 43 rules [done]
Phase 2: Design Alignment ─────────── Dead code removal, REST session client, docs [in-progress]
Phase 3: Modular Architecture ─────── Pricing backends, pyproject restructure [in-progress]
Phase 4: Session & Intelligence ───── Cost estimation, EXPLAIN enrichment [todo, unblocked after P2]
Phase 5: CLI Completion ───────────── Rewire check, SARIF output, event log [todo]
Phase 6: Databricks Backend ────────── PricingBackend impls, dollar estimates [todo]
Phase 7: Integration & Hardening ──── E2E tests, CI examples, packaging [todo]
Phase 8: Validation ───────────────── Dogfood, security audit, ship v0.2.0 [todo]
```

> **Strategic Position (April 2026):**
> - **Spark-generic** — 43 active lint rules + CostGraph work on any Spark code (EMR, Glue, Dataproc, on-prem, Databricks); cost-in-dollars requires a pricing-backend extra (rule count reconciliation: P3-06)
> - **CLI-first** — `burnt check` is the product; notebook API is a second mode
> - **REST API runtime** — `start_session()` uses the Spark monitoring REST API; no sparkMeasure dependency
> - **Full notebook hygiene** — cost + style + structure rules ("ruff for Databricks notebooks, and beyond")

---

## Phase 2: Design Alignment *(do these first)*

| Task | Status | What |
|------|--------|------|
| `P2/01a-cli-surface-cleanup` | done | Remove advise, tutorial CLI commands; fix graph/estimate.py stub |
| `P2/01b-subtree-removal` | done | Delete watch/, _watch/, alerts/, intelligence/, templates/burnt_monitor.py, run_ts.rs |
| `P2/02-rest-session-client` | todo | Implement REST API session client (replaces sparkMeasure) |
| `P2/03-cli-rewire` | todo | Wire `burnt check` to `_check.run()` instead of old antipatterns path |
| `P2/04-sarif-output` | todo | Add SARIF 2.1.0 output format for GitHub Code Scanning |
| `P2/05-design-doc-update` | done | Update DESIGN.md, README.md, docs/ |
| `P2/06-tasks-cleanup` | done | Archive old tasks, rewrite thin tasks, update this README |
| `P2/07-public-api-cleanup` | done | Drop watch() from `__init__.py`, strip pre-pivot config() params |

---

## Phase 3: Modular Architecture

| Task | Status | What |
|------|--------|------|
| `P3/01-pyproject-extras-restructure` | done | Remove `[sql]`,`[spark]`,`[alerts]`; add `[databricks]`,`[*-databricks]`,`[onprem-spark]` |
| `P3/02-pricing-backend-protocol` | todo | Create `PricingBackend` protocol in `src/burnt/core/pricing.py`; stub `src/burnt/cloud/` dirs |
| `P3/04-cli-fix-diff-flags` | todo | Add `--fix`, `--unsafe-fixes`, `--diff <ref>` to `burnt check` (Rust engine, no libcst) |
| `P3/05-config-schema-additions` | todo | Add `[burnt.databricks.system_tables]` + `[burnt.pricing] backend` to config schema |
| `P3/06-rule-count-reconciliation` | todo | Make `burnt rules \| wc -l` match README; add TOML stubs for Tier 2/3 rules |
| `P3/07-vulture-ci-enforcement` | todo | Add vulture to CI to catch dead code re-introduction |
| `P3/09-rust-physical-plan-parser` | todo | Parse `/sql/{id}` physical plan JSON in Rust engine, annotate CostGraph nodes |

*(P3/03-notebook-extra-split was cancelled — [notebook] extra dropped; .dbc parsing is core; HTML output removed)*

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
| `P1/11-tier1-rules` | done | TOML-based pattern rules |
| `P1/12-tier2-rules` | done | Rust context-aware rules |
| `P1/13-tier3-rules` | done | Rust semantic/dataflow rules |
| `P1/14-rule-pipeline` | done | Rule execution and suppression |
| `P1/15-pyo3-bridge` | done | Expose engine to Python via PyO3 |
| `P1/16-parity-validation` | done | Ensure parity with v1.0 |
| `P1/17-better-rule-creation-system` | done | Improved rule creation framework |
| `P1/18-refactor-code-base` | done | Code refactoring and optimizations |
| `P1/19-rework-rules-to-use-cpl` | done | CPL pattern language integration |
| `P1/20-rework-burnt-engine` | done | Engine cleanup and optimization |

## Phase 4: Session & Intelligence

| Task | Status | What |
|------|--------|-------|
| `P4/01-pydantic-models` | done | Core models (CostEstimate, CheckResult, Finding) |
| `P4/02-env-detection` | done | Spark detection (not just Databricks) |
| `P4/03-spark-integration` | done | Session listener (superseded by P2-02 REST client) |
| `P4/04-rest-backend` | done | databricks-sdk moved to optional extra |
| `P4/06-delta-enrichment` | todo | DESCRIBE DETAIL via DatabricksBackend |
| `P4/08-explain-enrichment` | todo | EXPLAIN EXTENDED parsing and enrichment (blocked: P2-02) |
| `P4/09-scaling-functions` | done | 5 scaling models (Linear, Quadratic, etc.) |
| `P4/10-cost-estimation` | todo | Merge REST stage data with graph nodes (blocked: P2-02) |
| `P4/11-session-cost` | done | Session cost analysis (idle vs execution) |

*(P4/05-dabs-parser, P4/07-dlt-enrichment, P4/12-recommendations, P4/13-feedback-loop, P4/14-instance-catalog → archived)*

## Phase 5: CLI Completion

| Task | Status | What |
|------|--------|------|
| `P5/01-notebook-renderer` | done | Terminal Rich table output |
| `P5/02-terminal-renderer` | done | Rich table output for CLI |
| `P5/03-export` | done | JSON and Markdown export |
| `P5/04-check-wiring` | done | `burnt.check()` orchestrates Rust + runtime merge |
| `P5/05-config-system` | done | `burnt.toml` / `pyproject.toml` loading |
| `P5/06-cli-implementation` | todo | Rewire check, add SARIF + event-log (blocked: P2-01a, P2-03, P2-04) |
| `P5/07-graceful-degradation` | done | Static-only when Spark/Databricks unavailable |
| `P5/08-performance-tuning` | todo | Benchmark script, latency and memory targets (blocked: P5-06) |

## Phase 6: Databricks Backend

| Task | Status | What |
|------|--------|-------|
| `P6/00-onprem-spark-backend` | todo | user-supplied $/vCPU-hour in burnt.toml; zero cloud SDKs (blocked: P3-02, P3-05) |
| `P6/05-azure-databricks-backend` | todo | Azure DBU × VM SKU pricing (blocked: P6-00, P4-01) |

## Phase 7: Integration & Hardening

| Task | Status | What |
|------|--------|------|
| `P7/01-e2e-tests` | todo | Fixtures and E2E tests for full pipeline (blocked: P5-06, P2-02) |
| `P7/02-dynamic-sql` | todo | Variable resolution in SQL strings |
| `P7/03-error-handling-audit` | todo | Eliminate tracebacks on failure |
| `P7/05-config-validation` | todo | Catch invalid configs with clear errors |
| `P7/06-ci-examples` | todo | Pre-commit, GitHub Actions (SARIF + cost gate) (blocked: P2-04, P5-06) |
| `P7/07-packaging` | todo | Verify wheels work without databricks-sdk |
| `P7/08-documentation` | todo | CHANGELOG, docs/ site |

*(P7/04-access-level-tests → archived — old access-level model replaced by optional extras)*

## Phase 8: Validation

| Task | Status | What |
|------|--------|------|
| `P8/01-dogfood` | todo | Test on 5+ real-world notebooks |
| `P8/02-performance-validation` | todo | Profile latency and memory |
| `P8/03-security-audit` | todo | cargo audit, pip-audit |
| `P8/04-edge-case-testing` | todo | Empty, large, syntax-error notebooks |
| `P8/05-version-pins` | todo | Finalize Python dependency bounds |
| `P8/06-ship` | todo | Tag v0.2.0 and publish |

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

- `P3/03-notebook-extra-split` — `[notebook]` extra cancelled; .dbc parsing is core; HTML output removed
- `P4/05-dabs-parser` — Databricks Asset Bundle parsing is Databricks-only, out of scope
- `P4/07-dlt-enrichment` — DLT Pipelines API is Databricks-only, out of scope
- `P4/12-recommendations` — Replaced by simpler generic Spark advice
- `P4/13-feedback-loop` — Removed (temporal mismatch, telemetry burden)
- `P4/14-instance-catalog` — DBU pricing moved to optional Databricks module (P6)
- `P7/04-access-level-tests` — Old access-level model replaced by optional extras

## Archived Tasks

See `tasks/archive/` — pre-pivot Databricks watch features (tag attribution, idle cluster
detection, cost drift, job/pipeline reports, monitoring template). These completed the old
crystal-ball design and do not apply to the current architecture.
