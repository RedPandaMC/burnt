# tasks/ — Phase-Based Task Queue

This directory is the **handoff protocol** between the Planner agent and the Executor agent.

---

## Roadmap

```
Phase 0: Base Rework ─────────────── Cleanup, new architecture setup, test adaptation [todo]
Phase 1: Rust Engine ─────────────── tree-sitter, SQL/Python parsing, CostGraph, rules
Phase 2: Python Intelligence ─────── Enrichment, estimation, recommendations, feedback
Phase 3: Display & CLI ───────────── check(), CLI, notebook/terminal layouts
Phase 4: Monitoring & Alerts ─────── watch(), .alert(), drift/idle/tag monitoring
Phase 5: Integration & Hardening ─── E2E tests, dynamic SQL, error handling
Phase 6: Validation ──────────────── Dogfood, security audit, ship v2.0.0
```

### Phase 0: Base Rework

| Task | Status | What |
|------|--------|------|
| `P0/01-remove-unneeded-code` | todo | Cleanup old estimators, advisor, etc. |
| `P0/02-setup-new-package-structure` | todo | Create burnt-engine/ and scaffold src/burnt/ |
| `P0/03-adapt-existing-tests` | todo | Refactor current tests for new architecture |

### Phase 1: Rust Engine

| Task | Status | What |
|------|--------|------|
| `P1/01-cargo-setup` | todo | Basic Cargo and core Rust types |
| `P1/02-format-parsers` | todo | Databricks .py, .ipynb, and .sql formats |
| `P1/03-run-resolution` | todo | Handle %run directives in Rust engine |
| `P1/04-tree-sitter-python` | todo | tree-sitter for Python and SQL fragments |
| `P1/05-tree-sitter-sql` | todo | tree-sitter and sqlparser-rs for SQL |
| `P1/06-mode-detection` | todo | Detect if Python, SQL, or DLT mode |
| `P1/07-semantic-model` | todo | Scope, bindings, and call chains |
| `P1/08-python-cost-graph` | todo | Build CostGraph for Python code |
| `P1/09-sql-cost-graph` | todo | Build CostGraph for SQL statements |
| `P1/10-dlt-pipeline-graph` | todo | Build PipelineGraph for DLT |
| `P1/11-tier1-rules` | todo | ~48 TOML-based rules |
| `P1/12-tier2-rules` | todo | ~25 Rust context-aware rules |
| `P1/13-tier3-rules` | todo | ~11 Rust semantic rules |
| `P1/14-rule-pipeline` | todo | Rule execution and suppression |
| `P1/15-pyo3-bridge` | todo | Expose engine to Python via PyO3 |
| `P1/16-parity-validation` | todo | Ensure parity with v1.0 |

### Phase 2: Python Intelligence

| Task | Status | What |
|------|--------|------|
| `P2/01-pydantic-models` | todo | Define models for graphs and estimates |
| `P2/02-env-detection` | todo | Detect runtime env and access level |
| `P2/03-spark-integration` | todo | Extract data from active SparkSession |
| `P2/04-rest-backend` | todo | Connect via databricks-sdk outside Spark |
| `P2/05-dabs-parser` | todo | Parse databricks.yml for job context |
| `P2/06-delta-enrichment` | todo | Enrichment via DESCRIBE DETAIL/HISTORY |
| `P2/07-dlt-enrichment` | todo | Enrichment via Pipelines API |
| `P2/08-explain-enrichment` | todo | Enrichment via EXPLAIN COST |
| `P2/09-scaling-functions` | todo | 7 scaling models (Linear, Quadratic, etc.) |
| `P2/10-cost-estimation` | todo | Topological walk and cost summation |
| `P2/11-session-cost` | todo | Idle vs execution cost in notebooks |
| `P2/12-recommendations` | todo | SKU, Photon, and auto-term recs |
| `P2/13-feedback-loop` | todo | Calibrate coefficients from billing |
| `P2/14-instance-catalog` | todo | Pricing for Azure/Databricks SKUs |

### Phase 3: Display & CLI

| Task | Status | What |
|------|--------|------|
| `P3/01-notebook-renderer` | todo | HTML output for notebooks |
| `P3/02-terminal-renderer` | todo | Rich terminal output |
| `P3/03-export` | todo | JSON and Markdown export |
| `P3/04-check-wiring` | todo | Full burnt.check() orchestration |
| `P3/05-config-system` | todo | burnt.toml and pyproject.toml loading |
| `P3/06-cli-implementation` | todo | burnt check CLI command |
| `P3/07-graceful-degradation` | todo | Handling missing permissions/APIs |
| `P3/08-performance-tuning` | todo | Meet < 3s and < 50MB targets |

### Phase 4: Monitoring & Alerts

| Task | Status | What |
|------|--------|------|
| `P4/01-tag-attribution` | todo | Cost reporting by Databricks tags |
| `P4/02-idle-cluster-detection` | todo | Find and alert on wasted cost |
| `P4/03-cost-drift` | todo | Detect significant cost deviations |
| `P4/04-job-report` | todo | Historical cost trends for jobs |
| `P4/05-pipeline-report` | todo | Table-level cost trends for DLT |
| `P4/06-watch-orchestration` | todo | Full burnt.watch() orchestration |
| `P4/07-alert-dispatch` | todo | Slack, Teams, and Webhook alerts |
| `P4/08-monitoring-template` | todo | Deployable monitoring notebook |

### Phase 5: Integration & Hardening

| Task | Status | What |
|------|--------|------|
| `P5/01-e2e-tests` | todo | 6 fixtures through full pipeline |
| `P5/02-dynamic-sql` | todo | Variable resolution in SQL strings |
| `P5/03-error-handling-audit` | todo | Eliminate tracebacks on failure |
| `P5/04-access-level-tests` | todo | Verify 4 access level behaviors |
| `P5/05-config-validation` | todo | Catch invalid configs with clear errors |
| `P5/06-ci-examples` | todo | GitHub Actions and Azure DevOps YAML |
| `P5/07-packaging` | todo | Wheel building and platform testing |
| `P5/08-documentation` | todo | README, docs, and CHANGELOG finalization |

### Phase 6: Validation

| Task | Status | What |
|------|--------|------|
| `P6/01-dogfood` | todo | Test on 5+ real-world notebooks |
| `P6/02-performance-validation` | todo | Production performance verification |
| `P6/03-security-audit` | todo | Dependency and code security scan |
| `P6/04-edge-case-testing` | todo | Empty, nested, and circular test cases |
| `P6/05-version-pins` | todo | Finalize dependencies and DBR versions |
| `P6/06-ship` | todo | Tag v2.0.0 and publish |

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

## Archived Tasks

Sprint-based tasks (S1-S5) have been superseded by the Phase-based restructure (P0-P6) as the project moves to its v2.0 architecture.
