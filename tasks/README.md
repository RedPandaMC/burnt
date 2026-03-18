# tasks/ — Sprint-Based Task Queue

This directory is the **handoff protocol** between the Planner agent and the Executor agent.

---

## Sprint Roadmap

```
Sprint 1: The Core Loop ─────────── Get advise_current_session() working  [complete]
Sprint 2: The Developer Experience ─ Lint rules, benchmarks, CLI/API redesign  [in-progress]
Sprint 3: Estimation Accuracy ────── Wire all 4 tiers, total cost (DBU + VM)
Sprint 4: Production Hardening ───── Error handling, caching, observability
Sprint 5: ML & Forecasting ───────── Feature extraction, classification, Prophet
```

### Sprint 2: The Developer Experience

| Task | Status | What | Blocked By |
|------|--------|------|------------|
| `s2-03-benchmark-dataset` | done | 5 reference queries, monotonicity + Hypothesis tests, integration fixtures | — |
| `s2-04-ast-lint-rules` | in-progress | 7 of 12 rules done; 5 missing + severity fixes + AST migration needed | — |
| `s2-05a-cli-api-redesign` | todo | CLI check/init/tutorial/cache/rules; remove estimate/advise/whatif; rename WhatIf→Simulation | — |
| `s2-05b-simulate-api` | superseded | Merged into s2-05a | — |
| `s2-07-cost-guard` | done | `raise_if_exceeds()` budget guard with currency conversion | — |
| `s2-08-doctor-command` | done | `burnt doctor` diagnostic command | — |
| `s2-09-cluster-config-enrichment` | done | `ClusterConfig.from_databricks_json()`, `ClusterProfile`, default currency system | — |
| `s2-10-offline-mode-fix` | done | Suppress dollar amounts in offline mode; DBU→cost for connected mode; inline SQL in `check` | — |

### Sprint 3: Estimation Accuracy

| Task | Status | What | Blocked By |
|------|--------|------|------------|
| `s3-01-delta-scan-integration` | todo | DESCRIBE DETAIL → scan size enrichment | s1-01 |
| `s3-02-fingerprint-lookup` | todo | Historical query matching → p50/p95 | s1-01, s3-01 |
| `s3-03-pipeline-hardening` | todo | Total cost (DBU+VM), confidence calibration | s3-01, s3-02 |

### Sprint 4: Production Hardening

| Task | Status | What | Blocked By |
|------|--------|------|------------|
| `s4-01-error-handling` | todo | Typed exceptions, retry, graceful failures | s3-03 |
| `s4-02-caching` | todo | TTL cache, connection pooling | s4-01 |
| `s4-03-observability` | todo | Structured logging, --debug, timing metrics | s4-01 |

### Sprint 5: ML & Forecasting

| Task | Status | What | Blocked By |
|------|--------|------|------------|
| `s5-01-feature-extraction` | todo | ExplainPlan + Delta + cluster → feature vector | s3-03, s2-03 |
| `s5-02-classification-model` | todo | Cost bucket classifier (scikit-learn) | s5-01 |
| `s5-03-prophet-forecasting` | todo | Per-SKU time-series cost projection | s5-01 |

---

## Dependency Graph

```
[Sprint 1 Complete]
       │
s2-03 (Benchmarks) ──done──┐
s2-04 (Lint Rules) ─partial─┤──→ [Sprint 2 Complete]
s2-05a (CLI/API) ───todo────┘
       │
s3-01 (Delta) ──→ s3-02 (Fingerprint) ──→ s3-03 (Pipeline) ──→ [Sprint 3]
                                                 │
                                    s4-01 (Errors) ──→ s4-02 (Cache)
                                                 │──→ s4-03 (Observability)
                                                 │
                                    s5-01 (Features) ──→ s5-02 (ML)
                                                    ──→ s5-03 (Prophet)
```

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

## Status Values

| Status | Meaning |
|--------|---------|
| `todo` | Ready to pick up |
| `in-progress` | Claimed by the executor |
| `done` | Implemented, tested, and archived |
| `blocked` | Cannot proceed (see `blocked_reason`) |

## Parallel Execution Rules

1. Check `blocked_by` before starting
2. One task at a time per agent
3. If two tasks touch the same file, the second must list `blocked_by`

## Archived Tasks

Pre-sprint-restructure tasks are in `tasks/archive/pre-sprint-restructure/`. These were the old `p4a-*` through `p9-*` phase-based tasks that have been superseded by the sprint structure.
