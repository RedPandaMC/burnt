# tasks/ — Sprint-Based Task Queue

This directory is the **handoff protocol** between Planner agents and Executor agents.

---

## Sprint Roadmap

```
Sprint 1: The Core Loop ─────────── Get advise_current_session() working
Sprint 2: The Developer Experience ─ Fluent what-if, right-sizing JSON, bug fixes
Sprint 3: Estimation Accuracy ────── Wire all 4 tiers, total cost (DBU + VM)
Sprint 4: Production Hardening ───── Error handling, caching, observability
Sprint 5: ML & Forecasting ───────── Feature extraction, classification, Prophet
```

### Sprint 1: The Core Loop

| Task | Status | What | Blocked By |
|------|--------|------|------------|
| `s2-02-remaining-bugs` | done | Fix 39+ bugs across codebase | — |
| `s2-03-benchmark-dataset` | todo | TPC-DS queries + known costs for validation | — |

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
s1-01 (RuntimeBackend) ──┬──→ s1-03 (Advisor) ──→ [Sprint 1 Complete]
s1-02 (Instance Catalog) ┘         │
       │                           │
       └──→ s2-01 (WhatIfBuilder)  │
                                   │
s2-02 (Bug fixes) ─────────────────┤
s2-03 (Benchmarks) ────────────────┤──→ [Sprint 2 Complete]
                                   │
s1-01 ──→ s3-01 (Delta) ──→ s3-02 (Fingerprint) ──→ s3-03 (Pipeline) ──→ [Sprint 3]
                                                           │
                                              s4-01 (Errors) ──→ s4-02 (Cache)
                                                           │──→ s4-03 (Observability)
                                                           │
                                              s5-01 (Features) ──→ s5-02 (ML)
                                                              ──→ s5-03 (Prophet)
```

**Critical path to flagship feature:**
```
s1-01 + s1-02 (parallel) → s1-03 = advise_current_session() working
```

Only 2 tasks (parallel) before the core feature is unblocked. Compare to old plan: 5 serial tasks.

---

## How It Works

```
Planner → creates task file (status: todo)
         ↓
Executor → claims (status: in-progress, agent: <name>)
         → implements code, runs tests/lint
         → writes handoff notes (status: validation-pending)
         ↓
Validator → runs benchmarks, checks formulas
          → archives to tasks/archive/ (status: done)
```

## Status Values

| Status | Meaning |
|--------|---------|
| `todo` | Ready to pick up |
| `in-progress` | Claimed by an executor |
| `validation-pending` | Executor finished, awaiting validation |
| `done` | Validated and archived |
| `blocked` | Cannot proceed (see `blocked_reason`) |

## Parallel Execution Rules

1. Check `blocked_by` before starting
2. One task at a time per agent
3. If two tasks touch the same file, the second must list `blocked_by`

## Archived Tasks

Pre-sprint-restructure tasks are in `tasks/archive/pre-sprint-restructure/`. These were the old `p4a-*` through `p9-*` phase-based tasks that have been superseded by the sprint structure.
