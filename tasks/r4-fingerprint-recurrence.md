# Task: R4 - Fingerprint Recurrence Rates

---

## Metadata

```yaml
id: r4-fingerprint-recurrence
status: todo
sprint: 0
priority: normal
agent: ~
blocked_by: []
created_by: planner
```

---

## Context

### Goal

Determine the "cache-hit" rate for historical queries. We normalize query SQL (e.g., stripping whitespace, replacing literals with `?`) and hash it to lookup historical executions. We need to verify how often this yields matches in a real enterprise environment.

### Files to Read

```
DESIGN.md § "Research Backlog"
src/burnt/tables/queries.py
```

### Action Items

1. Ingest a substantial sample of query history (either from `system.query.history` or an anonymized log).
2. Apply our normalization and SHA-256 fingerprinting pipeline.
3. Measure the percentage of unique queries that have run multiple times (recurrence rate).
4. Evaluate the standard deviation of execution times/costs for recurring fingerprints to validate our p50/p95 metric approach.

---

## Acceptance Criteria

- [ ] Fingerprint recurrence rate analyzed on a dataset.
- [ ] Variance among matching fingerprints recorded.
- [ ] Findings used to calibrate the historical fingerprinting estimator weights.

---

## Handoff

```yaml
status: todo
```