# Task: R2 - EXPLAIN COST Accuracy Benchmarking

---

## Metadata

```yaml
id: r2-explain-cost-accuracy
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

Measure the accuracy of Databricks' `EXPLAIN COST` output versus actual execution metrics. This helps in understanding the error multipliers, which will be used to generate confidence scores in the hybrid estimator pipeline.

### Files to Read

```
DESIGN.md § "Research Backlog"
src/burnt/estimators/hybrid.py
```

### Action Items

1. Run a sample set of queries from the benchmark dataset.
2. Capture the `EXPLAIN COST` estimates (sizeInBytes, rowCount) for these queries.
3. Compare these estimates against actual execution metrics from `system.query.history`.
4. Measure the difference with and without `ANALYZE TABLE` (statistics gathering) run beforehand.
5. Feed these error multipliers into the confidence scoring weighting in `hybrid.py`.

---

## Acceptance Criteria

- [ ] Data collected comparing EXPLAIN COST size/rows against actuals.
- [ ] Accuracy multipliers and standard deviations documented.
- [ ] Confidence bounds adjusted in `src/burnt/estimators/hybrid.py` or the pipeline orchestrator.

---

## Handoff

```yaml
status: todo
```