# Task: R5 - Billing Attribution Accuracy at Concurrency

---

## Metadata

```yaml
id: r5-billing-attribution-concurrency
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

Measure the skew in billing attribution when multiple queries or notebooks run concurrently on All-Purpose clusters. This tests the limits of proportional DBU allocation based on query duration and cluster utilization.

### Files to Read

```
DESIGN.md § "Research Backlog"
src/burnt/tables/attribution.py
```

### Action Items

1. Run single isolated queries and record baseline cost attribution.
2. Run identical queries concurrently on a single cluster.
3. Compare the fractional DBU calculation methodology against the aggregate cluster cost.
4. Measure the variance (attribution skew) to decide if we need a concurrent concurrency weighting factor.

---

## Acceptance Criteria

- [ ] Execute concurrency tests on an All-Purpose cluster.
- [ ] Measure attribution skew against isolated runs.
- [ ] Update `tables/attribution.py` and `estimators/` logic to handle concurrency skew if necessary.

---

## Handoff

```yaml
status: todo
```