# Task: R3 - AQE Plan Divergence Rate

---

## Metadata

```yaml
id: r3-aqe-plan-divergence
status: todo
sprint: 0
priority: low
agent: ~
blocked_by: []
created_by: planner
```

---

## Context

### Goal

Understand how often Adaptive Query Execution (AQE) fundamentally alters the physical plan during runtime compared to the statically estimated logical plan. This research helps calibrate expected error rates for complex queries.

### Files to Read

```
DESIGN.md § "Research Backlog"
```

### Action Items

1. Identify queries known to trigger AQE interventions (e.g., dynamically handling skew, converting sort-merge joins to broadcast joins).
2. Compare the initial `EXPLAIN` plan to the final physical plan logged in the Spark UI / SQL history.
3. Calculate the frequency of major divergence (where performance characteristics fundamentally shift).
4. Document findings to influence heuristic confidence adjustments.

---

## Acceptance Criteria

- [ ] Execute tests triggering AQE and capture before/after plans.
- [ ] Determine the rate and impact of plan divergence.
- [ ] Document the findings.

---

## Handoff

```yaml
status: todo
```