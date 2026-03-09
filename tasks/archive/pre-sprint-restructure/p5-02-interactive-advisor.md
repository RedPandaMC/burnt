# Task: The Interactive-to-Production Advisor

---

## Metadata

```yaml
id: p5-02-interactive-advisor
status: todo
phase: 5
priority: high
agent: ~
blocked_by: [p5-01-top-level-api, p4a-03-table-registry-runtime-backend]
created_by: planner
```

---

## Context

### Goal

Build the "Developer's Best Friend" feature: `burnt advise`. Instead of trying to guess PySpark execution times statically, fetch the actual execution metrics from a recent interactive test run (via `system.query.history` or `lakeflow`) and run them through our `WhatIf` engine to recommend optimal production Job cluster configurations and calculate the savings.

### Files to read

```
# Required
src/burnt/estimators/whatif.py
src/burnt/tables/queries.py
src/burnt/cli/main.py
```

### Background

When a developer tests a notebook interactively, they typically run it on an All-Purpose cluster ($0.55/DBU). When orchestrated, it should run on Jobs Compute ($0.30/DBU) with an optimized worker count. This feature translates their test run into a production recommendation.

---

## Acceptance Criteria

- [ ] Fetch the metrics (duration, read_bytes, spill, max_memory) for a specific `run_id` or `statement_id` from Databricks system tables.
- [ ] Feed those metrics into the `whatif` engine to calculate the cost of running that exact workload on 1) Jobs Compute, 2) Serverless, and 3) Spot Instances.
- [ ] Identify if peak memory utilization is low enough to recommend a smaller instance type.
- [ ] CLI: Implement `burnt advise --run-id <id>` to print the Compute Migration Analysis table (as defined in `docs/cli-workflows.md`).
- [ ] Programmatic: Ensure `burnt.advise(run_id="...")` returns the structured `AdvisoryReport`.

---

## Verification

### Commands

```bash
uv run pytest -m unit -v
uv run ruff check src/ tests/
```

### Integration Check

- [ ] Run `burnt advise --run-id TEST12345` (mocking the databricks connection) and verify it prints the Compute Migration Analysis table and recommendations.

---

## Handoff

### Result

```yaml
status: todo
```
