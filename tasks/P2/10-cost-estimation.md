```yaml
id: P2-10-cost-estimation
status: todo
phase: 2
priority: high
agent: ~
blocked_by: [PX-02-sparkmeasure-session]
created_by: planner
```

## Context

### Goal

Implement `graph/estimate.py` to merge sparkMeasure stage data with Rust-built graph nodes, producing per-node `actual_compute_seconds` for observed operations and scaling-function estimates for unobserved ones.

### Files to read

```
# Required
src/burnt/graph/estimate.py       (currently raises NotImplementedError)
src/burnt/graph/model.py          (CostNode, CostGraph)
src/burnt/graph/scaling.py        (scaling functions)
src/burnt/_check/__init__.py      (_merge_runtime)
src/burnt/_session.py             (SessionState shape after PX-02)

# Reference
DESIGN.md §6 (Cost Graph), §8 (Estimation)
tasks/PX/02-sparkmeasure-session.md
```

### Background

After sparkMeasure collection, `SessionState.collected` contains a list of dicts with one entry per completed stage:

```python
{
    "stageId": 3,
    "name": "crossJoin at notebook.py:42",   # contains source location
    "executorRunTime": 84300,                  # milliseconds
    "shuffleReadBytes": 53687091200,           # bytes
    "shuffleWriteBytes": 26843545600,
    "memoryBytesSpilled": 0,
    "diskBytesSpilled": 1073741824,
    "inputBytes": 4509715456,
}
```

**Correlation algorithm:** match `stage["name"]` to `CostNode.line_number` by extracting the line number from the stage name string (e.g., `"crossJoin at notebook.py:42"` → line 42), then matching nodes within ±5 lines.

**Formula for observed nodes:**
```
node.actual_compute_seconds = sum(stage["executorRunTime"] for matching stages) / 1000
node.actual_shuffle_bytes   = sum(stage["shuffleReadBytes"] + stage["shuffleWriteBytes"])
```

**Formula for unobserved nodes (scaling function fallback):**
```
linear:            estimated_compute_seconds = (input_bytes / 1e9) * 30
linear_with_cliff: same until input_bytes > memory_threshold, then × 3
quadratic:         (left_bytes * right_bytes) / 1e18 * 300
```
These are rough heuristics. The goal is order-of-magnitude guidance, not precision.

---

## Acceptance Criteria

- [ ] `estimate_cost(graph, session)` in `graph/estimate.py` returns a `CostEstimate` with `breakdown: dict[node_id, float]` (compute seconds per node)
- [ ] Nodes with a matching sparkMeasure stage get `actual_compute_seconds` populated; nodes without get a scaling-function estimate
- [ ] Stage-to-node correlation uses line number extracted from stage name; falls back to operation kind match if no line number found
- [ ] `CheckResult.compute_seconds` = sum of all node compute seconds
- [ ] Findings are re-sorted by their node's compute seconds (highest first)
- [ ] Unit test: mock `SessionState` with 2 stage dicts + a 3-node graph → correct nodes populated, third node gets scaling estimate
- [ ] Unit test: empty session → all nodes get scaling estimates, no exception

## Verification

```bash
uv run pytest tests/unit/graph/test_estimate.py -v
uv run ruff check src/burnt/graph/estimate.py
```

### Integration Check

- [ ] `burnt check tests/fixtures/e2e/cross_join.py` produces a `CheckResult` with `compute_seconds` set (even without a live Spark session, the scaling fallback runs)
