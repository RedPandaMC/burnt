# Task: Spill Risk Scoring

---

## Metadata

```yaml
id: s3-05-spill-risk-scoring
status: todo
phase: 3
priority: high
agent: ~
blocked_by: [s3-01-delta-scan-integration]
created_by: planner
```

---

## Context

### Goal

Shuffle spill to disk is the #1 hidden cost multiplier on undersized clusters — it can 3–10× execution time. The estimation pipeline has no model for this. Add a spill risk scorer that computes whether estimated shuffle data exceeds cluster memory, flags high-risk queries with a warning, and provides actionable recommendations. The scorer must be pool-aware: in enterprise environments where instance pools have capacity limits, scale-out recommendations are constrained.

### Files to read

```
# Required
src/burnt/estimators/pipeline.py
src/burnt/estimators/hybrid.py
src/burnt/core/models.py           ← CostEstimate (warnings field), ClusterConfig
src/burnt/core/instances.py        ← AZURE_INSTANCE_CATALOG (memory per instance)
src/burnt/parsers/explain.py       ← ExplainPlan (shuffle count, estimated bytes)

# Reference
DESIGN.md
tasks/s2-12-cluster-profile.md     ← instance_pool_max_capacity field
tasks/s3-01-delta-scan-integration.md
```

### Background

**Spill ratio formula:**
```
spill_ratio = estimated_shuffle_bytes / (executor_memory_bytes × num_workers)
```

If `spill_ratio > 1.0` → spill risk. If `spill_ratio > 2.0` → high spill risk.

**Executor memory estimate:** From `InstanceSpec.memory_gb` in `AZURE_INSTANCE_CATALOG`.
Reserve 40% for Spark overhead and OS: `executor_memory_bytes = instance_memory_gb × 0.6 × 1e9`.

**Estimated shuffle bytes source (in order of preference):**
1. `ExplainPlan.shuffle_count × average_partition_size` (from EXPLAIN COST output)
2. Static heuristic: `scan_bytes × 0.3` for GROUP BY, `scan_bytes × 0.5` for sort-merge joins

**Pool awareness:** If `ClusterProfile.instance_pool_max_capacity` is set and current
workers are near that capacity, scale-out recommendations must note the constraint.

**Recommendations (unconstrained):**
```
⚠ SPILL_RISK: Estimated shuffle 48GB exceeds cluster memory 28GB (1.7×)
  Recommendations:
  1. Add 2 workers (estimated headroom: 56GB > 48GB shuffle)
  2. Upsize to Standard_E8s_v3 (64GB memory per node)
```

**Recommendations (pool-constrained):**
```
⚠ SPILL_RISK: Estimated shuffle 48GB exceeds cluster memory 28GB (1.7×)
  Instance pool 'prod-pool' at 18/20 capacity — scale-out limited.
  Recommendations:
  1. Add broadcast hint for small dimension tables (< 1GB)
  2. Increase shuffle partitions to 400 (reduces per-partition size)
  3. Request pool capacity increase from workspace admin
```

**Integration:** The scorer runs as a post-processing step in `EstimationPipeline.estimate()`.
If spill risk detected, append a structured warning to `CostEstimate.warnings`. Also
append an estimated cost multiplier to `CostEstimate.breakdown` (e.g., `"spill_risk_multiplier": 2.5`).

---

## Acceptance Criteria

- [ ] `src/burnt/estimators/spill.py` exists with `compute_spill_risk(cluster, estimated_shuffle_bytes) -> SpillRisk`
- [ ] `SpillRisk` dataclass has: `spill_ratio: float`, `risk_level: Literal["none", "low", "high"]`, `recommendations: list[str]`
- [ ] Spill ratio uses `executor_memory = instance_memory_gb × 0.6` from instance catalog
- [ ] Pool-aware: when `ClusterProfile.instance_pool_max_capacity` is near capacity, scale-out recommendations are suppressed and pool-specific suggestions provided
- [ ] `EstimationPipeline` calls spill scorer and appends warning to `CostEstimate.warnings` when `spill_ratio > 1.0`
- [ ] Warning text includes spill ratio and at least 2 actionable recommendations
- [ ] `CostEstimate.breakdown` includes `"spill_risk_multiplier"` when spill detected
- [ ] Unit tests cover: no-spill case, pool-constrained case, high-spill case
- [ ] All existing tests still pass
- [ ] Lint passes: `uv run ruff check src/ tests/`

---

## Verification

### Commands

```bash
uv run pytest -m unit -v -k "spill_risk"
uv run ruff check src/ tests/
```

### Integration Check

- [ ] Estimate a query with a 6-table join on a 2-worker `Standard_DS3_v2` cluster (14GB memory each). With estimated shuffle of 20GB: assert `spill_ratio ≈ 1.19` and a spill warning appears in `estimate.warnings`.

---

## Handoff

### Result

[Executor fills this in when done.]

```yaml
status: todo
```

### Blocked reason

Blocked on s3-01 (delta scan integration provides shuffle byte estimates from EXPLAIN COST).
