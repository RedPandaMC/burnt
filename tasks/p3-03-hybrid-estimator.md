# Task: Implement hybrid estimator combining static + EXPLAIN signals

---

## Metadata

```yaml
id: p3-03-hybrid-estimator
status: done
phase: 3
priority: high
agent: claude-sonnet-4-6
blocked_by: [p3-01-explain-parser, p2-01-databricks-connection]
created_by: planner
```

---

## Context

### Goal

Implement `src/dburnrate/estimators/hybrid.py` that combines signals from:
1. Static complexity analysis (`estimators/static.py`)
2. EXPLAIN COST plan data (`parsers/explain.py`)
3. Historical query data (`tables/queries.py`) when available

The hybrid estimator should produce more accurate `CostEstimate` objects by weighting signals based on availability: EXPLAIN data > historical match > static analysis alone.

### Files to read

```
# Required
src/dburnrate/estimators/static.py
src/dburnrate/parsers/explain.py    (from p3-01)
src/dburnrate/core/models.py        (CostEstimate, ExplainPlan, ClusterConfig)
src/dburnrate/core/exceptions.py

# Reference
src/dburnrate/estimators/whatif.py  (for style)
RESEARCH.md   # Section on hybrid estimation and confidence calibration
```

### Background

Weighting strategy (from RESEARCH.md):
- If `ExplainPlan.stats_complete = True`: weight EXPLAIN 70%, static 30%
- If `ExplainPlan.stats_complete = False`: weight EXPLAIN 40%, static 60%
- If historical match found (exact fingerprint): use p50 historical duration, confidence = high
- If no EXPLAIN available: fall back to static estimator only

DBU calculation from EXPLAIN:
```
scan_dbu = total_size_bytes / 1e9 * scan_dbu_per_gb
shuffle_dbu = shuffle_count * shuffle_dbu_per_shuffle
join_penalty = sum(join_weight[jtype] for jtype in join_types)
total_dbu = (scan_dbu + shuffle_dbu + join_penalty) * cluster_dbu_rate
```
(Use empirical weights from RESEARCH.md or define reasonable defaults)

Confidence levels:
- `high`: historical exact match OR (stats_complete AND plan_depth < 5)
- `medium`: partial stats OR static + EXPLAIN combined
- `low`: static analysis only

---

## Acceptance Criteria

- [ ] `src/dburnrate/estimators/hybrid.py` exists
- [ ] `HybridEstimator` class with `estimate(query, cluster_config, explain_plan=None, historical=None) -> CostEstimate`
- [ ] Implements weighted combination of static + EXPLAIN signals
- [ ] Falls back gracefully if `explain_plan` is None (pure static)
- [ ] Uses historical data if provided (list of `QueryRecord` from p2-03)
- [ ] Confidence level correctly reflects signal availability
- [ ] All Decimal arithmetic — no float for monetary values
- [ ] Unit tests in `tests/unit/estimators/test_hybrid.py`
- [ ] Tests cover: static-only, explain-only, combined, historical match
- [ ] `uv run pytest -m unit -v` passes
- [ ] `uv run ruff check src/ tests/` exits 0

---

## Verification

### Commands (run all, in order)

```bash
uv run pytest -m unit -v tests/unit/estimators/test_hybrid.py -v
uv run pytest -m unit -v 2>&1 | tail -3
uv run ruff check src/ tests/
uv run ruff format --check src/ tests/
```

---

## Handoff

### Result

Implemented `src/dburnrate/estimators/hybrid.py` with `HybridEstimator` class.

Key implementation decisions:
- `CostEstimate` has no `explanation` field — used `breakdown` (dict) and `warnings` (list) to carry signal metadata
- `CostEstimate.estimated_cost_usd` is `float | None` (not Decimal) — matched existing model convention
- Static estimator `estimate()` takes `(query, language, cluster)` — called with `cluster=cluster` kwarg
- `ClusterConfig.dbu_per_hour` confirmed as the correct field name (float)
- Nominal `$0.20/DBU` rate used for cost_usd when no pricing is available

Signal priority order: historical p50 median > EXPLAIN COST blend > static fallback.
Confidence assignment: historical=high, stats_complete=high, stats_incomplete=medium, static-only=low/medium.

Tests: 19 unit tests covering all required scenarios.
All 263 unit tests pass. ruff check and format clean.

### Blocked reason

N/A
