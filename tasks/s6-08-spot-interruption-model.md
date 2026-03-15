# Task: Spot Instance Interruption Cost Model

---

## Metadata

```yaml
id: s6-08-spot-interruption-model
status: todo
phase: 6
priority: medium
agent: ~
blocked_by: [s4-04-table-registry, s4-07-idle-cluster-alerting]
created_by: planner
```

---

## Context

### Goal

Add `burnt.spot_analysis(job_id, lookback_days)` that models the true total cost of
ownership for jobs running on spot instances. The simulation builder's `use_spot()`
applies a fixed 40% discount but ignores the retry overhead from spot preemptions. This
feature uses historical `system.compute.node_timeline` data to compute actual
interruption rates and model the expected retry cost, then compares on-demand vs spot TCO.

### Files to read

```
# Required
src/burnt/tables/compute.py
src/burnt/tables/billing.py
src/burnt/estimators/whatif.py     ← use_spot() current implementation
src/burnt/core/table_registry.py
src/burnt/core/models.py
src/burnt/__init__.py

# Reference
DESIGN.md
tasks/s4-04-table-registry.md
tasks/s4-07-idle-cluster-alerting.md   ← node_timeline parsing patterns
```

### Background

**Module location:** `src/burnt/tables/spot.py`

**Interruption rate from `system.compute.node_timeline`:**

A spot preemption appears as a node with a short lifetime (< job expected duration) and
termination reason `"PREEMPTED"` or `"SPOT_PREEMPTION"`. If the cluster has on-demand
fallback, the job continues on on-demand nodes after preemption.

```
interruption_rate = preempted_node_count / total_spot_node_count
avg_retry_cost_factor = 1 + (interruption_rate × avg_wasted_work_fraction)
# avg_wasted_work_fraction ≈ 0.5 (lost half the run on average before retry)
```

**TCO model:**

```
spot_tco = base_cost × (1 - spot_discount) × avg_retry_cost_factor
on_demand_tco = base_cost

breakeven_interruption_rate = 1 - (1 / (1 + spot_discount / (1 - spot_discount)))
# If actual rate < breakeven, spot is still cheaper despite retries
```

**`SpotAnalysis` model:**
```python
@dataclass
class SpotAnalysis:
    job_id: str
    lookback_days: int
    total_runs: int
    interrupted_runs: int
    interruption_rate: float           # 0.0–1.0
    avg_retry_cost_factor: float       # 1.0 = no retries, 1.5 = 50% overhead
    spot_discount_pct: float           # observed discount vs on-demand
    on_demand_tco_usd_per_month: float
    spot_tco_usd_per_month: float      # includes retry overhead
    net_savings_usd_per_month: float   # positive = spot still wins
    breakeven_interruption_rate: float
    recommendation: str               # "use_spot" | "use_on_demand" | "use_spot_with_fallback"
    simulation_result: WhatIfResult   # updated use_spot() result with real retry factor
```

**`use_spot()` calibration update:**

When `SpotAnalysis` is available for a job, the simulation builder's `use_spot()` should
use the actual `spot_discount_pct` and `avg_retry_cost_factor` instead of the generic
40% discount. Provide `WhatIfBuilder.use_spot_calibrated(spot_analysis)` for this.

---

## Acceptance Criteria

- [ ] `src/burnt/tables/spot.py` exists with `spot_analysis()` function
- [ ] `SpotAnalysis` model in `src/burnt/core/models.py`
- [ ] `interruption_rate` computed from `system.compute.node_timeline` preemption events
- [ ] `avg_retry_cost_factor` uses 0.5 wasted work fraction by default (overridable)
- [ ] `spot_tco_usd_per_month` includes retry overhead
- [ ] `on_demand_tco_usd_per_month` computed from billing history for on-demand comparison
- [ ] `breakeven_interruption_rate` computed from observed discount
- [ ] Recommendation logic: `"use_spot"` when `interruption_rate < breakeven_interruption_rate`
- [ ] `WhatIfBuilder.use_spot_calibrated(spot_analysis)` added to simulation builder
- [ ] `burnt.spot_analysis()` exported from `src/burnt/__init__.py`
- [ ] Unit tests: no interruptions (spot clearly better), high interruptions (on-demand better), near-breakeven
- [ ] All existing tests still pass
- [ ] Lint passes: `uv run ruff check src/ tests/`

---

## Verification

### Commands

```bash
uv run pytest -m unit -v -k "spot_analysis"
uv run ruff check src/ tests/
```

### Integration Check

- [ ] With mock node timeline (10% interruption rate, 35% observed discount): `spot_analysis()` returns `recommendation="use_spot"` and `net_savings_usd_per_month > 0`.

---

## Handoff

### Result

[Executor fills this in when done.]

```yaml
status: todo
```

### Blocked reason

Blocked on s4-04 (table registry) and s4-07 (node_timeline parsing patterns).
