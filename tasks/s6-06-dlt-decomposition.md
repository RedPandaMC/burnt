# Task: DLT Pipeline Cost Decomposition

---

## Metadata

```yaml
id: s6-06-dlt-decomposition
status: todo
phase: 6
priority: medium
agent: ~
blocked_by: [s4-04-table-registry, s4-06-tag-cost-attribution]
created_by: planner
```

---

## Context

### Goal

Add `burnt.dlt_decomposition(pipeline_id, days)` that breaks down DLT pipeline costs
per table and per dataset layer (Bronze/Silver/Gold or raw/cleaned/aggregated). Today,
DLT billing appears as a single line in `system.billing.usage` with no per-table
attribution. This feature joins DLT event logs with billing data to estimate table-level
cost contribution using compute-time fractions.

### Files to read

```
# Required
src/burnt/tables/billing.py
src/burnt/core/table_registry.py
src/burnt/core/models.py
src/burnt/__init__.py

# Reference
DESIGN.md
tasks/s4-04-table-registry.md
```

### Background

**Module location:** `src/burnt/tables/dlt.py`

**Data sources:**

| Table | Purpose |
|-------|---------|
| `system.lakeflow.pipeline_event_log` | Per-update events with dataset name and duration |
| `system.billing.usage` | Total DBU cost for the pipeline cluster by time window |

**Attribution model:**

DLT update events in `pipeline_event_log` have `origin.dataset_name` and timestamps.
For each dataset, compute its fraction of total compute time in the billing window.
Apply that fraction to the total billing cost.

```
dataset_cost_fraction = dataset_compute_time / total_pipeline_compute_time
dataset_cost_usd = total_pipeline_cost_usd × dataset_cost_fraction
```

**Tier detection:** Infer Bronze/Silver/Gold from naming convention:
- Names ending in `_raw`, `_bronze`, or starting with `bronze_` → Bronze
- Names ending in `_clean`, `_silver` → Silver
- Names ending in `_agg`, `_gold`, `_mart` → Gold
- Otherwise → Unknown

**`DLTPipelineDecomposition` model:**
```python
@dataclass
class DLTDatasetCost:
    dataset_name: str
    tier: str                            # "bronze" | "silver" | "gold" | "unknown"
    compute_seconds: float
    cost_fraction: float
    cost_usd: float
    update_count: int
    avg_update_duration_seconds: float

@dataclass
class DLTPipelineDecomposition:
    pipeline_id: str
    lookback_days: int
    total_cost_usd: float
    datasets: list[DLTDatasetCost]
    bronze_cost_usd: float               # sum by tier
    silver_cost_usd: float
    gold_cost_usd: float
    unknown_cost_usd: float
    most_expensive_dataset: DLTDatasetCost
    attribution_method: str              # "compute_time_fraction"
    coverage_warning: str | None         # None if attribution is clean
```

**Coverage warning:** If `total_pipeline_compute_time` differs from billing window by
>20%, note that the attribution is approximate (different update frequencies may not
align perfectly with billing windows).

---

## Acceptance Criteria

- [ ] `src/burnt/tables/dlt.py` exists with `dlt_decomposition()` function
- [ ] `DLTDatasetCost` and `DLTPipelineDecomposition` models in `src/burnt/core/models.py`
- [ ] Attribution uses compute time fractions from `system.lakeflow.pipeline_event_log`
- [ ] Bronze/Silver/Gold tier detection from naming convention (all 4 tiers)
- [ ] `bronze_cost_usd`, `silver_cost_usd`, `gold_cost_usd`, `unknown_cost_usd` sums correct
- [ ] `coverage_warning` set when compute time / billing window mismatch > 20%
- [ ] `TableRegistry` used for both system table paths
- [ ] `burnt.dlt_decomposition()` exported from `src/burnt/__init__.py`
- [ ] `DLTPipelineDecomposition.display()` shows per-dataset breakdown sorted by cost
- [ ] Unit tests: 3-tier pipeline, single-tier, coverage warning trigger, tier detection
- [ ] All existing tests still pass
- [ ] Lint passes: `uv run ruff check src/ tests/`

---

## Verification

### Commands

```bash
uv run pytest -m unit -v -k "dlt_decomposition"
uv run ruff check src/ tests/
```

### Integration Check

- [ ] With mock pipeline event log (3 datasets: `orders_raw`, `orders_clean`, `revenue_mart`) and $100 total billing: `dlt_decomposition()` returns Bronze/Silver/Gold costs summing to ≈ $100.

---

## Handoff

### Result

[Executor fills this in when done.]

```yaml
status: todo
```

### Blocked reason

Blocked on s4-04 (table registry) and s4-06 (billing attribution patterns reused).
