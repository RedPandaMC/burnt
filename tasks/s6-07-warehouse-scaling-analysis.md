# Task: Warehouse Auto-Scaling Efficiency Analysis

---

## Metadata

```yaml
id: s6-07-warehouse-scaling-analysis
status: todo
phase: 6
priority: medium
agent: ~
blocked_by: [s4-04-table-registry]
created_by: planner
```

---

## Context

### Goal

Add `burnt.warehouse_scaling_analysis(warehouse_id, days)` that analyses SQL warehouse
auto-scaling patterns to identify: queue time causing latency, idle cluster-hours wasting
money, and optimal min/max cluster count settings. Databricks charges for idle cluster
time on warehouses with `min_num_clusters > 0`, making this a common source of surprise
costs for teams that set "always-on" warehouses.

### Files to read

```
# Required
src/burnt/tables/compute.py
src/burnt/core/table_registry.py
src/burnt/core/pricing.py          ← SQL Classic, SQL Pro rates
src/burnt/core/models.py
src/burnt/__init__.py

# Reference
DESIGN.md
tasks/s4-04-table-registry.md
tasks/s4-07-idle-cluster-alerting.md   ← reuse idle detection patterns
```

### Background

**Module location:** `src/burnt/tables/warehouse.py`

**Data sources:**

| Table | Purpose |
|-------|---------|
| `system.compute.warehouse_events` | Scale-up/down events, query queue events |
| `system.query.history` | Per-query execution times and start times |
| `system.billing.usage` | Warehouse billing (sku_name LIKE 'SQL%') |

**Key metrics to compute:**

```
idle_cluster_hours = sum of time windows where cluster count > min and query load = 0
idle_cost_usd = idle_cluster_hours × cluster_dbu_per_hour × sql_dbu_rate

queue_events = count of events where type = "QUEUED"
avg_queue_time_seconds = mean duration of QUEUED events
max_queue_time_seconds = p99 of QUEUED event durations

utilisation_pct = actual_query_execution_time / total_cluster_uptime
```

**Scaling recommendation logic:**

```
if max_clusters_used < max_num_clusters × 0.7:
    → "max_num_clusters is set too high; scale down to {max_clusters_used + 1}"
if avg_queue_time_seconds > 30:
    → "queries queuing; increase min_num_clusters from X to {min + 1}"
if idle_cluster_hours > total_hours × 0.3:
    → "30%+ of uptime is idle; reduce min_num_clusters or enable auto-stop"
```

**`WarehouseScalingAnalysis` model:**
```python
@dataclass
class WarehouseScalingAnalysis:
    warehouse_id: str
    warehouse_name: str | None
    lookback_days: int
    sku: str                              # "SQL_CLASSIC" | "SQL_PRO" | "SQL_SERVERLESS"
    total_cost_usd: float
    idle_cost_usd: float
    idle_pct: float
    avg_queue_time_seconds: float
    max_queue_time_seconds: float
    queue_event_count: int
    max_clusters_observed: int
    current_min_clusters: int | None      # from warehouse settings (if accessible)
    current_max_clusters: int | None
    recommended_min_clusters: int
    recommended_max_clusters: int
    utilisation_pct: float
    recommendations: list[str]
    monthly_savings_if_right_sized_usd: float
```

---

## Acceptance Criteria

- [ ] `src/burnt/tables/warehouse.py` exists with `warehouse_scaling_analysis()` function
- [ ] `WarehouseScalingAnalysis` model in `src/burnt/core/models.py`
- [ ] `idle_cost_usd` computed from idle cluster-hours × SQL DBU rate
- [ ] `queue_event_count` and `avg_queue_time_seconds` from warehouse events
- [ ] All 3 recommendation rules implemented
- [ ] `recommended_min_clusters` and `recommended_max_clusters` set based on observed scaling
- [ ] `monthly_savings_if_right_sized_usd` estimated from idle cost reduction
- [ ] `TableRegistry` used for all 3 system table paths
- [ ] `burnt.warehouse_scaling_analysis()` exported from `src/burnt/__init__.py`
- [ ] `WarehouseScalingAnalysis.display()` shows metrics table and recommendations
- [ ] Unit tests: idle warehouse, queuing warehouse, right-sized warehouse (no recommendations)
- [ ] All existing tests still pass
- [ ] Lint passes: `uv run ruff check src/ tests/`

---

## Verification

### Commands

```bash
uv run pytest -m unit -v -k "warehouse_scaling"
uv run ruff check src/ tests/
```

### Integration Check

- [ ] With mock warehouse events (30% idle time, some QUEUED events): `warehouse_scaling_analysis()` returns at least 1 recommendation and `idle_pct ≈ 0.30`.

---

## Handoff

### Result

[Executor fills this in when done.]

```yaml
status: todo
```

### Blocked reason

Blocked on s4-04 (table registry).
