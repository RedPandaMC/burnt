# Task: Idle Cluster Cost Alerting

---

## Metadata

```yaml
id: s4-07-idle-cluster-alerting
status: todo
phase: 4
priority: medium
agent: ~
blocked_by: [s4-01-error-handling, s4-04-table-registry]
created_by: planner
```

---

## Context

### Goal

Add `burnt.detect_idle_clusters()` that identifies all-purpose clusters that are running
but consuming little or no CPU. Idle all-purpose clusters are often the largest single
source of waste in enterprise Databricks environments — a DS4_v2 cluster running idle
costs ~$265/month. The function queries `system.compute.node_timeline` for CPU utilisation,
computes a cost-per-useful-work ratio, and recommends auto-termination settings.

### Files to read

```
# Required
src/burnt/tables/compute.py
src/burnt/core/table_registry.py
src/burnt/core/models.py
src/burnt/core/pricing.py          ← DBU rates
src/burnt/core/instances.py        ← InstanceSpec (vm_cost_per_hour)
src/burnt/__init__.py

# Reference
DESIGN.md
tasks/s4-04-table-registry.md
```

### Background

**Module location:** `src/burnt/tables/idle.py`

**`system.compute.node_timeline` schema (relevant columns):**
- `cluster_id` — string
- `cluster_name` — string
- `driver` — boolean (True = driver node)
- `node_type` — string (instance type)
- `start_time` — timestamp
- `end_time` — timestamp
- `average_cpu_utilization` — double (0.0–1.0)
- `total_dbus` — decimal

**Idle definition:**
- Average CPU utilization < threshold (default 5%) over the observation window
- Observation window must be ≥ 15 minutes (avoid flagging clusters during startup)

**Idle cost calculation:**
```
idle_hours = sum of time windows where cpu_util < threshold
idle_cost_usd = idle_hours
                × (dbu_per_hour × dbu_rate_per_dbu   # DBU cost
                   + num_nodes × vm_cost_per_hour)     # VM cost
```

**`IdleCluster` model:**
```python
@dataclass
class IdleCluster:
    cluster_id: str
    cluster_name: str
    node_type: str
    num_nodes: int
    idle_hours: float
    idle_cost_usd: float
    avg_cpu_pct: float             # average CPU during idle periods
    last_active: datetime | None   # last time CPU > threshold
    recommendation: str            # auto-generated text
    auto_termination_minutes: int  # recommended setting (15, 30, or 60)
```

**Auto-termination recommendation logic:**
- If `last_active` > 4 hours ago: recommend 15-minute auto-termination
- If `last_active` 1–4 hours ago: recommend 30-minute auto-termination
- If cluster has never been active in the window: recommend termination now + permanent auto-termination

**`burnt.detect_idle_clusters()` signature:**
```python
def detect_idle_clusters(
    lookback_hours: int = 24,
    cpu_threshold_pct: float = 5.0,
    min_idle_hours: float = 0.5,    # ignore briefly idle clusters
    backend: Backend | None = None,
) -> list[IdleCluster]:
    ...
```

**Important scoping note:** Only flag `ALL_PURPOSE` SKU clusters. Jobs compute clusters
are expected to be busy or stopped — do not flag those.

---

## Acceptance Criteria

- [ ] `src/burnt/tables/idle.py` exists with `detect_idle_clusters()` function
- [ ] `IdleCluster` model defined in `src/burnt/core/models.py`
- [ ] Only ALL_PURPOSE clusters are flagged (not Jobs compute)
- [ ] CPU threshold comparison uses `average_cpu_utilization < cpu_threshold_pct / 100`
- [ ] Minimum observation window of 15 minutes enforced
- [ ] `min_idle_hours` parameter filters out briefly idle clusters
- [ ] `idle_cost_usd` formula includes both DBU cost and VM cost
- [ ] `auto_termination_minutes` correctly assigned based on `last_active` recency
- [ ] `TableRegistry` used for `system.compute.node_timeline` path resolution
- [ ] `burnt.detect_idle_clusters()` exported from `src/burnt/__init__.py`
- [ ] Unit tests: no idle clusters, one always-idle, one intermittently idle, `min_idle_hours` filter
- [ ] All existing tests still pass
- [ ] Lint passes: `uv run ruff check src/ tests/`

---

## Verification

### Commands

```bash
uv run pytest -m unit -v -k "idle_cluster"
uv run ruff check src/ tests/
```

### Integration Check

- [ ] With mock node timeline data (one cluster at 2% CPU for 6 hours): `detect_idle_clusters()` returns 1 `IdleCluster` with `auto_termination_minutes=15` and `idle_cost_usd > 0`.

---

## Handoff

### Result

[Executor fills this in when done.]

```yaml
status: todo
```

### Blocked reason

Blocked on s4-01 (error handling) and s4-04 (table registry).