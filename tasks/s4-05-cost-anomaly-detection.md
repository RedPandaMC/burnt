# Task: Cost Anomaly Detection

---

## Metadata

```yaml
id: s4-05-cost-anomaly-detection
status: todo
phase: 4
priority: high
agent: ~
blocked_by: [s4-01-error-handling, s4-04-table-registry]
created_by: planner
```

---

## Context

### Goal

Add a `burnt.detect_anomalies()` function that identifies Databricks job runs whose cost
deviates significantly from the historical baseline. Uses Median Absolute Deviation (MAD)
rather than standard deviation because billing distributions are heavy-tailed — a single
expensive run shouldn't inflate the "normal" baseline. Supports both direct system table
access and pre-queried DataFrames (for enterprise environments where billing data is
exposed through governance views).

### Files to read

```
# Required
src/burnt/tables/billing.py
src/burnt/tables/attribution.py
src/burnt/core/table_registry.py
src/burnt/core/models.py
src/burnt/__init__.py

# Reference
DESIGN.md
tasks/s4-04-table-registry.md
```

### Background

**Module location:** `src/burnt/tables/anomaly.py`

**MAD-based anomaly detection:**

```
rolling_median = median(cost[t-window:t])
MAD = median(|cost[i] - rolling_median| for i in window)
z_score = (actual_cost - rolling_median) / (1.4826 × MAD)
```

The 1.4826 constant makes MAD a consistent estimator of standard deviation for normal
distributions. Flag runs where `z_score > k` (default `k=3.0`).

**For bimodal patterns** (jobs that run daily vs weekly with different cost profiles),
group by schedule cadence before computing baselines. Detect bimodality by checking if
the distribution has two clearly separated modes (gap > 2× MAD between modes).

**`CostAnomaly` model:**
```python
@dataclass
class CostAnomaly:
    job_id: str
    run_id: str
    actual_cost_usd: float
    expected_cost_usd: float       # rolling median
    deviation_factor: float        # z_score (MAD multipliers above baseline)
    timestamp: datetime
    possible_causes: list[str]     # heuristic list
    severity: Literal["warning", "critical"]  # warning: 3-5×, critical: >5×
```

**Possible causes heuristics:**
- `"cluster resized"` — if cluster config changed between this run and the baseline
- `"data volume spike"` — if run duration increased but cluster unchanged
- `"new query added"` — if number of distinct query fingerprints in the run increased
- `"job schedule change"` — if the run time is outside the usual schedule window

**Dual-input API:**

```python
# Direct (burnt queries system tables)
anomalies = burnt.detect_anomalies(job_id="12345", lookback_days=30)

# Indirect (pre-queried data for enterprise/governance environments)
df = spark.sql("SELECT * FROM governance.cost_views.billing_usage WHERE ...")
anomalies = burnt.detect_anomalies(job_id="12345", billing_data=df)
```

The `TableRegistry` from s4-04 handles the table path mapping transparently.

---

## Acceptance Criteria

- [ ] `src/burnt/tables/anomaly.py` exists with `detect_anomalies()` function
- [ ] `CostAnomaly` dataclass defined in `src/burnt/core/models.py`
- [ ] MAD-based z-score formula implemented correctly (1.4826 constant)
- [ ] Returns empty list when no anomalies exceed threshold
- [ ] `billing_data` parameter accepted as alternative to direct system table query
- [ ] `TableRegistry` used for system table path resolution
- [ ] `possible_causes` list populated from at least 2 heuristics
- [ ] `severity` field: `"warning"` for z_score 3–5, `"critical"` for z_score > 5
- [ ] `burnt.detect_anomalies()` exported from `src/burnt/__init__.py`
- [ ] Unit tests cover: no anomalies, single anomaly, bimodal distribution, billing_data parameter
- [ ] All existing tests still pass
- [ ] Lint passes: `uv run ruff check src/ tests/`

---

## Verification

### Commands

```bash
uv run pytest -m unit -v -k "anomaly"
uv run ruff check src/ tests/
```

### Integration Check

- [ ] With mock billing data (10 normal runs + 1 run at 5× cost): `detect_anomalies()` returns exactly 1 `CostAnomaly` with `severity="critical"`.

---

## Handoff

### Result

[Executor fills this in when done.]

```yaml
status: todo
```

### Blocked reason

Blocked on s4-01 (error handling hierarchy needed) and s4-04 (table registry for billing table path resolution).
