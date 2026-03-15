# Task: Data Freshness vs Cost Tradeoff Analyser

---

## Metadata

```yaml
id: s6-03-freshness-cost-tradeoff
status: todo
phase: 6
priority: medium
agent: ~
blocked_by: [s4-04-table-registry, s5-06-uc-lineage-cost]
created_by: planner
```

---

## Context

### Goal

Add `burnt.freshness_analysis(job_id, lookback_days)` that compares a job's run schedule
against how often its downstream consumers actually query its output tables. When a job
runs hourly but consumers only read the output once a day, 23 of 24 runs are "wasted" —
producing fresh data that nobody queries before the next run overwrites it. This feature
surfaces those wasted runs as a concrete dollar amount.

### Files to read

```
# Required
src/burnt/tables/billing.py
src/burnt/tables/queries.py        ← system.query.history
src/burnt/core/table_registry.py
src/burnt/core/models.py
src/burnt/__init__.py

# Reference
DESIGN.md
tasks/s4-04-table-registry.md
tasks/s5-06-uc-lineage-cost.md     ← downstream table discovery reused
```

### Background

**Module location:** `src/burnt/tables/freshness.py`

**Analysis approach:**

```
1. Get job run schedule: from system.lakeflow.job_run_timeline (trigger times)
2. Get job output tables: from system.lakeflow.jobs.settings (write targets)
   OR from UC Lineage API (if available)
3. Get downstream query times: from system.query.history WHERE table_name IN (output_tables)
4. For each run: check if any downstream query happened before the NEXT run
   "useful_run" = at least one downstream query between this run and the next run
   "wasted_run" = no downstream query consumed this run's output before the next run
5. Wasted cost = wasted_runs / total_runs × total_job_cost_usd
```

**`FreshnessAnalysis` model:**
```python
@dataclass
class RunFreshnessRecord:
    run_id: str
    start_time: datetime
    end_time: datetime
    cost_usd: float
    downstream_queries: int              # queries of output tables before next run
    is_useful: bool

@dataclass
class FreshnessAnalysis:
    job_id: str
    lookback_days: int
    total_runs: int
    useful_runs: int
    wasted_runs: int
    wasted_cost_usd_per_month: float
    wasted_pct: float                   # wasted_runs / total_runs
    current_schedule_description: str   # e.g. "runs every 1 hour"
    recommended_schedule: str | None    # e.g. "consider 4-hour schedule"
    output_tables: list[str]
    avg_query_lag_hours: float          # avg time between job end and first downstream query
    runs: list[RunFreshnessRecord]
```

**Schedule recommendation logic:**

```
avg_consumption_cadence_hours = mean gap between downstream queries
recommended_interval_hours = avg_consumption_cadence_hours × 0.5
# Run twice as often as consumers read, not 10× more often
```

If `wasted_pct > 0.5` (more than half of runs are wasted):
```
"Running every 1h but consumers read output every 8h on average.
Consider changing schedule to every 4h. Estimated savings: $480/month."
```

---

## Acceptance Criteria

- [ ] `src/burnt/tables/freshness.py` exists with `freshness_analysis()` function
- [ ] `FreshnessAnalysis` and `RunFreshnessRecord` models in `src/burnt/core/models.py`
- [ ] `useful_run` correctly defined as: at least 1 downstream query between this run and the next
- [ ] `wasted_cost_usd_per_month` extrapolates from lookback window to 30 days
- [ ] `recommended_schedule` populated when `wasted_pct > 0.5`
- [ ] `avg_query_lag_hours` computed from downstream query timestamps
- [ ] `TableRegistry` used for system table path resolution
- [ ] `burnt.freshness_analysis()` exported from `src/burnt/__init__.py`
- [ ] `FreshnessAnalysis.display()` shows run timeline summary with wasted % highlighted
- [ ] Unit tests: all-useful runs, all-wasted, mixed, schedule recommendation trigger
- [ ] All existing tests still pass
- [ ] Lint passes: `uv run ruff check src/ tests/`

---

## Verification

### Commands

```bash
uv run pytest -m unit -v -k "freshness_analysis"
uv run ruff check src/ tests/
```

### Integration Check

- [ ] With mock job run data (24 hourly runs) and mock query history (queries only every 6 hours): `freshness_analysis(job_id="123", lookback_days=7)` returns `wasted_pct ≈ 0.83` and a non-None `recommended_schedule`.

---

## Handoff

### Result

[Executor fills this in when done.]

```yaml
status: todo
```

### Blocked reason

Blocked on s4-04 (table registry) and s5-06 (lineage lookup for output table discovery).
