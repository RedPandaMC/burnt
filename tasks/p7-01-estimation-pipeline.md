# Task: EstimationPipeline Orchestrator + Missing Attribution Module

---

## Metadata

```yaml
id: p4a-02-estimation-pipeline
status: todo
phase: 4B
priority: high
agent: ~
blocked_by: [p4a-01-critical-bug-fixes, p4a-01b-remaining-bugs]
created_by: planner
```

---

## Context

### Goal

The hybrid estimator, EXPLAIN parser, Delta reader, and fingerprinting all exist but nothing connects them. The CLI uses only the static estimator. This task creates:
1. `src/burnt/estimators/pipeline.py` — `EstimationPipeline` that orchestrates all tiers
2. `src/burnt/tables/attribution.py` — billing × query history join (referenced in DESIGN.md but never implemented)

### Files to read (executor reads ONLY these)

```
# Required
src/burnt/estimators/hybrid.py
src/burnt/estimators/static.py
src/burnt/parsers/explain.py
src/burnt/parsers/delta.py
src/burnt/tables/connection.py
src/burnt/tables/queries.py
src/burnt/tables/billing.py
src/burnt/core/models.py
src/burnt/core/pricing.py
src/burnt/cli/main.py
tests/unit/estimators/

# Reference
files/02-ARCHITECTURE-GAPS.md    # EstimationPipeline design + attribution SQL
files/05-RESEARCH-BACKLOG.md     # R5 attribution accuracy, Lakeflow SQL patterns
DESIGN.md                        # §"Phase 4B" + §"Lakeflow Job Cost Attribution"
```

### Background

**EstimationPipeline design** (from `files/02-ARCHITECTURE-GAPS.md §2.1`):

```python
class EstimationPipeline:
    def __init__(self, backend=None, registry=None):
        self._backend = backend  # None = offline
        self._hybrid = HybridEstimator()
        self._static = StaticEstimator()

    def estimate(self, query: str, cluster: ClusterConfig) -> CostEstimate:
        # Tier 1: always runs
        result = self._static.estimate(query, cluster)
        if self._backend is None:
            return result  # offline mode

        # Tier 2: Delta metadata (DESCRIBE DETAIL per table)
        tables = extract_tables(query)  # from sqlglot
        delta_infos = {t: self._backend.describe_detail(t) for t in tables}

        # Tier 3: EXPLAIN COST
        explain_text = self._backend.explain_cost(query)
        explain_plan = parse_explain_cost(explain_text)

        # Tier 4: fingerprint + history
        fp = fingerprint_sql(query)
        history = get_similar_queries(fp, self._backend)

        return self._hybrid.estimate(query, cluster,
                                     explain_plan=explain_plan,
                                     delta_tables=delta_infos,
                                     historical_records=history)
```

Gracefully skip tiers that fail — e.g., EXPLAIN fails → log warning → continue with Tier 1+2 result.

**Attribution SQL** (from `files/02-ARCHITECTURE-GAPS.md §2.3`):

```sql
-- Per-job-run cost attribution
SELECT t1.usage_metadata.job_id, t1.usage_metadata.job_run_id,
       SUM(t1.usage_quantity * list_prices.pricing.default) as list_cost
FROM system.billing.usage t1
INNER JOIN system.billing.list_prices list_prices
    ON t1.cloud = list_prices.cloud AND t1.sku_name = list_prices.sku_name
    AND t1.usage_start_time >= list_prices.price_start_time
    AND (t1.usage_end_time <= list_prices.price_end_time OR list_prices.price_end_time IS NULL)
WHERE t1.billing_origin_product = 'JOBS'
GROUP BY ALL

-- SQL Warehouse per-query attribution (approximate, duration-proportional)
SELECT q.statement_id,
       (q.total_task_duration_ms / hourly_total.total_ms) * u.usage_quantity AS attributed_dbu
FROM system.query.history q
JOIN system.billing.usage u
    ON q.compute.warehouse_id = u.usage_metadata.warehouse_id
    AND q.start_time BETWEEN u.usage_start_time AND u.usage_end_time
```

Note from R7: `system.query.history` covers SQL Warehouses + serverless only. All-purpose clusters and classic Jobs are NOT covered.

---

## Acceptance Criteria

- [ ] `src/burnt/estimators/pipeline.py` exists with `EstimationPipeline` class
- [ ] `EstimationPipeline.estimate()` runs Tier 1 (static) offline with no backend
- [ ] `EstimationPipeline.estimate()` gracefully skips Tier 2/3/4 if tier fails (logs warning, continues)
- [ ] `src/burnt/tables/attribution.py` exists with `AttributionClient` class
- [ ] `AttributionClient.get_job_cost(job_id, run_id)` returns attributed DBU cost
- [ ] `AttributionClient.get_warehouse_query_cost(statement_id)` returns duration-proportional DBU
- [ ] `AttributionClient.get_historical_fingerprint_cost(fingerprint)` returns p50/p95 from history
- [ ] CLI `estimate` command uses `EstimationPipeline` (not bare static estimator) by end of this task
- [ ] All public functions have type hints and docstrings
- [ ] `uv run pytest -m unit -v` passes
- [ ] `uv run ruff check src/ tests/` zero errors

---

## Verification

### Commands

```bash
uv run pytest -m unit -v
uv run ruff check src/ tests/
uv run ruff format --check src/ tests/
# Offline mode (no connection) must still work
uv run burnt estimate "SELECT * FROM orders JOIN customers ON orders.id = customers.id"
```

### Expected output

- pytest: all tests pass including new pipeline + attribution tests
- CLI smoke test works offline (Tier 1 only)

---

## Handoff

### Result

[Executor fills this in when done.]

```
status: todo
```

### Blocked reason

Blocked by `p4a-01-critical-bug-fixes` — the static and hybrid estimators must be fixed first, otherwise pipeline wires in wrong values.
