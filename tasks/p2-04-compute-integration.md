# Task: Implement system.compute system table queries

---

## Metadata

```yaml
id: p2-04-compute-integration
status: todo
phase: 2
priority: medium
agent: ~
blocked_by: [p2-01-databricks-connection]
created_by: planner
```

---

## Context

### Goal

Create `src/dburnrate/tables/compute.py` that queries `system.compute.node_types`, `system.compute.clusters`, and `system.compute.node_timeline` to get cluster configurations and utilization metrics. Expose `get_cluster_config(client, cluster_id) -> ClusterConfig` and `get_node_types(client) -> dict[str, float]` (node_type → DBU/hr). This grounds the static `ClusterConfig` model in real Databricks data.

### Files to read

```
# Required
src/dburnrate/tables/connection.py   (from p2-01)
src/dburnrate/core/models.py         (ClusterConfig is already defined here)
src/dburnrate/core/pricing.py        (AZURE_INSTANCE_DBU for reference)
src/dburnrate/core/exceptions.py
src/dburnrate/core/config.py
```

### Background

`system.compute.node_types` columns:
- `node_type_id`, `num_cores`, `memory_mb`, `instance_type_id`, `dbu_per_hour`

`system.compute.clusters` columns of interest:
- `cluster_id`, `cluster_name`, `cluster_source`, `spark_version`
- `node_type_id`, `driver_node_type_id`, `num_workers`, `autoscale_min_workers`, `autoscale_max_workers`
- `cluster_creator`, `start_time`, `terminated_time`

`system.compute.node_timeline` columns:
- `cluster_id`, `node_type`, `start_time`, `end_time`, `driver`, `num_nodes`

`get_cluster_config` should map the raw API response to the existing `ClusterConfig` pydantic model from `core/models.py`. Use the `dbu_per_hour` from `node_types` to fill in the `dbu_rate` field.

---

## Acceptance Criteria

- [ ] `src/dburnrate/tables/compute.py` exists
- [ ] `get_node_types(client) -> dict[str, float]` implemented (node_type_id → dbu_per_hour)
- [ ] `get_cluster_config(client, cluster_id) -> ClusterConfig` implemented
- [ ] `get_node_timeline(client, cluster_id, start_time, end_time) -> list[dict]` implemented
- [ ] No `SELECT *` in any SQL query
- [ ] `get_cluster_config` raises `DatabricksQueryError` if cluster not found
- [ ] Maps to existing `ClusterConfig` pydantic model — do not create a new model
- [ ] Unit tests in `tests/unit/tables/test_compute.py` with mocked client
- [ ] Tests cover: successful lookup, cluster not found, empty node_types
- [ ] `uv run pytest -m unit -v` passes
- [ ] `uv run ruff check src/ tests/` exits 0

---

## Verification

### Commands (run all, in order)

```bash
uv run pytest -m unit -v tests/unit/tables/test_compute.py -v
uv run pytest -m unit -v 2>&1 | tail -3
uv run ruff check src/ tests/
uv run ruff format --check src/ tests/
```

### Expected output

```
# All compute tests pass
# Total test count >= 122
All checks passed.
```

---

## Handoff

### Result

[Executor: fill in after completion]

### Blocked reason

[If blocked, explain here]
