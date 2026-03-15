# Task: ClusterConfig Factory + ClusterProfile

---

## Metadata

```yaml
id: s2-11-cluster-config-enrichment
status: todo
phase: 2
priority: medium
agent: ~
blocked_by: []
created_by: planner
supersedes: [s2-11-cluster-config-factory, s2-12-cluster-profile]
```

---

## Context

### Goal

Two closely related additions to the cluster config model, shipped as one unit:

1. **`ClusterConfig.from_databricks_json()`** — classmethod that parses a Jobs API `new_cluster` payload into a `ClusterConfig`. Removes friction for users who already have deployment manifests.

2. **`ClusterProfile`** — wraps `ClusterConfig` and adds runtime context: driver node type, Spark/DBR version, custom Spark config, cluster tags, instance pool fields, and cloud provider. Used internally by the estimation pipeline; `ClusterConfig` remains the minimal user-facing type.

### Files to Read

```
src/burnt/core/models.py
src/burnt/core/instances.py
src/burnt/core/pricing.py
src/burnt/estimators/whatif.py
src/burnt/advisor/session.py
```

---

## Specification

### Part 1: `ClusterConfig.from_databricks_json()`

Accepts the full Jobs API payload (with `new_cluster` key) OR a bare cluster dict.

```python
@classmethod
def from_databricks_json(cls, payload: dict) -> "ClusterConfig":
    cluster = payload.get("new_cluster", payload)
    node_type = cluster.get("node_type_id", "Standard_DS3_v2")
    dbu = cls._lookup_dbu_rate(node_type)  # falls back to default + logged warning
    spot_raw = cluster.get("azure_attributes", {}).get("availability", "ON_DEMAND")
    spot_map = {
        "ON_DEMAND": SpotPolicy.ON_DEMAND,
        "SPOT_WITH_ON_DEMAND_FALLBACK": SpotPolicy.SPOT_WITH_ON_DEMAND_FALLBACK,
        "SPOT": SpotPolicy.SPOT,
    }
    autoscale = cluster.get("autoscale", {})
    return cls(
        instance_type=node_type,
        num_workers=cluster.get("num_workers", autoscale.get("max_workers", 2)),
        dbu_per_hour=dbu,
        photon_enabled="photon" in cluster.get("spark_version", "").lower()
            or cluster.get("runtime_engine", "").upper() == "PHOTON",
        spot_policy=spot_map.get(spot_raw, SpotPolicy.ON_DEMAND),
        autoscale_min_workers=autoscale.get("min_workers"),
        autoscale_max_workers=autoscale.get("max_workers"),
    )
```

### Part 2: `ClusterProfile`

New Pydantic model in `src/burnt/core/models.py`. The pipeline uses `ClusterProfile` internally; `ClusterConfig` remains the type users construct directly.

```python
class ClusterProfile(BaseModel):
    """Full cluster configuration including runtime context used by the estimation pipeline."""
    config: ClusterConfig
    driver_node_type: str | None = None
    spark_version: str | None = None           # e.g. "15.4.x-scala2.12"
    custom_spark_conf: dict[str, str] = {}     # e.g. {"spark.sql.shuffle.partitions": "400"}
    cluster_tags: dict[str, str] = {}
    instance_pool_id: str | None = None
    instance_pool_max_capacity: int | None = None
    cloud_provider: Literal["AZURE", "AWS", "GCP"] = "AZURE"

    @classmethod
    def from_databricks_json(cls, payload: dict) -> "ClusterProfile":
        """Populates both config (via ClusterConfig.from_databricks_json) and extended fields."""
        ...
```

Simulation builder internals that accept `ClusterConfig` should be updated to accept `ClusterConfig | ClusterProfile`, using extended fields when present and ignoring them when absent.

---

## Acceptance Criteria

### ClusterConfig factory

- [ ] `ClusterConfig.from_databricks_json(payload)` importable from `burnt`
- [ ] Accepts full Jobs API payload (with `"new_cluster"` key) or bare cluster dict
- [ ] Maps `node_type_id` → `dbu_per_hour` for all catalog instances; unknown types get default + logged warning
- [ ] Maps `azure_attributes.availability` → `SpotPolicy`
- [ ] Maps `autoscale` → `autoscale_min_workers` / `autoscale_max_workers`
- [ ] Detects Photon from `spark_version` containing `"photon"` (case-insensitive) or `runtime_engine == "PHOTON"`
- [ ] Round-trip test: `ClusterConfig.from_databricks_json(config.to_api_json())` produces an equivalent config

### ClusterProfile

- [ ] `ClusterProfile` model exists in `src/burnt/core/models.py` with all fields listed above
- [ ] `ClusterProfile.from_databricks_json(payload)` correctly populates both `config` and extended fields
- [ ] `ClusterProfile` exported from `burnt` top-level `__init__.py`
- [ ] Simulation builder internals accept `ClusterConfig | ClusterProfile` without errors
- [ ] `advise_current_session()` / `advise()` builds and attaches a `ClusterProfile` to `AdvisoryReport` (including `spark_version` and `custom_spark_conf` from SparkSession conf when available)

### Tests

- [ ] Unit tests cover: standard payload, bare dict, Photon detection, spot mapping, unknown node type, `ClusterProfile` construction, `from_databricks_json`, field fallback in simulation
- [ ] All existing tests still pass
- [ ] Lint passes: `uv run ruff check src/ tests/`

---

## Verification

```bash
uv run pytest -m unit -v -k "cluster_config_factory or from_databricks_json or cluster_profile"
uv run ruff check src/ tests/
```

### Integration Check

- [ ] `from burnt import ClusterConfig; c = ClusterConfig.from_databricks_json({"node_type_id": "Standard_DS4_v2", "num_workers": 4}); assert c.dbu_per_hour == 1.5`
- [ ] `from burnt import ClusterProfile; p = ClusterProfile.from_databricks_json({"node_type_id": "Standard_DS4_v2", "spark_version": "15.4.x-photon-scala2.12", "num_workers": 4}); assert p.spark_version == "15.4.x-photon-scala2.12"; assert p.config.photon_enabled is True`

---

## Handoff

### Result

[Executor fills this in when done.]

```yaml
status: todo
```
