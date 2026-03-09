# Task: Azure Instance Catalog + Cluster Right-Sizer

---

## Metadata

```yaml
id: s1-02-instance-catalog
status: todo
sprint: 1
priority: critical
agent: ~
blocked_by: []
created_by: planner
```

---

## Context

### Goal

Build the Azure VM instance catalog and the `right_size()` function that produces three-tier cluster recommendations (economy/balanced/performance) with Databricks API-compatible JSON output. This is the core "suggest cluster configurations" capability.

The original `legacy.py` had a working 86-instance catalog with `to_api_json()` output — this task brings that capability back into the new architecture with proper Pydantic models and empirically grounded data.

### Files to Read

```
src/dburnrate/core/models.py          # Current ClusterConfig model (needs extension)
src/dburnrate/core/pricing.py         # DBU rate lookups
DESIGN.md § "Azure Instance Catalog"  # Architecture spec
DESIGN.md § "Right-Sizing Logic"      # Sizing heuristics
```

### Files to Create

```
src/dburnrate/core/instances.py        # AzureInstanceCatalog + right-sizing logic
tests/unit/test_instances.py
```

### Files to Modify

```
src/dburnrate/core/models.py           # Extend ClusterConfig, add ClusterRecommendation, InstanceSpec
```

---

## Specification

### InstanceSpec Model

```python
class InstanceSpec(BaseModel):
    instance_type: str              # e.g., "Standard_DS3_v2"
    vcpus: int
    memory_gb: float
    local_storage_gb: float
    dbu_rate: float                 # DBU/hr for this instance
    photon_dbu_rate: float          # DBU/hr with Photon (2.5× on Azure)
    vm_cost_per_hour: float         # Azure VM price $/hr
    category: Literal["general", "memory", "compute", "storage"]
```

### AzureInstanceCatalog

- Hardcoded catalog of 22+ Azure VM types across 4 categories
- `filter_by(category=, min_memory_gb=, min_vcpus=)` → filtered list
- `get(instance_type: str)` → InstanceSpec or raise
- `find_smaller(current: str)` → next size down in same family
- `find_larger(current: str)` → next size up in same family
- `recommend_for_workload(profile: WorkloadProfile)` → InstanceSpec
- Data sourced from Azure pricing API (`prices.azure.com/api/retail/prices`) with hardcoded fallback

### WorkloadProfile Model

```python
class WorkloadProfile(BaseModel):
    peak_memory_pct: float = 0.0       # 0-100
    peak_cpu_pct: float = 0.0          # 0-100
    spill_to_disk_bytes: int = 0
    data_gb: float = 0.0
    shuffle_bytes: int = 0
    compute_intensity: float = 0.5     # 0=IO, 1=CPU
    memory_intensity: float = 0.5      # 0=light, 1=heavy
```

### Extended ClusterConfig

Add to existing `ClusterConfig`:
```python
spot_policy: Literal["ON_DEMAND", "SPOT_WITH_ON_DEMAND_FALLBACK", "SPOT"] = "ON_DEMAND"
autoscale_min_workers: int | None = None
autoscale_max_workers: int | None = None

def to_api_json(self, spark_version: str = "15.4.x-scala2.12") -> dict:
    """Return Databricks Jobs API-compatible cluster definition."""
```

### ClusterRecommendation Model

```python
class ClusterRecommendation(BaseModel):
    economy: ClusterConfig      # Smallest viable
    balanced: ClusterConfig     # Default recommendation
    performance: ClusterConfig  # Headroom for growth
    current_cost_usd: float
    rationale: str              # Human-readable explanation

    def comparison_table(self) -> str: ...
    def display(self) -> None: ...
```

### Right-Sizing Logic

```python
def right_size(
    profile: WorkloadProfile,
    current_config: ClusterConfig | None = None,
    prefer_spot: bool = True,
    max_ips: int | None = None,
) -> ClusterRecommendation:
```

Classification:
- `peak_memory_pct < 30% AND peak_cpu_pct < 40%` → downsize
- `spill_to_disk_bytes > 0` → upsize memory (E-series)
- `total_task / execution > 4` → scale out (shuffle-bound)
- `compute_intensity > 0.7` → F-series
- Default → D-series general purpose

---

## Acceptance Criteria

- [ ] `AzureInstanceCatalog` has 22+ instances across 4 categories with verified pricing
- [ ] `InstanceSpec` model includes VM cost, DBU rate, and Photon DBU rate
- [ ] `right_size()` returns `ClusterRecommendation` with economy/balanced/performance tiers
- [ ] `ClusterConfig.to_api_json()` produces valid Databricks Jobs API cluster definition
- [ ] `ClusterRecommendation.comparison_table()` renders a readable ASCII table
- [ ] Spot policy defaults to `SPOT_WITH_ON_DEMAND_FALLBACK` for jobs
- [ ] IP-awareness: if `max_ips` constrains workers, recommend larger instances to compensate
- [ ] All pricing data cites source (Azure pricing API or Databricks pricing page)
- [ ] Unit tests cover: each workload category → correct instance family, to_api_json() schema, edge cases

---

## Verification

```bash
uv run pytest -m unit -v
uv run ruff check src/ tests/
uv run ruff format --check src/ tests/
```

---

## Handoff

```yaml
status: todo
```
