# Task: Cluster Policy Cost Guardrail Generator

---

## Metadata

```yaml
id: s6-02-cluster-policy-generator
status: todo
phase: 6
priority: medium
agent: ~
blocked_by: [s2-11-cluster-config-factory]
created_by: planner
```

---

## Context

### Goal

Add `burnt.generate_cluster_policy(max_cost_per_hour_usd, ...)` that generates a
Databricks Cluster Policy JSON document from cost ceiling constraints. Works fully
offline using the instance catalog — no workspace connection required. This makes cost
guardrails self-service: a FinOps team can generate policy JSON, paste it into the
Databricks workspace UI, and enforce cost limits across all teams.

### Files to read

```
# Required
src/burnt/core/instances.py        ← AZURE_INSTANCE_CATALOG
src/burnt/core/pricing.py          ← DBU rates, vm_cost_per_hour
src/burnt/core/models.py
src/burnt/__init__.py

# Reference
DESIGN.md
tasks/s2-11-cluster-config-factory.md
```

### Background

**Databricks Cluster Policy JSON format:**
```json
{
  "name": "Cost-Guardrail-Policy",
  "definition": {
    "node_type_id": {
      "type": "allowlist",
      "values": ["Standard_DS3_v2", "Standard_DS4_v2"],
      "defaultValue": "Standard_DS3_v2"
    },
    "num_workers": {
      "type": "range",
      "minValue": 1,
      "maxValue": 4,
      "defaultValue": 2
    },
    "autotermination_minutes": {
      "type": "fixed",
      "value": 30
    },
    "enable_elastic_disk": {
      "type": "fixed",
      "value": true
    }
  }
}
```

**Cost ceiling → allowed instance list:**

```
hourly_cost = dbu_per_hour × num_workers × dbu_rate + vm_cost_per_hour × (num_workers + 1)
```

All instances where `max_workers <= max_workers_param` AND the above formula ≤ `max_cost_per_hour_usd` qualify.

**Module location:** `src/burnt/core/policy.py`

**`ClusterPolicySpec` model:**
```python
@dataclass
class ClusterPolicySpec:
    name: str
    max_cost_per_hour_usd: float
    allowed_instance_types: list[str]
    max_workers: int
    recommended_autotermination_minutes: int
    sku: str                             # "ALL_PURPOSE" | "JOBS_COMPUTE"
    photon_allowed: bool

    def to_policy_json(self) -> dict:    # Databricks API format
        ...
    def to_json_string(self) -> str:     # pretty-printed JSON string
        ...
```

**`burnt.generate_cluster_policy()` signature:**
```python
def generate_cluster_policy(
    max_cost_per_hour_usd: float,
    sku: str = "ALL_PURPOSE",
    max_workers: int = 8,
    allow_photon: bool = True,
    autotermination_minutes: int = 30,
    policy_name: str | None = None,
) -> ClusterPolicySpec:
    ...
```

**CLI integration:**
```bash
burnt generate-policy --max-cost-per-hour 5.00 --sku ALL_PURPOSE
# Prints JSON to stdout, ready to paste into Databricks UI
```

---

## Acceptance Criteria

- [ ] `src/burnt/core/policy.py` exists with `generate_cluster_policy()` and `ClusterPolicySpec`
- [ ] Works fully offline (no backend required)
- [ ] `ClusterPolicySpec.allowed_instance_types` contains only instances within the cost ceiling
- [ ] `ClusterPolicySpec.to_policy_json()` returns valid Databricks Cluster Policy JSON
- [ ] `autotermination_minutes` set as a fixed policy value in the JSON
- [ ] `num_workers` range uses `maxValue = max_workers` in the policy JSON
- [ ] `burnt.generate_cluster_policy()` exported from `src/burnt/__init__.py`
- [ ] `burnt generate-policy` CLI command prints JSON to stdout
- [ ] Unit tests: various cost ceilings, photon allowed/blocked, Photon cost premium included
- [ ] All existing tests still pass
- [ ] Lint passes: `uv run ruff check src/ tests/`

---

## Verification

### Commands

```bash
uv run pytest -m unit -v -k "cluster_policy"
uv run ruff check src/ tests/
burnt generate-policy --max-cost-per-hour 5.00 --sku ALL_PURPOSE
```

### Integration Check

- [ ] `burnt.generate_cluster_policy(max_cost_per_hour_usd=3.00)` returns a policy with only `Standard_DS3_v2` (or similar small instance) in `allowed_instance_types`.
- [ ] `policy.to_json_string()` is valid JSON parseable by `json.loads()`.

---

## Handoff

### Result

[Executor fills this in when done.]

```yaml
status: todo
```

### Blocked reason

Blocked on s2-11 (instance catalog must be accessible via `ClusterConfig.from_databricks_json()` to validate policy generation round-trips).
