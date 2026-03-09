# Task: Fluent WhatIfBuilder with Method Chaining

---

## Metadata

```yaml
id: s2-01-whatif-builder
status: todo
sprint: 2
priority: high
agent: ~
blocked_by: [s1-02-instance-catalog]
created_by: planner
```

---

## Context

### Goal

Rewrite `estimators/whatif.py` from standalone functions into a fluent `WhatIfBuilder` class with method chaining. This is the "cuts like butter" UX — users chain modifications naturally and get a before/after comparison.

The original `legacy.py` had a working `WhatIfBuilder` with `.partition_by()`, `.z_order_by()`, `.broadcast()`, `.enable_photon()`, and `.compare()`. This task restores that pattern in the new architecture.

### Files to Read

```
src/dburnrate/estimators/whatif.py    # Current standalone functions (to be rewritten)
src/dburnrate/core/models.py          # CostEstimate, ClusterConfig
src/dburnrate/core/instances.py       # InstanceSpec, AzureInstanceCatalog (from s1-02)
DESIGN.md § "Fluent What-If Builder"  # Target API
```

### Files to Rewrite

```
src/dburnrate/estimators/whatif.py     # Full rewrite: WhatIfBuilder class
tests/unit/test_whatif.py              # Tests for builder pattern
```

### Files to Modify

```
src/dburnrate/__init__.py              # Export what_if() convenience function
src/dburnrate/core/models.py           # Add WhatIfResult model
```

---

## Specification

### WhatIfBuilder API

```python
class WhatIfBuilder:
    """Fluent builder for what-if cost scenario modeling."""

    def __init__(self, estimate: CostEstimate, cluster: ClusterConfig): ...

    # Compute type migrations
    def migrate_to(self, sku: str) -> "WhatIfBuilder": ...
    def use_serverless(self) -> "WhatIfBuilder": ...

    # Cluster modifications
    def enable_photon(self, query_type: str = "complex_join") -> "WhatIfBuilder": ...
    def disable_photon(self) -> "WhatIfBuilder": ...
    def downsize_to(self, instance_type: str, num_workers: int | None = None) -> "WhatIfBuilder": ...
    def upsize_to(self, instance_type: str, num_workers: int | None = None) -> "WhatIfBuilder": ...
    def use_spot(self, fallback: bool = True) -> "WhatIfBuilder": ...
    def set_workers(self, num_workers: int) -> "WhatIfBuilder": ...

    # Data layout optimizations
    def partition_by(self, column: str, cardinality: int = 365) -> "WhatIfBuilder": ...
    def z_order_by(self, columns: list[str]) -> "WhatIfBuilder": ...
    def enable_liquid_clustering(self) -> "WhatIfBuilder": ...

    # Output
    def compare(self) -> WhatIfResult: ...
```

### WhatIfResult Model

```python
class WhatIfModification(BaseModel):
    name: str                   # e.g., "Enable Photon", "Downsize to DS3_v2"
    cost_multiplier: float      # e.g., 0.66 = 34% cheaper
    rationale: str              # Human-readable explanation
    trade_offs: list[str]       # e.g., ["Requires 2× speedup to break even"]

class WhatIfResult(BaseModel):
    original: CostEstimate
    projected: CostEstimate
    modifications: list[WhatIfModification]
    total_savings_pct: float
    recommended_cluster: ClusterConfig | None

    def display(self) -> None: ...
    def comparison_table(self) -> str: ...
    def summary(self) -> str: ...
```

### Entry Points

```python
# From an existing estimate
result = estimate.what_if().enable_photon().compare()

# From raw parameters (convenience function at package level)
result = dburnrate.what_if(dbu=10.0, sku="ALL_PURPOSE").migrate_to("JOBS_COMPUTE").compare()
```

This requires adding a `.what_if()` method to `CostEstimate`:

```python
class CostEstimate(BaseModel):
    # ... existing fields ...
    _cluster: ClusterConfig | None = None  # Set by pipeline

    def what_if(self) -> WhatIfBuilder:
        return WhatIfBuilder(self, self._cluster or ClusterConfig())
```

### Cost Calculation Rules

- **SKU migration:** Apply rate ratio (ALL_PURPOSE $0.55 → JOBS $0.30 = 0.545× cost)
- **Photon:** Apply `2.5× DBU multiplier / speedup_factor` per operation type
- **Downsize:** Scale by `new_dbu_per_hour / old_dbu_per_hour` ratio, add VM cost delta
- **Spot:** Apply 60-80% VM cost reduction (DBU unchanged)
- **Partitioning:** `(1 - pruned_pct) × scan_cost` where `pruned_pct = 1 - (1/cardinality)`
- **Z-ORDER:** 2-5× speedup for filtered queries with matching predicates
- **Modifications stack multiplicatively**

---

## Acceptance Criteria

- [ ] `WhatIfBuilder` supports all methods listed above with fluent chaining
- [ ] Each method returns `self` for chaining
- [ ] `.compare()` returns `WhatIfResult` with original vs projected costs
- [ ] `WhatIfResult.display()` renders side-by-side comparison
- [ ] `WhatIfResult.summary()` returns one-line description (e.g., "Photon + downsize: $45 → $12 (-73%)")
- [ ] `CostEstimate.what_if()` returns a `WhatIfBuilder` bound to the estimate
- [ ] `dburnrate.what_if(dbu=, sku=)` convenience function works at package level
- [ ] Modifications stack correctly (photon + downsize + spot = multiplicative)
- [ ] Photon warns when speedup insufficient for break-even
- [ ] Existing `apply_photon_scenario`, `apply_cluster_resize`, `apply_serverless_migration` preserved as internal helpers or deprecated
- [ ] Unit tests: chaining, each modification type, stacking, display output, edge cases

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
