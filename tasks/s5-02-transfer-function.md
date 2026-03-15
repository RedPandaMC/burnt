# Task: Transfer Function — Source-to-Target Cluster Scaling

---

## Metadata

```yaml
id: s5-02-transfer-function
status: todo
sprint: 5
priority: high
agent: ~
blocked_by: [s5-01-feature-extraction]
created_by: planner
```

---

## Context

### Goal

Implement `src/burnt/estimators/transfer.py` — a physics-based scaling module that transforms observed session metrics (from an all-purpose interactive cluster) into predicted metrics for a target cluster configuration (jobs compute, serverless, different instance type/worker count). This is **Layer 2** of the three-layer architecture, bridging live observations to the base model's training distribution.

### Why Physics, Not ML

The transfer function encodes invariants that hold regardless of training data:

1. **Total I/O is cluster-independent.** A query scanning 50GB scans 50GB on any cluster.
2. **Duration scales inversely with parallelism** (Amdahl's Law, ~0.7–0.85 efficiency for shuffle-heavy Spark).
3. **Spill behaviour depends on per-worker memory pressure**, with a threshold effect (~60% of executor memory).
4. **DBU = runtime_hours × workers × dbu_per_hour** — deterministic billing formula.

With <1K training samples, learning these relationships via ML would require seeing sufficient cluster variation. The physics-based approach encodes them directly, making the model more robust.

### Files to Read

```
tasks/r7-ml-architecture-v4.md              # Architecture: Layer 2 design
tasks/s5-01-feature-extraction.md            # SessionMetrics → QueryFeatures
src/burnt/runtime/listener.py                # SessionMetrics dataclass
src/burnt/core/models.py                     # ClusterConfig
src/burnt/core/instances.py                  # InstanceSpec, AzureInstanceCatalog
```

---

## Specification

### Core Data Model

```python
@dataclass(frozen=True)
class TransferredMetrics:
    """Metrics scaled from source cluster to target cluster."""
    
    # I/O (invariant — same on any cluster)
    read_bytes: int
    shuffle_read_bytes: int
    shuffle_write_bytes: int
    
    # Scaled by parallelism (Amdahl's Law)
    estimated_duration_ms: int
    estimated_task_duration_ms: int
    
    # Scaled by memory pressure (threshold effect)
    estimated_disk_spill_bytes: int
    estimated_memory_spill_bytes: int
    
    # Derived DBU (deterministic billing formula)
    estimated_dbu: float
    
    # Confidence signal
    transfer_confidence: str  # "high" | "medium" | "low"
    scaling_notes: list[str]  # human-readable explanations
```

### Transfer Function

```python
def transfer_to_target(
    observed: SessionMetrics,
    source_cluster: ClusterConfig,
    target_cluster: ClusterConfig,
    *,
    source_spec: InstanceSpec | None = None,
    target_spec: InstanceSpec | None = None,
    scaling_efficiency: float = 0.8,
) -> TransferredMetrics:
    """Scale observed session metrics to a target cluster configuration.
    
    Invariants applied:
    1. Total I/O bytes are cluster-independent (same query, same data)
    2. Duration scales with worker_ratio^scaling_efficiency (Amdahl's Law)
       - scaling_efficiency: 0.7 for shuffle-heavy, 0.9 for scan-only, 0.8 default
    3. Spill depends on per-worker memory pressure vs threshold
       - Below 60% executor memory: ~0 spill
       - Above 60%: spill grows proportionally
       - Regime change (source below, target above threshold): flag as low confidence
    4. DBU = (duration_hours) × (target_workers) × (target_dbu_per_hour)
    
    Args:
        observed: Live session metrics from sparkMeasure/listener
        source_cluster: Current interactive cluster config
        target_cluster: Desired orchestration cluster config
        source_spec: Instance spec for source (memory_gb, vcpus). Auto-lookup if None.
        target_spec: Instance spec for target. Auto-lookup if None.
        scaling_efficiency: Amdahl's coefficient (0.7–0.9). Loaded from calibration.
    
    Returns:
        TransferredMetrics with predicted values and confidence assessment.
    """
```

### Scaling Laws (The Math)

**Duration scaling (Amdahl's Law):**
```
worker_ratio = source_workers / target_workers
transferred_duration = observed_duration × (worker_ratio ^ scaling_efficiency)

# scaling_efficiency empirically:
#   0.7 — shuffle-heavy queries (many SortMergeJoins)
#   0.8 — mixed workloads (default)
#   0.9 — scan-only queries (simple filters, no shuffle)
```

**Memory pressure and spill:**
```
source_bytes_per_worker = total_input_bytes / source_workers
target_bytes_per_worker = total_input_bytes / target_workers

source_memory_pressure = source_bytes_per_worker / (source_memory_gb × 1e9)
target_memory_pressure = target_bytes_per_worker / (target_memory_gb × 1e9)

SPILL_THRESHOLD = 0.6

if target_pressure > SPILL_THRESHOLD and source_pressure <= SPILL_THRESHOLD:
    # REGIME CHANGE: target will spill when source didn't
    transferred_spill = target_bytes_per_worker × 0.3 × target_workers
    confidence = "low"  # uncertain territory
elif target_pressure <= SPILL_THRESHOLD and source_pressure > SPILL_THRESHOLD:
    # Target won't spill when source did — cost improvement
    transferred_spill = 0
elif source_pressure > 0:
    # Same regime — proportional scaling
    transferred_spill = observed_spill × (target_pressure / source_pressure)
```

**DBU formula (deterministic):**
```
runtime_hours = transferred_duration_ms / 3_600_000
total_dbu = runtime_hours × target_workers × target_dbu_per_hour
```

**Photon adjustment:**
```
if source has Photon and target doesn't:
    # Remove Photon speedup: duration × 2.7 (average speedup reversed)
    # Remove Photon DBU multiplier: dbu × (1.0 / 2.5)
    
if target has Photon and source doesn't:
    # Apply Photon speedup: duration / 2.7
    # Apply Photon DBU multiplier: dbu × 2.5
    
# Net effect: Photon typically saves ~20-57% cost for complex SQL
# but increases cost ~72% for simple appends/inserts
# Use query type (from AST) to select appropriate speedup factor
```

### Confidence Assessment

```python
def _compute_transfer_confidence(
    worker_ratio: float,
    source_memory_pressure: float,
    target_memory_pressure: float,
    photon_change: bool,
    sku_change: bool,
) -> str:
    """Assess confidence of the transfer prediction.
    
    "high"   — same SKU, similar worker count (0.5-2×), no spill regime change
    "medium" — moderate changes (2-4× workers, or Photon toggle)
    "low"    — large changes (>4× workers), spill regime change, or SKU change
    """
```

---

## Acceptance Criteria

- [ ] `src/burnt/estimators/transfer.py` created
- [ ] `TransferredMetrics` dataclass with all fields
- [ ] `transfer_to_target()` implements all four scaling laws:
  - I/O invariance (bytes unchanged)
  - Duration via Amdahl's Law with configurable efficiency
  - Spill via memory pressure threshold model
  - DBU via deterministic billing formula
- [ ] Photon toggle adjustment with per-query-type speedup factors
- [ ] Confidence assessment based on transfer magnitude
- [ ] `scaling_notes` list explains each transformation in human-readable form
- [ ] Edge cases handled: 0 workers, unknown instance types, missing specs
- [ ] New unit tests: `tests/unit/estimators/test_transfer.py`
  - Same cluster → identity transform (output ≈ input)
  - Double workers → ~halved duration, same total DBU
  - Half workers → ~doubled duration, same total DBU
  - Spill regime change detection (source no-spill → target spill)
  - Photon toggle: on→off and off→on
  - SKU change: ALL_PURPOSE → JOBS_COMPUTE pricing
  - Confidence levels: high/medium/low for various scenarios
- [ ] All existing tests pass
- [ ] Lint passes

---

## Verification

```bash
uv run pytest -m unit -v tests/unit/estimators/test_transfer.py
uv run pytest -m unit -v 2>&1 | tail -5
uv run ruff check src/ tests/
```

---

## Handoff

```yaml
status: todo
```
