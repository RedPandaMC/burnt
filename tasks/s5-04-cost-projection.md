# Task: Cost Projection + Calibration (Batch & Streaming)

---

## Metadata

```yaml
id: s5-04-cost-projection
status: todo
sprint: 5
priority: high
agent: ~
blocked_by: [s5-02-transfer-function, s5-03-cost-regressor]
created_by: planner
supersedes: [s5-04-cost-projection, s5-05-streaming-cost-projection]
```

---

## Context

### Goal

Implement three modules:

1. **`src/burnt/estimators/projection.py`** — Projects a per-run DBU estimate + schedule + pricing to daily/monthly/yearly cost, including VM infrastructure for classic clusters.

2. **`src/burnt/estimators/calibration.py`** — **Layer 3** of the three-layer ML architecture. Stores a `scaling_efficiency` scalar and `bias_correction` in `~/.burnt/calibration.json`, updated via EMA when actual billing data arrives.

3. **`src/burnt/estimators/streaming.py`** — Extends the projection framework for Structured Streaming workloads, which have fundamentally different cost profiles (continuous compute + per-GB processing costs).

### Architecture Position

```
Layer 1: Base Model (s5-03)        ← predicts base DBU from normalised features
Layer 2: Transfer Function (s5-02) ← scales session observations to target cluster
Layer 3: CALIBRATION (this task)   ← corrects systematic bias over time
         + PROJECTION (batch)      ← per-run → daily/monthly/yearly
         + PROJECTION (streaming)  ← continuous cost + growth scenarios
```

### Files to Read

```
tasks/docs/ml-architecture.md       # Three-layer design rationale
src/burnt/estimators/ml.py          # CostRegressor.predict()
src/burnt/estimators/transfer.py    # TransferredMetrics
src/burnt/core/pricing.py           # get_dbu_rate()
src/burnt/core/instances.py         # InstanceSpec, vm_cost_per_hour
src/burnt/core/models.py            # ClusterConfig
```

---

## Specification

### Module 1: `src/burnt/estimators/projection.py`

```python
@dataclass(frozen=True)
class JobCostProjection:
    """Cost projection for a scheduled Databricks batch job."""
    # Per-run
    per_run_dbu: float
    per_run_dbu_usd: float
    per_run_vm_usd: float | None    # None for serverless
    per_run_total_usd: float
    confidence: str
    # Schedule
    runs_per_day: float
    # DBU cost
    daily_dbu: float
    daily_dbu_usd: float
    monthly_dbu_usd: float
    yearly_dbu_usd: float
    # VM infrastructure (classic only)
    daily_vm_usd: float | None
    monthly_vm_usd: float | None
    yearly_vm_usd: float | None
    # Total Cost of Ownership
    daily_total_usd: float
    monthly_total_usd: float
    yearly_total_usd: float
    # Metadata
    sku_name: str
    instance_type: str | None
    num_workers: int
    photon_enabled: bool
    # Comparison to source (if Mode A)
    source_per_run_usd: float | None
    savings_pct: float | None


def project_job_cost(
    per_run_dbu: float,
    target_cluster: ClusterConfig,
    runs_per_day: float = 1.0,
    confidence: str = "medium",
    estimated_runtime_minutes: float | None = None,
    source_cost_usd: float | None = None,
) -> JobCostProjection:
    """Project per-run DBU to daily/monthly/yearly cost.

    VM cost = (num_workers + 1 driver) × vm_cost_per_hour × runtime_hours
    Constants: 30.44 avg days/month, 365 days/year
    """
```

### Module 2: `src/burnt/estimators/calibration.py`

```python
CALIBRATION_PATH = Path("~/.burnt/calibration.json").expanduser()

@dataclass
class CalibrationState:
    scaling_efficiency: float = 0.8     # Amdahl's coefficient
    bias_correction: float = 1.0        # multiplicative bias
    observations: int = 0
    last_updated: str | None = None

    def save(self, path: Path = CALIBRATION_PATH) -> None: ...

    @classmethod
    def load(cls, path: Path = CALIBRATION_PATH) -> "CalibrationState": ...


def update_calibration(
    predicted_dbu: float,
    actual_dbu: float,
    current: CalibrationState,
    alpha: float = 0.1,
) -> CalibrationState:
    """EMA update. scaling_efficiency clamped [0.5, 1.0], bias_correction clamped [0.5, 2.0]."""


def apply_calibration(raw_prediction: float, calibration: CalibrationState) -> float:
    return raw_prediction * calibration.bias_correction
```

#### CLI: `burnt calibrate`

```bash
uv run burnt calibrate --job-id 12345 --predicted-dbu 0.45 --warehouse-id sql-xxxx
# Fetches actual DBU from system.billing.usage, updates ~/.burnt/calibration.json
```

### Module 3: `src/burnt/estimators/streaming.py`

Streaming cost has two orthogonal components (unlike batch where cost ∝ duration):

```
compute_cost_per_hour = (dbu_per_hour × dbu_rate) + (num_nodes × vm_cost_per_hour)
processing_cost_per_gb = total_state_operations_cost / total_gb_processed

monthly_cost = compute_cost_per_hour × 24 × 30.44
             + processing_cost_per_gb × monthly_gb_throughput
```

```python
@dataclass
class StreamingProfile:
    trigger_interval_seconds: float         # 0 = continuous, >0 = microbatch
    avg_micro_batch_duration_seconds: float
    avg_records_per_second: float
    avg_bytes_per_second: float
    state_store_size_gb: float              # 0 if stateless
    checkpointing_enabled: bool
    upstream_topic_count: int               # Kafka/Event Hubs topics
    is_dlt_pipeline: bool
    dlt_sku: str | None                     # "DLT_PRO" | "DLT_ADVANCED" | None


@dataclass
class StreamingCostProjection:
    cost_per_hour_usd: float
    cost_per_gb_usd: float
    monthly_cost_usd: float                 # at current throughput
    monthly_cost_usd_low: float             # at 0.5× throughput growth
    monthly_cost_usd_high: float            # at 2.0× throughput growth
    compute_cost_pct: float
    data_cost_pct: float
    state_store_cost_usd_per_month: float   # Azure Blob hot storage: $0.018/GB/month
    annual_cost_usd: float
    growth_sensitivity: str                 # "low" | "medium" | "high"
    # growth_sensitivity: low = data_cost_pct < 20%, medium = 20-60%, high = >60%


def project_streaming_cost(
    cluster: ClusterConfig,
    profile: StreamingProfile,
    monthly_gb_throughput: float,
    growth_factor: float = 1.0,
) -> StreamingCostProjection:
    ...
```

DLT pipelines use `DLT_PRO`/`DLT_ADVANCED` DBU rate when `profile.is_dlt_pipeline` is True.

### End-to-End Flow

```python
def advise_current_session(
    target_cluster: ClusterConfig | str | None = None,
    target_workers: int | None = None,
    target_sku: str = "JOBS_COMPUTE",
    runs_per_day: float = 1.0,
) -> JobCostProjection:
    """Full pipeline: listener → transfer → model → calibrate → project."""
```

---

## Acceptance Criteria

### Projection

- [ ] `JobCostProjection` dataclass with all cost breakdowns
- [ ] `project_job_cost()` computes DBU + VM + total for daily/monthly/yearly
- [ ] VM cost = `(workers + 1) × vm_cost_per_hour × runtime_hours` for classic; `None` for serverless
- [ ] `savings_pct` computed when `source_cost_usd` provided
- [ ] Runtime derived from DBU when `estimated_runtime_minutes` not provided
- [ ] `burnt estimate-job` CLI command with `--runs-per-day`

### Calibration

- [ ] `CalibrationState` with save/load to `~/.burnt/calibration.json`
- [ ] `update_calibration()` via EMA with α=0.1
- [ ] `scaling_efficiency` clamped to [0.5, 1.0], `bias_correction` clamped to [0.5, 2.0]
- [ ] `apply_calibration()` multiplies raw prediction by bias_correction
- [ ] `burnt calibrate` CLI command

### Streaming

- [ ] `src/burnt/estimators/streaming.py` with `project_streaming_cost()` and both models
- [ ] `StreamingProfile` and `StreamingCostProjection` in `src/burnt/core/models.py`
- [ ] DLT pipelines use correct SKU rate
- [ ] `monthly_cost_usd_low` at 0.5× throughput; `monthly_cost_usd_high` at 2.0×
- [ ] `growth_sensitivity` classified by `data_cost_pct` thresholds
- [ ] `state_store_cost_usd_per_month` uses $0.018/GB/month
- [ ] `burnt.project_streaming_cost()` exported from `src/burnt/__init__.py`
- [ ] `StreamingCostProjection.display()` shows fixed vs variable cost breakdown and growth scenarios

### Integration

- [ ] `advise_current_session()` wires all three layers + projection
- [ ] Falls back gracefully when model unavailable (heuristic pipeline only)
- [ ] Falls back when calibration file doesn't exist (`bias_correction = 1.0`)

### Tests

- [ ] `tests/unit/estimators/test_projection.py` — daily/monthly/yearly arithmetic, VM cost vs None for serverless, savings %
- [ ] `tests/unit/estimators/test_calibration.py` — EMA convergence, clamping, save/load round-trip, missing file → defaults
- [ ] `tests/unit/estimators/test_streaming.py` — stateless, stateful, DLT pipeline, high-growth scenario
- [ ] All existing tests pass
- [ ] Lint passes: `uv run ruff check src/ tests/`

---

## Verification

```bash
uv run pytest -m unit -v tests/unit/estimators/test_projection.py
uv run pytest -m unit -v tests/unit/estimators/test_calibration.py
uv run pytest -m unit -v tests/unit/estimators/test_streaming.py
uv run pytest -m unit -v 2>&1 | tail -5
uv run ruff check src/ tests/
```

### Integration Check

- [ ] `burnt estimate-job --help` shows all flags
- [ ] `burnt calibrate --help` shows all flags
- [ ] `burnt.advise_current_session(target_sku="JOBS_COMPUTE", runs_per_day=24)` returns `JobCostProjection`
- [ ] `project_streaming_cost(cluster, profile, monthly_gb_throughput=720)` returns projection with all fields populated

---

## Handoff

```yaml
status: todo
```
