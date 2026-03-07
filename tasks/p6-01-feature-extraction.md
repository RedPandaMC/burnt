# Task: Implement ML feature extraction from ExplainPlan, Delta, and ClusterConfig

---

## Metadata

```yaml
id: p6-01-feature-extraction
status: todo
phase: 6
priority: medium
agent: ~
blocked_by: [p6-00-research-ml-models, p4-02-delta-scan-size]
created_by: planner
```

---

## Context

### Goal

Implement `src/dburnrate/estimators/features.py` — a module that converts an `ExplainPlan`, optional `DeltaTableInfo` dict, and `ClusterConfig` into a flat numeric feature vector suitable for sklearn models.

### Files to read

```
docs/ml-cost-model-research.md        (from p6-00 — defines exact feature list)
src/dburnrate/parsers/explain.py       # ExplainPlan, PlanNode models
src/dburnrate/parsers/delta.py         # DeltaTableInfo model
src/dburnrate/core/models.py           # ClusterConfig, CostEstimate
src/dburnrate/tables/queries.py        # QueryRecord (for training label)
```

### Background

The feature extractor should produce a `QueryFeatures` dataclass and a `FeatureVector` (numpy array or plain list):

```python
@dataclass
class QueryFeatures:
    # From ExplainPlan
    operator_count: int
    scan_count: int
    join_count: int
    agg_count: int
    sort_count: int
    broadcast_join_count: int
    shuffle_join_count: int
    shuffle_count: int
    total_size_bytes: int
    max_node_cardinality: int

    # From DeltaTableInfo (aggregated)
    table_count: int
    delta_total_size_bytes: int
    delta_avg_file_count: float

    # From ClusterConfig
    num_workers: int
    dbu_per_hour: float
    photon_enabled: bool   # → 0/1
    # instance_family one-hot: is_ds, is_d, is_f, is_e, is_l (5 bools)

    # Derived
    bytes_per_worker: float   # total_size_bytes / max(num_workers, 1)
    joins_per_scan: float     # join_count / max(scan_count, 1)

def extract_features(
    plan: ExplainPlan | None,
    delta_tables: dict[str, DeltaTableInfo] | None,
    cluster: ClusterConfig,
) -> QueryFeatures: ...

def to_vector(features: QueryFeatures) -> list[float]:
    """Return ordered flat list suitable for sklearn input."""
    ...
```

All features must handle `None` plan gracefully (zero-fill). Document the column order in `to_vector()` with a `FEATURE_NAMES` constant — this must match the trained model's expected column order.

---

## Acceptance Criteria

- [ ] `src/dburnrate/estimators/features.py` created
- [ ] `QueryFeatures` dataclass with all fields from research doc
- [ ] `extract_features(plan, delta_tables, cluster)` — handles `None` plan (zero-fill)
- [ ] `to_vector(features)` — returns `list[float]`, length matches `len(FEATURE_NAMES)`
- [ ] `FEATURE_NAMES: list[str]` constant — one name per feature, in order
- [ ] Instance family one-hot encoding for top 5 families (ds, d, f, e, l prefix)
- [ ] `bytes_per_worker` and `joins_per_scan` derived features
- [ ] New unit tests: `tests/unit/estimators/test_features.py`
  - Test with full plan + delta + cluster
  - Test with None plan (zero-fill)
  - Test vector length == len(FEATURE_NAMES)
  - Test instance family encoding
- [ ] All existing tests still pass
- [ ] `uv run ruff check src/ tests/` exits 0

---

## Verification

```bash
uv run pytest -m unit -v tests/unit/estimators/test_features.py
uv run pytest -m unit -v 2>&1 | tail -5
uv run ruff check src/ tests/
uv run ruff format --check src/ tests/
```

---

## Handoff

### Result

[Executor: fill in after completion]

### Blocked reason

[If blocked, explain here]
