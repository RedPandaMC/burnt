# Task: ML Feature Extraction v4 — Dual-Mode with Normalised Ratios

---

## Metadata

```yaml
id: s5-01-feature-extraction
status: todo
sprint: 5
priority: high
agent: ~
blocked_by: [s5-00-listener-infrastructure, s3-01-delta-scan-integration]
created_by: planner
revision: 4 (dual-mode + normalised ratios for transfer learning)
revision_date: 2026-03-14
```

---

## Context

### Goal

Implement `src/burnt/estimators/features.py` — the feature extraction module that produces a normalised, cluster-invariant feature vector from either:

- **Mode A (In-Session):** Live `SessionMetrics` from sparkMeasure/listener + AST + table stats + source/target cluster config
- **Mode B (Cold/CLI):** AST + table stats + target cluster config only (no observed metrics)

The key design principle: **features are normalised ratios**, not raw metrics. This ensures the base model trained on historical data generalises to live sessions on different clusters without distribution shift.

### Why Normalised Ratios

A query scanning 50GB on 4 workers produces `bytes_per_worker = 12.5GB`. The same query on 8 workers produces `bytes_per_worker = 6.25GB`. But the total DBU consumed is roughly the same (same total work, just parallelised differently). By training on `bytes_per_worker`, `shuffle_ratio`, `spill_ratio`, and `selectivity` instead of raw bytes, the model learns cluster-invariant cost relationships.

### Files to Read

```
tasks/r7-ml-architecture-v4.md              # Three-layer architecture
tasks/s5-00-listener-infrastructure.md       # SessionMetrics, CapturedStageMetrics
src/burnt/runtime/listener.py                # SessionMetrics dataclass
src/burnt/parsers/sql.py                     # SQLGlot: analyze_query(), extract_tables()
src/burnt/parsers/pyspark.py                 # PySpark AST: analyze_pyspark()
src/burnt/parsers/delta.py                   # DeltaTableInfo, parse_describe_detail()
src/burnt/parsers/antipatterns.py            # Anti-pattern detection signals
src/burnt/core/models.py                     # ClusterConfig, QueryProfile
src/burnt/core/instances.py                  # InstanceSpec, AzureInstanceCatalog
```

---

## Specification

### Feature Vector: 4 Groups

#### Group 1: AST Features (12 features — always available)

| # | Feature | Type | Monotonic | Extraction |
|---|---------|------|-----------|------------|
| 0 | `table_count` | int | +1 | `len(extract_tables(sql))` |
| 1 | `join_count` | int | +1 | Count JOIN nodes in AST |
| 2 | `join_type_max_weight` | float | +1 | max(Cartesian=2.0, SortMerge=0.5, Broadcast=0.1) |
| 3 | `agg_count` | int | +1 | GROUP BY + aggregate function count |
| 4 | `window_function_count` | int | +1 | OVER() clause count |
| 5 | `subquery_count` | int | +1 | Nested SELECT count |
| 6 | `filter_count` | int | 0 | WHERE/HAVING predicate count |
| 7 | `has_order_by_no_limit` | int | +1 | Global sort detected (0/1) |
| 8 | `has_cross_join` | int | +1 | CROSS JOIN in AST (0/1) |
| 9 | `has_udf` | int | +1 | UDF calls detected (0/1) |
| 10 | `has_distinct` | int | +1 | DISTINCT keyword (0/1) |
| 11 | `write_operation` | int | 0 | 0=read, 1=INSERT, 2=MERGE, 3=CTAS |

#### Group 2: Table Statistics (6 features — require DESCRIBE DETAIL)

| # | Feature | Type | Monotonic | Extraction |
|---|---------|------|-----------|------------|
| 12 | `log_total_table_bytes` | float | +1 | `log1p(sum of all table sizes)` |
| 13 | `log_max_table_bytes` | float | +1 | `log1p(largest table)` |
| 14 | `table_count_above_1gb` | int | +1 | Count tables > 1GB |
| 15 | `has_partition_filter` | int | 0 | WHERE references partition col (0/1) |
| 16 | `selectivity_estimate` | float | 0 | `estimated_scan / total_table_bytes` (0.0–1.0) |
| 17 | `log_estimated_scan_bytes` | float | +1 | `log1p(sum(table_size × selectivity))` |

#### Group 3: Normalised Observed Metrics (8 features — Mode A only, zero in Mode B)

| # | Feature | Type | Monotonic | Extraction |
|---|---------|------|-----------|------------|
| 18 | `log_bytes_per_worker` | float | +1 | `log1p(input_bytes / num_workers)` |
| 19 | `shuffle_ratio` | float | +1 | `shuffle_read / max(input_bytes, 1)` |
| 20 | `spill_ratio` | float | +1 | `disk_spill / max(input_bytes, 1)` |
| 21 | `memory_spill_ratio` | float | +1 | `memory_spill / max(input_bytes, 1)` |
| 22 | `gc_time_fraction` | float | +1 | `gc_time / max(executor_run_time, 1)` (sparkMeasure only) |
| 23 | `cpu_utilisation` | float | 0 | `cpu_time / max(executor_run_time, 1)` (sparkMeasure only) |
| 24 | `log_exec_time_per_worker` | float | +1 | `log1p(executor_run_time / num_workers)` |
| 25 | `tasks_per_worker` | float | 0 | `num_tasks / num_workers` |

#### Group 4: Target Cluster + Transfer Signals (8 features)

| # | Feature | Type | Monotonic | Extraction |
|---|---------|------|-----------|------------|
| 26 | `target_num_workers` | int | 0 | Target cluster config |
| 27 | `target_dbu_per_hour` | float | +1 | Target instance DBU rate |
| 28 | `target_photon_enabled` | int | 0 | 0/1 |
| 29 | `target_sku_encoded` | int | 0 | ordinal(JOBS=0, AP=1, SQL=2, DLT=3) |
| 30 | `target_memory_gb` | float | 0 | From instance catalog |
| 31 | `worker_ratio` | float | 0 | `target_workers / max(source_workers, 1)` (1.0 in Mode B) |
| 32 | `dbu_rate_ratio` | float | 0 | `target_dbu_rate / max(source_dbu_rate, 0.01)` (1.0 in Mode B) |
| 33 | `mode_flag` | int | 0 | 1 = session (Mode A), 0 = cold (Mode B) |

**Total: 34 features.** Feature pruning during training (s5-03) reduces to ~15-20 effective features.

### Selectivity Estimation

```python
def estimate_selectivity(
    table_name: str,
    table_info: DeltaTableInfo,
    filters: list[FilterPredicate],
) -> float:
    """Estimate scan fraction (0.0–1.0). Conservative: overestimates scan.
    
    - Equality on partition column: 1 / num_partitions (if known)
    - Range on partition column: 0.3
    - Equality on non-partition: 0.1
    - Range on non-partition: 0.3
    - LIKE: 0.2
    - No filter: 1.0 (full scan)
    - Multiple filters: multiply (independence assumption)
    - Floor: 0.01, Cap: 1.0
    """
```

### Main Extraction Function

```python
def extract_features(
    statement_text: str,
    target_cluster: ClusterConfig,
    *,
    language: str = "sql",
    session_metrics: SessionMetrics | None = None,
    source_cluster: ClusterConfig | None = None,
    delta_tables: dict[str, DeltaTableInfo] | None = None,
    instance_spec: InstanceSpec | None = None,
) -> QueryFeatures:
    """Extract normalised feature vector for ML prediction.
    
    Mode A (session_metrics provided): Uses observed metrics from listener,
        normalised as ratios. source_cluster is the current interactive cluster.
    
    Mode B (no session_metrics): Uses AST + table stats only.
        Observed metric features are zero-filled. mode_flag = 0.
    """
```

### Feature Importance Pruning

```python
def prune_features(
    X: np.ndarray, y: np.ndarray, threshold: float = 0.02,
) -> tuple[np.ndarray, list[int], list[str]]:
    """Two-pass importance pruning. Train on all 34 → drop < threshold → return mask."""
```

---

## Acceptance Criteria

- [ ] `src/burnt/estimators/features.py` created
- [ ] `QueryFeatures` dataclass with 34 fields (frozen, slots)
- [ ] `FEATURE_NAMES` constant — 34 names matching `to_vector` order
- [ ] `MONOTONIC_CONSTRAINTS` constant — 34 values
- [ ] `SKU_ENCODING` dict covering all Databricks SKU strings
- [ ] `extract_features()` supports Mode A (with SessionMetrics) and Mode B (without)
- [ ] Normalised ratios: `shuffle_ratio`, `spill_ratio`, `gc_time_fraction`, `cpu_utilisation`, `bytes_per_worker`
- [ ] Transfer signals: `worker_ratio`, `dbu_rate_ratio`, `mode_flag`
- [ ] `estimate_selectivity()` with partition-aware heuristics
- [ ] `prune_features()` for importance-based selection during training
- [ ] Zero-fill for Mode B (all observed metrics = 0, mode_flag = 0, ratios = 1.0)
- [ ] PySpark support via `analyze_pyspark()` for Group 1 features
- [ ] New unit tests: `tests/unit/estimators/test_features.py`
  - Mode A: full SQL with SessionMetrics + table stats + source/target cluster
  - Mode B: SQL with table stats only (no SessionMetrics)
  - Normalised ratios computed correctly (shuffle_ratio, spill_ratio, etc.)
  - Selectivity estimation with partition filters vs no filters
  - All-zero inputs produce valid features
  - Vector length == 34
  - mode_flag correctly set per mode
  - worker_ratio and dbu_rate_ratio computed correctly
- [ ] All existing tests pass
- [ ] Lint passes

---

## Verification

```bash
uv run pytest -m unit -v tests/unit/estimators/test_features.py
uv run pytest -m unit -v 2>&1 | tail -5
uv run ruff check src/ tests/
```

---

## Handoff

```yaml
status: todo
```
