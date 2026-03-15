# Task: Cost Regression Model — HistGBR on Normalised Features

---

## Metadata

```yaml
id: s5-03-cost-regressor
status: todo
sprint: 5
priority: high
agent: ~
blocked_by: [s5-01-feature-extraction, s2-03-benchmark-dataset]
created_by: planner
```

---

## Context

### Goal

Implement `src/burnt/estimators/ml.py` — the **Layer 1 Base Model** of the three-layer architecture. A `HistGradientBoostingRegressor` trained on normalised features from historical `system.query.history` data, predicting `log1p(dbu)`. Per-workspace, serialised to `~/.burnt/cost_model.joblib`.

This model learns structural cost relationships from historical data. It does NOT handle cluster-to-cluster transfer (that's s5-02) or cost projection (that's s5-04). It answers: "given these normalised query/cluster features, what's the expected DBU consumption?"

### Architecture Position

```
Layer 1: BASE MODEL (this task)     ← learns structural relationships
Layer 2: Transfer Function (s5-02)  ← scales observations to target cluster
Layer 3: Calibration (s5-04)        ← corrects systematic bias over time
```

### Files to Read

```
tasks/r7-ml-architecture-v4.md              # Three-layer design
tasks/s5-01-feature-extraction.md            # QueryFeatures, MONOTONIC_CONSTRAINTS
src/burnt/estimators/features.py             # extract_features(), to_vector(), prune_features()
src/burnt/core/models.py                     # ClusterConfig, CostEstimate
src/burnt/core/config.py                     # Settings
```

---

## Specification

### CostRegressor

```python
class CostRegressor:
    """Layer 1: Base cost regression model.
    
    Predicts log1p(dbu) from normalised QueryFeatures.
    Trained per-workspace on <1K historical records.
    Uses HistGradientBoostingRegressor with monotonic constraints.
    Two-pass feature pruning reduces 34 → ~15-20 effective features.
    
    Training modes:
        - From xlsx: burnt train-model --from-xlsx masked_data.xlsx
        - In-workspace: burnt train-model --warehouse-id sql-xxxx --days 90
    
    Model artifact saved to ~/.burnt/cost_model.joblib containing:
        - model: trained HistGradientBoostingRegressor
        - kept_feature_indices: list[int] (after pruning)
        - kept_feature_names: list[str]
        - metadata: dict (n_samples, r2, overfit_gap, residuals, etc.)
        - calibration: dict (scaling_efficiency, bias_correction)
    """
    
    def train(
        self,
        features_list: list[QueryFeatures],
        dbu_labels: list[float],
    ) -> dict:
        """Train with two-pass feature pruning.
        
        Pass 1: Fit on all 34 features → compute feature_importances_
        Pass 2: Drop features with importance < 0.02 → refit
        
        Validation: RepeatedKFold (5-fold, 10 repeats).
        Target: log1p(dbu).
        
        Returns dict with: n_samples, n_features_pruned, median_r2,
            p10_r2, overfit_gap, residual_p50, residual_p90.
        """
    
    def predict(self, features: QueryFeatures) -> tuple[float, str]:
        """Predict DBU from normalised features.
        
        Applies saved feature mask from pruning.
        Returns (predicted_dbu, confidence).
        """
    
    def predict_batch(self, features_list: list[QueryFeatures]) -> list[tuple[float, str]]:
        """Batch prediction for multi-task jobs / notebooks."""
    
    def is_available(self) -> bool:
        """True if trained model file exists at model_path."""
```

### Model Hyperparameters (Optimised for <1K Samples)

```python
model = HistGradientBoostingRegressor(
    max_iter=100,
    max_depth=4,              # shallow trees
    min_samples_leaf=20,      # ~2% of dataset
    l2_regularization=1.0,    # heavy regularisation
    learning_rate=0.05,       # slow learning
    monotonic_cst=MONOTONIC_CONSTRAINTS,  # from s5-01
    random_state=42,
)
```

### Training Data Ingestion

```python
def load_training_data_from_xlsx(
    xlsx_path: str | Path,
) -> tuple[list[QueryFeatures], list[float]]:
    """Load from masked xlsx export.
    
    Expects sheets matching system.query.history + system.billing.usage.
    Joins on warehouse_id + time window.
    Extracts features from statement_text (AST) + runtime metrics.
    Normalises metrics as ratios (same as inference-time).
    Filters: FINISHED, not cached, usage_quantity > 0.
    
    For observed metrics: uses query.history's read_bytes etc. directly
    (the cluster that ran the query IS the source cluster).
    """

def load_training_data_from_workspace(
    backend: Backend,
    warehouse_id: str,
    days: int = 90,
) -> tuple[list[QueryFeatures], list[float]]:
    """Load from live workspace.
    
    Queries system.query.history + system.billing.usage.
    Runs DESCRIBE DETAIL on referenced tables for Tier 2 features.
    """
```

### Validation Strategy

```python
cv = RepeatedKFold(n_splits=5, n_repeats=10, random_state=42)
scores = cross_validate(model, X, y_log, cv=cv,
    scoring=["r2", "neg_mean_absolute_error"],
    return_train_score=True)

# Acceptance gates:
# median R² > 0.80  (target: 0.85)
# p10 R² > 0.55     (guards against lucky folds)
# overfit gap < 0.15 (train_r2 - test_r2)
```

### CLI Commands

```bash
# Train from masked xlsx
uv run burnt train-model --from-xlsx masked_data.xlsx

# Train from live workspace  
uv run burnt train-model --warehouse-id sql-xxxx --days 90

# Show model info
uv run burnt model-info
# Output: n_samples, n_features, median_r2, trained_at, model_path
```

---

## Acceptance Criteria

- [ ] `src/burnt/estimators/ml.py` created
- [ ] `CostRegressor` with `train()`, `predict()`, `predict_batch()`, `is_available()`
- [ ] Two-pass feature pruning: 34 → ~15-20 (threshold 0.02)
- [ ] `kept_feature_indices` saved in model artifact
- [ ] `HistGradientBoostingRegressor` with `MONOTONIC_CONSTRAINTS`
- [ ] Target transform: `log1p(dbu)` / `expm1` inverse
- [ ] `RepeatedKFold` validation (5-fold, 10 repeats) with metrics
- [ ] Warnings logged when R² < 0.60 or overfit gap > 0.15
- [ ] `load_training_data_from_xlsx()` for masked data
- [ ] `load_training_data_from_workspace()` for live training
- [ ] Model saved to `~/.burnt/cost_model.joblib` (dir auto-created)
- [ ] `train-model` CLI with `--from-xlsx` and `--warehouse-id` modes
- [ ] `model-info` CLI command
- [ ] Import guard for sklearn
- [ ] New unit tests: `tests/unit/estimators/test_ml.py`
  - `is_available()` when no model file
  - `predict()` with mock model artifact (with feature mask)
  - `train()` on synthetic data (R² > 0 on obvious signal)
  - Feature pruning drops low-importance features
  - log1p/expm1 round-trip
  - xlsx loading with mock data
  - Graceful sklearn import failure
- [ ] All existing tests pass
- [ ] Lint passes

---

## Verification

```bash
uv run pytest -m unit -v tests/unit/estimators/test_ml.py
uv run pytest -m unit -v 2>&1 | tail -5
uv run ruff check src/ tests/

# With ml extra:
uv sync --extra ml
uv run burnt train-model --help
uv run burnt model-info --help
```

---

## Handoff

```yaml
status: todo
```
