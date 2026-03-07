# Task: Implement ML cost bucket classification model

---

## Metadata

```yaml
id: p6-02-classification-model
status: todo
phase: 6
priority: medium
agent: ~
blocked_by: [p6-01-feature-extraction]
created_by: planner
```

---

## Context

### Goal

Implement `src/dburnrate/estimators/ml.py` — a cost bucket classifier using sklearn that integrates as an optional fourth signal in `HybridEstimator`. The model is trained on `system.query.history` data and serialized to disk. When the `[ml]` extra is installed and a trained model exists, `HybridEstimator` uses it to refine the estimate.

### Files to read

```
docs/ml-cost-model-research.md          (from p6-00 — model choice, bucket boundaries)
src/dburnrate/estimators/features.py    (from p6-01 — QueryFeatures, to_vector)
src/dburnrate/estimators/hybrid.py
src/dburnrate/core/models.py            # CostEstimate
src/dburnrate/tables/queries.py         # QueryRecord (training data)
src/dburnrate/core/config.py            # Settings
```

### Background

**Cost buckets** (from research doc):
```python
class CostBucket(str, Enum):
    LOW = "low"           # < 0.1 DBU
    MEDIUM = "medium"     # 0.1–1.0 DBU
    HIGH = "high"         # 1.0–10.0 DBU
    VERY_HIGH = "very_high"  # > 10.0 DBU
```

**Model implementation:**

```python
# src/dburnrate/estimators/ml.py
# Requires: pip install dburnrate[ml]

from sklearn.ensemble import HistGradientBoostingClassifier
import joblib
from pathlib import Path

DEFAULT_MODEL_PATH = Path("~/.dburnrate/cost_model.joblib").expanduser()

class CostBucketClassifier:
    def __init__(self, model_path: Path = DEFAULT_MODEL_PATH): ...

    def train(self, records: list[QueryRecord], clusters: list[ClusterConfig]) -> None:
        """Train from historical records. Saves model to model_path."""
        ...

    def predict(self, features: QueryFeatures) -> tuple[CostBucket, float]:
        """Returns (bucket, confidence_score 0–1)."""
        ...

    def is_available(self) -> bool:
        """True if trained model file exists."""
        ...
```

**CLI command for training:**
```bash
uv run dburnrate train-model --warehouse-id sql-xxxx --days 90
```

This fetches 90 days of `system.query.history`, extracts features for each record (using cached EXPLAIN if possible, otherwise zero-fill), labels by DBU bucket, trains classifier, saves to `~/.dburnrate/cost_model.joblib`.

**Integration in HybridEstimator:**

```python
# In hybrid.py — add optional ML signal
if self._ml_classifier and self._ml_classifier.is_available():
    bucket, ml_confidence = self._ml_classifier.predict(features)
    # Use to validate/refine estimate: if bucket contradicts static/explain signal, lower confidence
```

**Graceful import:**

```python
# In ml.py
from .._compat import require

def _require_sklearn():
    return require("sklearn", "ml")
```

---

## Acceptance Criteria

- [ ] `src/dburnrate/estimators/ml.py` created (only importable with `[ml]` extra)
- [ ] `CostBucket` enum with 4 buckets (boundary values from research doc)
- [ ] `CostBucketClassifier.train()` — trains `HistGradientBoostingClassifier`, saves with `joblib`
- [ ] `CostBucketClassifier.predict()` — returns `(CostBucket, float)`, loads model lazily
- [ ] `CostBucketClassifier.is_available()` — checks model file exists
- [ ] `train-model` CLI command added to `cli/main.py`
  - `--warehouse-id` required
  - `--days` optional (default 90)
  - `--model-path` optional (default `~/.dburnrate/cost_model.joblib`)
- [ ] `HybridEstimator` uses `CostBucketClassifier` when available (optional init arg)
- [ ] Import guarded: `ImportError` with helpful message if sklearn not installed
- [ ] Model directory (`~/.dburnrate/`) created automatically if missing
- [ ] New unit tests: `tests/unit/estimators/test_ml.py`
  - Test `CostBucket` boundaries
  - Test `is_available()` when no model file
  - Test `predict()` with mock model
  - Test graceful import failure
- [ ] All existing tests still pass
- [ ] `uv run ruff check src/ tests/` exits 0

---

## Verification

```bash
uv run pytest -m unit -v tests/unit/estimators/test_ml.py
uv run pytest -m unit -v 2>&1 | tail -5
uv run ruff check src/ tests/
uv run ruff format --check src/ tests/
# With ml extra:
uv sync --extra ml
uv run dburnrate train-model --help
```

---

## Handoff

### Result

[Executor: fill in after completion]

### Blocked reason

[If blocked, explain here]
