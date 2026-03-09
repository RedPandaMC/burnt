# Task: Create Benchmark Dataset for Accuracy Validation

---

## Metadata

```yaml
id: s2-03-benchmark-dataset
status: todo
sprint: 2
priority: high
agent: ~
blocked_by: []
created_by: planner
```

---

## Context

### Goal

263 tests verify types and edge cases. Zero tests verify estimate accuracy. Without a benchmark dataset, there is no way to tell if code changes improve or degrade accuracy. This task creates:
- `tests/benchmarks/` directory with reference queries, known EXPLAIN outputs, and expected cost ranges
- Sanity/monotonicity unit tests (no live Databricks needed)
- Integration test fixtures (skipped unless env vars set)
- Property-based tests using Hypothesis (already in dev deps)

This benchmark infrastructure must be in place before any estimation task can be marked "complete".

### Files to read (executor reads ONLY these)

```
# Required
tests/unit/estimators/test_static.py
tests/unit/estimators/test_hybrid.py
tests/conftest.py
src/burnt/estimators/static.py
src/burnt/estimators/hybrid.py
src/burnt/core/models.py

# Reference
files/06-TESTING-STRATEGY.md    # Full test strategy, accuracy targets, Hypothesis examples
files/07-AGENT-WORKFLOW.md      # §7.4 benchmarking asset spec
```

### Background

**Accuracy targets (from `files/06-TESTING-STRATEGY.md §6.3`):**
- Sprint: All estimates within **10×** of actual
- Sprint: Within **3×** of actual
- Sprint (ML): Within **2×** of actual

**Required benchmark structure:**
```
tests/benchmarks/
├── README.md
├── queries/
│   ├── simple_select.sql
│   ├── groupby_agg.sql
│   ├── two_table_join.sql
│   ├── five_table_join.sql
│   └── merge_into.sql
├── explain_outputs/
│   ├── simple_select.txt        # Real EXPLAIN COST output (can be synthetic for now)
│   └── groupby_agg.txt
├── expected_costs.json          # Known actual cost ranges (from billing or literature)
└── conftest.py                  # Fixtures + parametrized tests
```

**Sanity tests (no Databricks needed):**
```python
# Cost monotonicity: more complex query should cost more
def test_five_way_join_costs_more_than_simple_select(): ...
def test_adding_join_increases_cost(): ...
def test_larger_cluster_increases_cost(): ...

# Order of magnitude sanity
def test_simple_select_under_one_dbu(): ...
def test_full_table_scan_1tb_over_point_one_dbu(): ...
```

**Property-based tests using Hypothesis** (already in dev deps, currently unused):
```python
from hypothesis import given, strategies as st

@given(complexity=st.integers(min_value=1, max_value=500))
def test_estimate_is_positive(complexity): ...

@given(size_bytes=st.integers(min_value=0, max_value=10**15))
def test_larger_scan_produces_higher_estimate(size_bytes): ...

@given(sql=st.text(min_size=1))
def test_normalize_idempotent(sql): ...
```

**Integration fixtures (skip if no env vars):**
```python
@pytest.fixture
def databricks_client():
    settings = Settings()
    if not settings.workspace_url:
        pytest.skip("No Databricks connection (set BURNT_WORKSPACE_URL)")
    return DatabricksClient(settings)
```

---

## Acceptance Criteria

- [ ] `tests/benchmarks/` directory created with README.md
- [ ] At least 5 reference SQL queries in `tests/benchmarks/queries/`
- [ ] `tests/benchmarks/expected_costs.json` with cost ranges for each query (can use conservative ranges like `[0.000001, 0.01]` for simple SELECT based on R1 research)
- [ ] `tests/benchmarks/conftest.py` loads fixtures and defines `@pytest.mark.benchmark` marker
- [ ] At least 5 monotonicity/sanity tests that pass with fixed estimator (after p4a-01)
- [ ] At least 3 property-based tests using Hypothesis (`normalize_idempotent`, `larger_scan_higher_cost`, `estimate_is_positive`)
- [ ] `tests/integration/conftest.py` with `databricks_client` fixture that skips when no env vars
- [ ] All new tests pass: `uv run pytest -m "unit or benchmark" -v`
- [ ] `uv run ruff check tests/` zero errors

---

## Verification

### Commands

```bash
uv run pytest -m "unit or benchmark" -v
uv run pytest tests/benchmarks/ -v
uv run ruff check tests/
# Property-based (may take a moment)
uv run pytest tests/benchmarks/ -k "hypothesis or property" -v
```

### Expected output

All benchmark and unit tests pass. Sanity checks confirm the fixed estimator produces reasonable values.

---

## Handoff

### Result

[Executor fills this in when done.]

```
status: todo
```

### Blocked reason

Blocked by `p4a-01-critical-bug-fixes` — monotonicity tests will fail against the unfixed quadratic formula.
