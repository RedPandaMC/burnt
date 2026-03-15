# Task: Photon Eligibility Scoring

---

## Metadata

```yaml
id: s3-06-photon-eligibility
status: todo
phase: 3
priority: high
agent: ~
blocked_by: []
created_by: planner
```

---

## Context

### Goal

The simulation builder's `enable_photon()` currently applies a fixed 2.5× DBU multiplier
with an assumed "complex_join" query type. This is misleading for workloads where Photon
provides minimal benefit (simple appends, Python UDF-heavy queries). Add a Photon
eligibility scorer that analyses the query AST to compute a score 0–100, explains what
drives the score, and calibrates the `enable_photon()` simulation multiplier accordingly.
Offline AST analysis works without a backend; the data volume component (small data
penalty) requires Delta metadata.

### Files to read

```
# Required
src/burnt/parsers/sql.py
src/burnt/parsers/pyspark.py
src/burnt/parsers/antipatterns.py
src/burnt/estimators/whatif.py     ← enable_photon() implementation
src/burnt/core/models.py

# Reference
DESIGN.md
tasks/s3-01-delta-scan-integration.md
```

### Background

**Weight table (grounded from Databricks benchmarks):**

| Pattern | Weight | Direction |
|---------|--------|-----------|
| Hash/Sort-merge join | +15 per join | Positive |
| Aggregation (GROUP BY) | +10 | Positive |
| Window function | +10 | Positive |
| Columnar scan (wide table, >20 cols) | +8 | Positive |
| Filter pushdown eligible | +5 | Positive |
| Python UDF present | -20 | Negative |
| `collect()` / `toPandas()` | -15 | Negative |
| JDBC source | -10 | Negative |
| Simple append/INSERT only | -5 | Negative |
| Estimated data < 1GB (connected mode) | -10 | Negative |

Score clamped to [0, 100].

**Score → multiplier mapping:**
- Score ≥ 70: Use 2.5× DBU with label "likely 2–3× speedup"
- Score 40–69: Use 1.8× DBU with label "moderate benefit expected"
- Score < 40: Use 1.2× DBU with label "limited benefit — DBU cost increase may outweigh gain"

**Module location:** `src/burnt/estimators/photon.py`

```python
@dataclass
class PhotonEligibility:
    score: int                   # 0–100
    label: str                   # e.g. "likely 2–3× speedup on join-heavy operations"
    positive_factors: list[str]  # e.g. ["3 hash joins (+45)", "2 GROUP BY (+20)"]
    negative_factors: list[str]  # e.g. ["python_udf (-20)"]
    recommended: bool            # score >= 40
    dbu_multiplier: float        # calibrated based on score tier

def score_photon_eligibility(
    query_profile: QueryProfile,
    scan_bytes: int | None = None,  # None → offline mode (skip data volume check)
) -> PhotonEligibility:
    ...
```

**Integration with simulation builder:** When `enable_photon()` is called, compute
`PhotonEligibility` from the current query profile (available from the `CostEstimate`)
and use `eligibility.dbu_multiplier` instead of the fixed 2.5×. Display the eligibility
score and label in the simulation result.

---

## Acceptance Criteria

- [ ] `src/burnt/estimators/photon.py` exists with `score_photon_eligibility()` and `PhotonEligibility`
- [ ] All 10 weight table entries are implemented and unit-tested
- [ ] Score is clamped to [0, 100]
- [ ] Score → multiplier mapping (2.5×, 1.8×, 1.2×) is applied in `enable_photon()`
- [ ] `PhotonEligibility.positive_factors` and `negative_factors` list contributing patterns with their weights
- [ ] Offline mode: data volume check skipped when `scan_bytes is None`
- [ ] Connected mode: `scan_bytes < 1e9` applies -10 small-data penalty
- [ ] Simulation result from `enable_photon()` displays eligibility score and label
- [ ] Unit tests cover: join-heavy query (high score), UDF-heavy query (low score), append-only (low score), mixed
- [ ] All existing tests still pass
- [ ] Lint passes: `uv run ruff check src/ tests/`

---

## Verification

### Commands

```bash
uv run pytest -m unit -v -k "photon_eligibility"
uv run ruff check src/ tests/
```

### Integration Check

- [ ] `burnt.estimate("SELECT a.id, SUM(b.amount) FROM orders a JOIN items b ON a.id = b.order_id JOIN products p ON b.product_id = p.id GROUP BY a.id").simulate().cluster().enable_photon().compare()` — result should show Photon eligibility score ≥ 70 and use 2.5× multiplier.
- [ ] A query with only `INSERT INTO t SELECT * FROM s` should show score < 40 and use 1.2× multiplier.

---

## Handoff

### Result

[Executor fills this in when done.]

```yaml
status: todo
```

### Blocked reason

[If blocked, explain exactly what is missing.]
