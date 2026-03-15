# Task: EstimationTrace — Tier-by-Tier Estimation Transparency

---

## Metadata

```yaml
id: s3-09-estimation-trace
status: todo
phase: 3
priority: medium
agent: ~
blocked_by: [s3-01-delta-scan-integration]
created_by: planner
```

---

## Context

### Goal

When `burnt.estimate(sql)` in connected mode returns `confidence="medium"`, the user has
no idea why. Did the EXPLAIN fail because the warehouse was stopped? Did the historical
lookup find no matches? Or did it only run static analysis? Without this information,
debugging estimation quality is guesswork. Add an `EstimationTrace` dataclass that
records which tiers were attempted, which succeeded, which failed and why, which data
sources were queried, and how long each took. Attach it to `CostEstimate`.

### Files to read

```
# Required
src/burnt/estimators/pipeline.py   ← 4-tier orchestrator
src/burnt/estimators/hybrid.py
src/burnt/estimators/static.py
src/burnt/core/models.py           ← CostEstimate

# Reference
DESIGN.md
tasks/s3-01-delta-scan-integration.md
tasks/s3-02-fingerprint-lookup.md
tasks/s3-03-pipeline-hardening.md
```

### Background

**`EstimationTrace` model:**

```python
@dataclass
class EstimationTrace:
    tiers_attempted: list[str]         # ["static", "delta", "explain", "historical"]
    tiers_succeeded: list[str]         # ["static", "delta"]
    tiers_failed: dict[str, str]       # {"explain": "warehouse stopped", "historical": "no match"}
    data_sources_used: list[str]       # ["system.billing.usage", "DESCRIBE DETAIL orders"]
    elapsed_ms: dict[str, float]       # {"static": 2.1, "delta": 340.5}
    confidence_explanation: str        # human-readable: "Delta metadata available, EXPLAIN unavailable"
```

**Display format (terminal):**
```
EstimationTrace:
  ✓ static (2ms)
  ✓ delta (340ms) — 3 tables resolved
  ✗ explain — warehouse 'analytics-wh' is stopped
  ✗ historical — no matching fingerprint (0 of 1,247 queries matched)
  Confidence: medium (delta metadata available, no EXPLAIN)
```

**Integration points:**

1. `EstimationPipeline.estimate()` — collect tier results and build `EstimationTrace` during execution
2. `CostEstimate` — add `trace: EstimationTrace | None = None` field
3. `CostEstimate.display()` — optionally show trace when `verbose=True` or when `confidence != "high"`

**Tier names:** `"static"`, `"delta"`, `"explain"`, `"historical"` — match the 4-tier nomenclature in DESIGN.md.

**Timing:** Use `time.perf_counter()` around each tier call. Record elapsed time in `elapsed_ms`.

**Failure capture:** Wrap each tier in try/except. Store exception message (not full traceback)
in `tiers_failed`. The pipeline continues to the next tier on failure (existing behavior — this
just makes the failure visible).

---

## Acceptance Criteria

- [ ] `EstimationTrace` dataclass exists in `src/burnt/core/models.py`
- [ ] `CostEstimate.trace: EstimationTrace | None` field added (default `None`)
- [ ] `EstimationPipeline.estimate()` populates `trace` with all 4 tier results
- [ ] `tiers_attempted` lists all tiers the pipeline tried (even if they failed)
- [ ] `tiers_succeeded` lists only tiers that completed without exception
- [ ] `tiers_failed` maps tier name → error message for failed tiers
- [ ] `elapsed_ms` records wall-clock time for each tier
- [ ] `data_sources_used` lists all system tables and `DESCRIBE DETAIL` calls made
- [ ] `confidence_explanation` is a human-readable string explaining the confidence level
- [ ] `estimate.trace` is accessible and not None after a connected-mode call
- [ ] `estimate.display(verbose=True)` shows the trace inline
- [ ] Unit tests cover: all-tiers-succeed case, mixed success/failure case, offline-only case (trace still present with just "static")
- [ ] All existing tests still pass
- [ ] Lint passes: `uv run ruff check src/ tests/`

---

## Verification

### Commands

```bash
uv run pytest -m unit -v -k "estimation_trace"
uv run ruff check src/ tests/
```

### Integration Check

- [ ] `e = burnt.estimate("SELECT ..."); print(e.trace)` — shows at least one tier ("static") with elapsed time. In connected mode, shows delta and/or explain tiers.

---

## Handoff

### Result

[Executor fills this in when done.]

```yaml
status: todo
```

### Blocked reason

Blocked on s3-01 (need wired tiers to have something to trace beyond static).
