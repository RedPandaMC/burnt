# Task: Fix Remaining 4 Critical Bugs

---

## Metadata

```yaml
id: p4a-01b-remaining-bugs
status: todo
phase: 4A
priority: critical
agent: ~
blocked_by: []
created_by: planner
```

---

## Context

### Goal

Fix the remaining 4 bugs identified in the March 2026 audit that weren't in the original 7-bug fix task (p4a-01-critical-bug-fixes.md.completed).

### Files to read

```
# Required
src/burnt/estimators/static.py        # Bug 8
src/burnt/forecast/prophet.py         # Bug 9
src/burnt/cli/main.py                 # Bug 10
src/burnt/tables/                     # Bug 11

# Reference
files/01-CRITICAL-CODE-FIXES.md           # Full bug descriptions
files/00-EXECUTIVE-SUMMARY.md             # Audit findings
DESIGN.md                                 # §"Critical Bugs"
```

### Background

The original task (p4a-01) fixed bugs 1-7. These are the remaining issues:

**Bug 8 — SKU Inference (static.py):**
Current `_infer_sku()` uses fragile string matching:
```python
if "Standard_D" in cluster.instance_type:
    return "ALL_PURPOSE"
```
Misclassifies SQL Warehouses, serverless, DLT pipelines, and AWS/GCP instances.

**Bug 9 — Empty forecast/prophet.py Stub:**
File exists but contains no real implementation.

**Bug 10 — No Graceful CLI Degradation:**
If sqlglot isn't installed, `burnt estimate` crashes with ImportError instead of suggesting `uv sync --extra sql`.

**Bug 11 — Missing tables/attribution.py:**
Referenced in DESIGN.md but doesn't exist. Required for calibration, historical cost lookups, and ML training data.

---

## Acceptance Criteria

### Bug 8: Fix SKU Inference (estimators/static.py)
- [ ] Make SKU an explicit parameter in `ClusterConfig`
- [ ] Remove fragile string matching on `Standard_D` prefixes
- [ ] Add validation for valid SKU values
- [ ] Update CLI to accept `--sku` flag

### Bug 9: Fix Empty forecast/prophet.py Stub
- [ ] Either implement Prophet forecasting or mark as TODO/stub
- [ ] Add clear documentation if placeholder
- [ ] Update DESIGN.md to reflect current status

### Bug 10: Add Graceful CLI Degradation
- [ ] Add try/except around sqlglot import in CLI
- [ ] Show friendly error: "sqlglot required: pip install burnt[sql]"
- [ ] Or use lazy import with helpful message

### Bug 11: Create tables/attribution.py
- [ ] Implement billing × list_prices join
- [ ] Per-query attribution via warehouse_id + time overlap
- [ ] Lakeflow job-run cost attribution
- [ ] `get_historical_cost(fingerprint)` function for Tier 4 pipeline
- [ ] Wire into EstimationPipeline (p4a-02)

### General Requirements
- [ ] All public functions have type hints and docstrings
- [ ] `uv run pytest -m unit -v` passes
- [ ] `uv run ruff check src/ tests/` zero errors
- [ ] `uv run ruff format --check src/ tests/` passes

---

## Verification

### Commands

```bash
uv run pytest -m unit -v
uv run ruff check src/ tests/
uv run ruff format --check src/ tests/

# Test graceful degradation (uninstall sqlglot temporarily)
uv run burnt estimate "SELECT 1"
```

### Expected output

- All tests pass
- CLI shows helpful message when sqlglot unavailable

---

## Handoff

### Result

[Executor fills this in when done.]

```
status: todo
```

### Blocked reason

[If blocked, explain exactly what is missing.]
