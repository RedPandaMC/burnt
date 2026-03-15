# Task: Kill Dollar Amounts in Offline Mode

---

## Metadata

```yaml
id: s2-10-offline-mode-fix
status: todo
phase: 2
priority: high
agent: ~
blocked_by: []
created_by: planner
```

---

## Context

### Goal

When `burnt` runs without a backend (offline / static-only mode), the static estimator produces dollar amounts derived from a unitless complexity score — the formula has no real data volume input, so `scan_bytes` is a heuristic weight sum that is identical whether the table is 1 MB or 1 TB. Showing "$0.03" when the real answer might be "$30" actively destroys user trust. The fix is to suppress `estimated_cost_usd` and `estimated_cost_eur` when no backend is available, return only the complexity score and anti-pattern warnings, and direct users to `burnt doctor` for connected-mode estimates.

### Files to read

```
# Required
src/burnt/estimators/pipeline.py
src/burnt/estimators/static.py
src/burnt/core/models.py
src/burnt/cli/main.py

# Reference
DESIGN.md
tasks/s2-07-cost-guard.md
tasks/s2-08-doctor-command.md
```

### Background

The `EstimationPipeline` orchestrates 4 tiers. In offline mode only Tier 1 (static analysis) fires. The static estimator computes:

```
estimated_seconds = (scan_bytes / throughput_bps) + (shuffle_count × shuffle_overhead_s)
estimated_dbu     = (estimated_seconds / 3600) × cluster_dbu_per_hour
estimated_usd     = (estimated_dbu × dbu_rate) + (vm_rate × node_count × estimated_seconds / 3600)
```

`scan_bytes` in offline mode comes from `complexity_score × BASE_BYTES_PER_COMPLEXITY_POINT`, making it a made-up number. The fix: when `backend is None`, set `estimated_dbu = None` and `estimated_cost_usd = None` on the returned `CostEstimate`, keep `complexity_score` and `warnings` intact.

Implication for `raise_if_exceeds` (s2-07): when `estimated_cost_usd is None`, the guard should `warnings.warn()` but must not raise — this is already specced in s2-07, so no change needed there.

CLI output in offline mode should show:
```
Complexity: 34 (moderate)
Warnings:
  ⚠ cross_join — CROSS JOIN creates O(n×m) rows
  ⚠ order_by_no_limit — ORDER BY without LIMIT forces global sort

Connect to a workspace for cost estimates: burnt doctor
```

---

## Acceptance Criteria

- [ ] `EstimationPipeline.estimate()` returns `CostEstimate(estimated_dbu=None, estimated_cost_usd=None, estimated_cost_eur=None, confidence="none", ...)` when `backend is None`
- [ ] `complexity_score` and `warnings` are still populated in the offline result
- [ ] `CostEstimate.confidence` is `"none"` (not `"low"`) when no backend is available
- [ ] CLI `burnt check` (or `burnt estimate`) in offline mode prints complexity + warnings + doctor hint, not dollar amounts
- [ ] `raise_if_exceeds()` issues `warnings.warn()` but does not raise when `estimated_cost_usd is None`
- [ ] Unit tests cover the offline path explicitly: assert `estimated_cost_usd is None`
- [ ] All existing tests still pass
- [ ] Lint passes: `uv run ruff check src/ tests/`

---

## Verification

### Commands

```bash
uv run pytest -m unit -v
uv run ruff check src/ tests/
# Offline smoke test (no DATABRICKS_HOST set):
uv run burnt check "SELECT * FROM a CROSS JOIN b"
```

### Integration Check

- [ ] With no env vars set, run a `burnt check` or `burnt estimate` command and confirm no dollar amounts appear in output. Complexity score and anti-pattern warnings must still appear. A "connect to workspace" hint must appear.

---

## Handoff

### Result

[Executor fills this in when done.]

```yaml
status: todo
```

### Blocked reason

[If blocked, explain exactly what is missing.]
