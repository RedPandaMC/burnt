```yaml
id: P2-07-public-api-cleanup
status: done
phase: 2
priority: critical
agent: ~
blocked_by: []
created_by: planner
```

## Context

### Goal

Strip pre-pivot surface from the public Python API. This task is a prerequisite for
P2-01b (subtree deletion) because it removes the `watch()` function and config params
before the modules they depend on are deleted.

### Files to modify

```
src/burnt/__init__.py
src/burnt/_config/__init__.py
```

### Changes

**`src/burnt/__init__.py`:**
- Remove `watch()` function and its import of `burnt._watch` / `burnt.watch`
- Remove `watch` from `__all__`
- Drop config() parameters: `alert_slack`, `alert_teams`, `alert_webhook`,
  `drift_threshold`, `idle_threshold`, `calibration_store`, `tag_key`, `budget`
- Update module docstring to post-pivot identity (static analyzer + REST enrichment)
- Final `__all__` must be exactly:
  `BurntError`, `ConfigError`, `CostBudgetExceeded`, `CostEstimate`, `EstimationError`,
  `NotAvailableError`, `ParseError`, `PricingError`, `check`, `config`, `start_session`, `version`

**`src/burnt/_config/__init__.py`:**
- Remove the pre-pivot params from the `set()` function signature

---

## Acceptance Criteria

- [ ] `python -c "import burnt; print(burnt.__all__)"` outputs exactly the 12 names above
- [ ] `python -c "import burnt; burnt.watch"` raises `AttributeError`
- [ ] `burnt.config()` accepts no `alert_slack`, `alert_teams`, `alert_webhook`,
  `drift_threshold`, `idle_threshold`, `calibration_store`, `tag_key`, or `budget` param
- [ ] Module docstring does not mention sparkMeasure, advise, simulate, watch, or fleet

## Verification

```bash
python -c "import burnt; print(burnt.__all__)"
python -c "import burnt; burnt.watch" 2>&1 | grep AttributeError
uv run ruff check src/burnt/__init__.py src/burnt/_config/
```
