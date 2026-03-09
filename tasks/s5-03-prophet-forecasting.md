# Task: Prophet Forecasting

---

## Metadata

```yaml
id: s5-03-prophet-forecasting
status: todo
sprint: 5
priority: normal
agent: ~
blocked_by: [s5-01-feature-extraction]
created_by: planner
```

---

## Context

### Goal

Implement per-SKU time-series cost projection using Prophet, to push accuracy limits by forecasting likely future costs based on historical trends.

### Files to Read

```
src/burnt/forecast/prophet.py
DESIGN.md § "Sprint 5"
```

### Files to Create

```
tests/unit/test_prophet.py
```

### Files to Modify

```
src/burnt/forecast/prophet.py
```

---

## Specification

- Implement Prophet forecasting logic
- Ensure `<2×` error bounds or `15% MAPE` target

---

## Acceptance Criteria

- [ ] Prophet forecasting implemented
- [ ] Accuracy limits are met on dataset
- [ ] Tests passing

---

## Verification

```bash
uv run pytest -m unit -v
uv run ruff check src/ tests/
```

---

## Handoff

```yaml
status: todo
```
