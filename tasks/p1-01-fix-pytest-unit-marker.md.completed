# Task: Fix pytest -m unit selecting 0 tests

---

## Metadata

```yaml
id: p1-01-fix-pytest-unit-marker
status: done
phase: 1
priority: high
agent: claude-sonnet-4-6
blocked_by: []
created_by: planner
```

---

## Context

### Goal

`uv run pytest -m unit -v` collects 0 tests from 122. All 122 tests pass when run without the marker filter. Fix the auto-marker logic in `tests/conftest.py` so the `unit` marker is properly applied to all files under `tests/unit/` and `pytest -m unit` selects all of them.

### Files to read

```
# Required
tests/conftest.py
tests/unit/conftest.py

# Reference — check pytest version compatibility
pyproject.toml   (see [tool.pytest.ini_options] — filterwarnings = ["error"] is key)
```

### Background

The root conftest at `tests/conftest.py:16-17` uses `item.fspath` to detect the test path. In **pytest 9.x** (this project uses pytest-9.0.2), `item.fspath` is deprecated and may emit a `PytestWarning`. Because `pyproject.toml` sets `filterwarnings = ["error"]`, that warning becomes an error inside the hook, causing the `add_marker` call to be silently skipped.

Fix: replace `item.fspath` with `item.path` (a `pathlib.Path`) and update the string check accordingly. The combined SIM102 lint error (nested `if` → single `if` with `and`) should also be fixed at the same time.

Current broken code in `tests/conftest.py`:

```python
def pytest_collection_modifyitems(config, items):
    for item in items:
        if "unit" not in item.keywords and "integration" not in item.keywords:
            if "tests/unit" in str(item.fspath):   # <-- fspath deprecated in pytest 9
                item.add_marker(pytest.mark.unit)
```

---

## Acceptance Criteria

- [ ] `uv run pytest -m unit -v` selects and passes all 122 tests
- [ ] `uv run pytest -m unit -v` output ends with `122 passed, 0 failed`
- [ ] `uv run ruff check tests/conftest.py` produces zero errors for this file
- [ ] `uv run pytest` (no marker) still passes all 122 tests

---

## Verification

### Commands (run all, in order)

```bash
uv run pytest -m unit -v 2>&1 | tail -5
uv run pytest 2>&1 | tail -3
uv run ruff check tests/conftest.py
```

### Expected output

```
# First command — must show 122 selected, not 0 deselected
122 passed in ...

# Second command
122 passed in ...

# Third command
All checks passed.
```

---

## Handoff

### Result

Root cause: `"unit" not in item.keywords` was always False because the directory name `unit` appears as a keyword in pytest 9.x path-based keywords. Fixed by using `item.get_closest_marker("unit") is None` instead, which checks for actual registered markers. Also replaced deprecated `item.fspath` with `item.path`.

```
122 passed in 0.50s
```

### Blocked reason

[If blocked, explain here]
