# Task: Fix all 29 ruff lint errors

---

## Metadata

```yaml
id: p1-02-fix-lint-errors
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

`uv run ruff check src/ tests/` currently reports 29 errors (21 auto-fixable). Fix all of them so the command exits 0. Do NOT run `ruff --fix` blindly on src/ — some fixes change semantics (B019, UP042). Fix those manually.

### Files to read

```
# Required — these contain the errors
src/dburnrate/core/exchange.py
src/dburnrate/parsers/antipatterns.py
src/dburnrate/parsers/pyspark.py
tests/conftest.py
tests/unit/core/test_config.py
tests/unit/core/test_exchange.py
tests/unit/core/test_models.py
tests/unit/core/test_pricing.py
tests/unit/estimators/test_static.py
tests/unit/estimators/test_whatif.py
tests/unit/parsers/test_antipatterns.py
tests/unit/parsers/test_notebooks.py
tests/unit/parsers/test_pyspark.py
tests/unit/parsers/test_sql.py
```

### Background

**29 errors total, 21 auto-fixable.** The 8 non-auto-fixable ones need manual fixes:

#### Manual fixes (src/ — semantic changes):

1. **`exchange.py:11` — B019** `@lru_cache` on an instance method causes memory leaks (the instance is held in the cache dict).
   - Fix: Move the cache to module level using `functools.cache` on a standalone function, then call it from the method. Or use `@staticmethod` + `@lru_cache`. Simplest: convert `get_rate` to a `@staticmethod` since it only uses `self._rate` (wait, that's `FixedRateProvider`). For `FrankfurterProvider`, make a module-level `@lru_cache` helper function and delegate.

2. **`antipatterns.py:5` — UP042** `class Severity(str, Enum)` → `class Severity(StrEnum)`.
   - Fix: `from enum import StrEnum` (Python 3.11+, fine since this requires 3.12) and change the base class. Remove `str` from inheritance.

3. **`pyspark.py:33` — F841** `operations = []` assigned but never used (visitor stores results in `visitor.operations`).
   - Fix: Delete line 33 (`operations = []`).

4. **`pyspark.py:51-54` — SIM102** Three nested `if` → combine with `and`.
   - Fix: `if method_name == "repartition" and node.args and isinstance(node.args[0], ast_module.Constant) and node.args[0].value == 1:`

5. **`pyspark.py:63-64` — SIM102** Nested `if` → combine.
   - Fix: `if method_name == "sql" and isinstance(node.func.value, ast_module.Name) and node.func.value.id == "spark":`

6. **`conftest.py:16-17` — SIM102** Already covered in task p1-01. If that task is done first, this is already fixed. If running in parallel, fix it here too: combine the nested `if` statements.

#### Auto-fixable (run `ruff --fix` on tests/ only after manual src/ fixes):

All `I001` (import ordering) and `F401` (unused imports) in test files are auto-fixable:

```bash
uv run ruff check tests/ --fix
```

This handles the import issues in: test_config.py, test_exchange.py, test_models.py, test_pricing.py, test_static.py, test_whatif.py, test_antipatterns.py, test_notebooks.py, test_pyspark.py, test_sql.py.

**IMPORTANT**: Task p1-01 and p1-02 both touch `tests/conftest.py`. If running in parallel, coordinate: the SIM102 fix in conftest is the same fix (use `item.path` and combine the nested `if`). Apply both fixes together in whichever task runs first; the other task just verifies it's already done.

---

## Acceptance Criteria

- [ ] `uv run ruff check src/ tests/` exits 0 with "All checks passed."
- [ ] `uv run pytest` still passes all 122 tests (no regression)
- [ ] `exchange.py` no longer uses `@lru_cache` on an instance method
- [ ] `antipatterns.py` `Severity` now inherits from `StrEnum`
- [ ] `pyspark.py` has no unused variables and no nested `if` that can be combined

---

## Verification

### Commands (run all, in order)

```bash
uv run ruff check src/ tests/
uv run ruff format --check src/ tests/
uv run pytest 2>&1 | tail -3
```

### Expected output

```
# First command
All checks passed.

# Second command
All checks passed.

# Third command
122 passed in ...
```

---

## Handoff

### Result

All 29 errors fixed. Manual fixes: B019 resolved by wrapping `lru_cache` in `__init__` (per-instance cache, no memory leak); UP042 `StrEnum`; F841 removed unused `operations`; SIM102 collapsed nested ifs; removed dead `pass` block. Auto-fixed: 21 I001/F401 errors in test files via `ruff --fix`.

```
All checks passed.
122 passed in 0.50s
```

### Blocked reason

[If blocked, explain here]
