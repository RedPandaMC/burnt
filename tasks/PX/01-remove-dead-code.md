```yaml
id: PX-01-remove-dead-code
status: todo
phase: X
priority: critical
agent: ~
blocked_by: []
created_by: planner
```

## Context

### Goal

Remove all pre-pivot API debris from the CLI and public API. No deprecation wrappers, no stubs — delete. The product is CLI-first; commands that don't work should not exist.

**Commands to remove:** `burnt advise`, `burnt tutorial`
**Functions to remove:** `_run_tutorial()`, `_TUTORIAL_NOTEBOOKS`, tutorial notebook generation
**Broken stubs to fix:** `graph/estimate.py`'s misleading `NotImplementedError` message

### Files to modify

```
# Required
src/burnt/cli/main.py
src/burnt/graph/estimate.py
README.md

# Check these too (may reference removed APIs)
src/burnt/__init__.py
docs/ (any .md files referencing advise/estimate/simulate)
```

### Background

- `burnt advise` in `cli/main.py` calls `burnt.advise(job_id=...)` — this function does not exist in `__init__.py`. It will raise `AttributeError` at runtime.
- `burnt tutorial` generates notebooks using `burnt.estimate()` and `burnt.simulate()` — these were removed during the April 2026 pivot. The notebooks are pre-pivot debris.
- `graph/estimate.py` raises `NotImplementedError("Install with: pip install burnt[engine]")` — there is no `burnt[engine]` extra. This message will confuse users.
- `README.md` documents `result.api_json()` and `result.calibrate()` — neither method exists on `CheckResult`.

---

## Acceptance Criteria

- [ ] `burnt --help` lists only: `check`, `rules`, `init`, `doctor`, `cache`
- [ ] `burnt advise --help` prints "No such command 'advise'" (or equivalent typer error)
- [ ] `burnt tutorial` is gone — no reference in `cli/main.py`
- [ ] `_run_tutorial()`, `_TUTORIAL_NOTEBOOKS`, and `_NOTEBOOK_TEMPLATE` constants removed from `cli/main.py`
- [ ] `graph/estimate.py` `estimate_cost()` no longer raises `NotImplementedError` with a wrong extra name — replace with a clear `# TODO: implement in P2-10` comment and return an empty `CostEstimate(confidence="low")`
- [ ] `README.md` Python API section contains no references to `api_json()`, `calibrate()`, `advise()`, `estimate()`, `simulate()`
- [ ] No other `*.py` file imports `burnt.advise` or `burnt.estimate`
- [ ] `uv run ruff check src/` passes

## Verification

```bash
burnt --help
burnt advise 2>&1 | grep -i "no such"
uv run ruff check src/ tests/
uv run pytest tests/unit -x -q
grep -r "api_json\|calibrate\|\.advise\|\.estimate\|\.simulate" src/ README.md || echo "Clean"
```

### Integration Check

- [ ] `import burnt` in a fresh Python shell completes without error
- [ ] `burnt check tests/fixtures/e2e/empty.py` exits 0
