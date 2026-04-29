```yaml
id: P2-01a-cli-surface-cleanup
status: done
phase: 2
priority: critical
agent: ~
blocked_by: []
created_by: planner
```

## Context

### Goal

Remove pre-pivot CLI commands and fix a misleading error stub. Scope is narrow: CLI
surface only. Pre-pivot subtree deletion (watch/, alerts/, intelligence/) is P2-01b.
Public API cleanup is P2-07.

**Commands to remove:** `burnt advise`, `burnt tutorial`
**Functions to remove:** `_run_tutorial()`, `_TUTORIAL_NOTEBOOKS`, `_NOTEBOOK_TEMPLATE`
**Broken stub to fix:** `graph/estimate.py` misleading `NotImplementedError` message

### Files to modify

```
src/burnt/cli/main.py
src/burnt/graph/estimate.py
```

### Background

- `burnt advise` calls `burnt.advise(job_id=...)` — this function does not exist.
  It raises `AttributeError` at runtime.
- `burnt tutorial` generates notebooks using removed `burnt.estimate()` / `burnt.simulate()`.
- `graph/estimate.py` raises `NotImplementedError("Install with: pip install burnt[engine]")`
  — there is no `burnt[engine]` extra.

---

## Acceptance Criteria

- [ ] `burnt --help` lists only: `check`, `rules`, `init`, `doctor`, `cache`
- [ ] `burnt advise` → typer "No such command" error
- [ ] `burnt tutorial` → typer "No such command" error
- [ ] `_run_tutorial()`, `_TUTORIAL_NOTEBOOKS`, `_NOTEBOOK_TEMPLATE` removed from `cli/main.py`
- [ ] `graph/estimate.py` `estimate_cost()` returns `CostEstimate(confidence="low")` with a
  `# TODO: implement in P4-10` comment instead of raising with wrong extra name
- [ ] `uv run ruff check src/` passes

## Verification

```bash
burnt --help
burnt advise 2>&1 | grep -i "no such"
uv run ruff check src/ tests/
uv run pytest tests/unit -x -q
```
