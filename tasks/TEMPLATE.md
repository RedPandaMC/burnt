# Task: [Short title — imperative verb phrase]

---

## Metadata

```yaml
id: p{phase}-{seq:02d}-{slug}
status: todo          # todo | in-progress | done | blocked | cancelled
phase: 1              # which FUTURE_TODOS.md phase this belongs to
priority: high        # high | medium | low
agent: ~              # filled by executor: model-id that claimed this task
blocked_by: []        # list of task IDs that must be done first
created_by: planner
```

---

## Context

### Goal

[One paragraph. What must be done and why. Written for an executor who has not read FUTURE_TODOS.md.]

### Files to read (executor reads ONLY these)

```
# Required — read before starting
src/dburnrate/<path>
tests/<path>

# Reference — read if needed
PLAN.md              # for implementation patterns
```

### Background

[Any specific knowledge the executor needs. Reference exact sections of RESEARCH.md or CONCEPT.md if relevant. Keep this tight — do not copy-paste whole documents.]

---

## Acceptance Criteria

Each line is a binary yes/no check:

- [ ] ...
- [ ] All public functions have type hints and docstrings
- [ ] `uv run pytest -m unit -v` passes with no failures
- [ ] `uv run ruff check src/ tests/` produces zero errors

---

## Verification

### Commands (run all, in order)

```bash
uv run pytest -m unit -v
uv run ruff check src/ tests/
uv run ruff format --check src/ tests/
uv run bandit -c pyproject.toml -r src/
```

### Expected output

[Describe what passing looks like, e.g. "X passed, 0 failed" or specific test names that should now pass.]

---

## Handoff

### Result

[Executor fills this in when done. Paste key test output, summarize what changed.]

```
status: todo
# ^ change to done/blocked when finished
```

### Blocked reason

[If blocked, explain exactly what is missing and what the planner needs to do to unblock.]
