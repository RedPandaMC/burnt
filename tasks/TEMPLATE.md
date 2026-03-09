# Task: [Task Name]

---

## Metadata

```yaml
id: [phase]-[sequence]-[slug]
status: todo
phase: [phase number]
priority: [critical|high|medium|low]
agent: ~
blocked_by: []
created_by: planner
```

---

## Context

### Goal

[One paragraph describing what needs to be built and why.]

### Files to read

```
# Required
src/burnt/...
tests/...

# Reference
DESIGN.md
```

### Background

[Any relevant architectural decisions, API structures, or mathematical formulas. If this is an estimation task, YOU MUST include the formulas here along with justifications/sources for any constants.]

---

## Acceptance Criteria

- [ ] [Criterion 1]
- [ ] [Criterion 2]
- [ ] [Estimation tasks: Estimate output falls within Phase tolerance bounds (Phase 4: 10x, Phase 5: 3x, Phase 6: 2x)]

---

## Verification

### Commands

```bash
uv run pytest -m unit -v
uv run ruff check src/ tests/
# Add specific test run or CLI command here
```

### Integration Check

- [ ] Run `burnt estimate` (or relevant CLI command) and confirm the output reflects the new feature/fix. The new logic MUST be wired up and reachable from the user-facing CLI.

---

## Handoff

### Result

[Executor fills this in when done. Paste command outputs here.]

```yaml
status: todo
# ^ change to validation-pending / done / blocked when finished
```

### Blocked reason

[If blocked, explain exactly what is missing.]
