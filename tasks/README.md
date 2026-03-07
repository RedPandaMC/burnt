# tasks/ — Agentic Task Queue

This directory is the **handoff protocol** between Planner agents (Anthropic/Claude) and Executor agents (Kimi/Minimax/Sonnet).

---

## How it works

```
Planner                          Executor
  |                                 |
  |-- creates task file ----------> |
  |   status: todo                  |
  |                                 |-- claims task
  |                                 |   status: in-progress
  |                                 |   agent: kimi-128k
  |                                 |
  |                                 |-- implements code
  |                                 |-- runs verification
  |                                 |-- writes handoff notes
  |                                 |   status: done
  |                                 |
  |<-- reads handoff notes ---------|
  |-- updates FUTURE_TODOS.md       |
```

---

## Task File Format

Every task is a single Markdown file. Use `TEMPLATE.md` to create new tasks.

### Status values

| Status | Meaning |
|--------|---------|
| `todo` | Ready to be picked up by an executor |
| `in-progress` | Claimed by an executor (check `agent` field) |
| `done` | Completed — handoff notes written |
| `blocked` | Cannot proceed — reason in `handoff.blocked_reason` |
| `cancelled` | No longer needed |

### Naming convention

```
{phase}-{sequence}-{short-slug}.md

Examples:
  p1-01-fix-test-failures.md
  p1-02-fix-lint-errors.md
  p2-01-billing-table-stub.md
```

---

## Parallel execution rules

1. **Check `blocked_by`** before starting — wait for dependencies
2. **Check `context.files`** — never modify a file owned by another `in-progress` task
3. **One task at a time per agent** — finish or block before picking up another
4. **Atomic file ownership** — if two tasks touch the same file, the second must list `blocked_by` the first

---

## Planner checklist (before creating a task)

- [ ] Task is atomic — one logical unit of work
- [ ] `context.files` lists every file the executor must read (no extras)
- [ ] `context.goal` is unambiguous — an executor with no other context can understand it
- [ ] `acceptance_criteria` are testable — yes/no, not subjective
- [ ] `verification.commands` are exact shell commands that will pass when done
- [ ] `blocked_by` lists any task IDs that must complete first

---

## Executor checklist (before marking done)

- [ ] All `verification.commands` ran and passed (paste output in `handoff.result`)
- [ ] New/modified code has type hints and docstrings on public functions
- [ ] No new lint errors: `uv run ruff check src/ tests/`
- [ ] `handoff.result` summarizes what was changed and why
- [ ] If blocked, `handoff.blocked_reason` explains exactly what is needed to unblock
