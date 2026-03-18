# AGENTS.md - burnt Development Rules

**burnt** — Python pre-execution Databricks cost estimation. Stack: Python 3.12, uv, pydantic v2, typer, rich, sqlglot, databricks-sdk.

---

## Task Workflow

### PLANNER
1. Read DESIGN.md (§"Implementation Roadmap")
2. Check `tasks/` for existing work
3. Create task files with: file list, acceptance criteria, verification commands
4. Mark `status: todo`

### EXECUTOR
1. Pick `status: todo` task
2. Update: `status: in-progress`, `agent: <model-id>`
3. Implement
4. Run verification: `uv run pytest -m unit -v && uv run ruff check src/ tests/`
5. Update task with:
   - `status: done`, `completed_by: <model-id>`
   - Implementation section (files changed, key decisions, results)
   - Check off acceptance criteria
6. Update `tasks/README.md`: mark the task row `done` in the sprint table
7. Rename: `mv tasks/<id>.md tasks/<id>.md.completed`

---

## Rules

- **Verify everything** — tests + lint + format before marking complete
- **Minimal changes** — focused, incremental
- **No AI commits** — human only in git author
- **Commits**: `feat:`, `fix:`, `refactor:`, `test:`, `chore:`

---

## Task File Format (Required)

```yaml
status: done|todo|in-progress
agent: <model-id>
completed_by: <model-id>

## Implementation
### Changes Made
- src/file.py - what changed

### Implementation Notes
- Key decisions

### Verification Results
- Tests: N passed
- Lint: pass
```

---

## Common Commands

```bash
uv sync --extra sql    # Install deps
uv run pytest -m unit  # Run tests
uv run ruff check src/ # Lint
uv run ruff format src/ # Format
```
