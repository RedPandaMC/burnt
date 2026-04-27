# AGENTS.md - burnt Development Rules

**burnt** — Notebook quality and cost analysis for Databricks. CLI-first. Stack: Python 3.10+, uv, pydantic v2, typer, rich, maturin (Rust engine via PyO3).

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
- **No AI commits** — human only in git author; do NOT add `Co-Authored-By: Claude` or any Claude/Anthropic attribution to commit messages or pull requests
- **Commits**: `feat:`, `fix:`, `refactor:`, `test:`, `chore:`
- **Typing** — Use `X | Y` union syntax and built-in generics (`list[str]`, `dict[str, Any]`, `tuple[int, ...]`, `set[str]`) throughout. `from __future__ import annotations` is present in all files. Import `Any`, `Literal`, `Protocol`, `runtime_checkable`, `TYPE_CHECKING`, `TypeVar`, `Generic`, `overload`, `cast`, `ClassVar` from `typing` as needed. Do NOT use `Optional[X]`, `List[X]`, `Dict[str, Any]`, or `Union[X, Y]` — these are legacy forms.
- **No dead-code stubs** — if a function is not implemented, remove it rather than leaving a broken placeholder.

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
uv sync --all-extras           # Install all deps incl. optional extras
uv run maturin develop         # Build and install Rust engine (dev mode)
uv run pytest -m unit -v       # Run unit tests
uv run pytest -m "not slow"    # Skip slow/integration tests
uv run ruff check src/ tests/  # Lint
uv run ruff format src/ tests/ # Format
```
