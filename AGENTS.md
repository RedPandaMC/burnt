# AGENTS.md - LLM Working Rules for dburnrate

> Auto-loaded by Claude Code. Governs all opencode sessions in this repo.
> Roadmap: FUTURE_TODOS.md

---

## Project At a Glance

**dburnrate** — Python package for pre-execution Databricks cost estimation.
- Stack: Python 3.12, uv, hatchling, pydantic v2, typer, rich, sqlglot
- Source: `src/dburnrate/` | Tests: `tests/unit/` | CLI: `uv run dburnrate`
- Status: MVP built (phases 0-4 of PLAN.md)

---

## Core Principles

1. **Follow the research documents** - Major architectural decisions MUST be grounded in RESEARCH.md, CONCEPT.md, and PLAN.md
2. **Be critical** - Question assumptions, validate formulas empirically when possible, don't accept unverified estimates
3. **Verify everything** - Run tests, linting, and type checking before marking any work complete
4. **Minimal changes** - Make focused, incremental changes. Avoid scope creep.

---

## Before Starting Any Task

### Required Pre-flight Checks
1. Read FUTURE_TODOS.md to understand the current roadmap and priorities
2. Check existing implementation in `src/dburnrate/` before adding new code
3. Look at existing tests in `tests/` for patterns and conventions

### Multi-Step Tasks
Use the TodoWrite tool to track progress. Break complex tasks into smaller, verifiable steps.

---

## Two-Role Parallel Coding

This repo uses **planner/executor** pattern for AI-assisted development:

| Role | Model | Responsibility |
|------|-------|---------------|
| **Planner** | Anthropic (Claude Opus/Sonnet) | Reads FUTURE_TODOS.md, decomposes tasks, writes task specs to `tasks/` |
| **Executor** | Kimi / Minimax / Sonnet | Picks up task files from `tasks/`, implements code, runs tests, writes handoff |

### If you are a PLANNER agent:
1. Read FUTURE_TODOS.md — work Phase 1 first, then Phase 2, etc.
2. Check `tasks/` for existing in-progress/blocked tasks before creating new ones
3. Create self-contained task files using `tasks/TEMPLATE.md`
4. Tasks must include: file list to read, exact acceptance criteria, test commands
5. Mark tasks `status: todo` — executor picks them up
6. After executor marks `status: done`, verify the handoff notes, update FUTURE_TODOS.md

### If you are an EXECUTOR agent:
1. List `tasks/` and pick ONE `status: todo` task
2. Update status to `status: in-progress` and add `agent: <your-model-id>`
3. Read ONLY the files listed in the task's `context.files` section
4. Implement, then run the task's `verification.commands` — ALL must pass
5. Write results to `handoff.result` in the task file
6. Mark `status: done` or `status: blocked` (with reason)
7. Never start a second task until the first is done or blocked

### Parallel execution rules:
- Tasks with non-overlapping file sets can run in parallel
- Tasks with `blocked_by: [task-id]` must wait
- Never modify a file another in-progress task owns (check `context.files`)

### Task file lifecycle (MANDATORY)
- Active tasks: `tasks/<id>.md` (status: todo | in-progress | blocked)
- Completed tasks: rename to `tasks/<id>.md.completed` — **never delete, always rename**
- When marking a task done, the planner renames the file: `mv tasks/<id>.md tasks/<id>.md.completed`
- `ls tasks/*.md` should show only actionable tasks; `ls tasks/*.md.completed` shows history

---

## While Working

### Code Quality Standards

1. **Type hints** - All public functions MUST have return type hints
2. **Docstrings** - All public functions MUST have docstrings (unless trivial)
3. **Error handling** - Always handle exceptions gracefully with meaningful messages
4. **Input validation** - Validate all inputs, especially from CLI/API

### Testing Requirements

Before marking any feature complete, run:

```bash
# Unit tests
uv run pytest -m unit -v

# Lint
uv run ruff check src/ tests/
uv run ruff format --check src/ tests/

# Type check (if mypy configured)
uv run mypy src/

# Security
uv run bandit -c pyproject.toml -r src/
```

### Following Research Documents

When implementing features, reference the research:

- **RESEARCH.md** - Technical feasibility, EXPLAIN COST, Delta metadata, query fingerprinting
- **CONCEPT.md** - Design rationale, weight tables, competitive analysis
- **PLAN.md** - Implementation phases and ordering

Example: When implementing cost estimation, reference the weight table in CONCEPT.md and the hybrid architecture in RESEARCH.md.

---

## Specific Guidelines

### For New Features

1. Check FUTURE_TODOS.md for existing plans
2. Create a todo list for the implementation
3. Implement incrementally with testable checkpoints
4. Add unit tests for new functionality
5. Verify with lint/tests before committing

### For Bug Fixes

1. Write a test that reproduces the bug first
2. Fix the bug
3. Verify the test passes
4. Run full test suite to ensure no regressions

### For Refactoring

1. Ensure existing tests pass before starting
2. Make minimal changes
3. Run tests after each logical change
4. Commit in logical chunks

---

## Commit Messages

Follow conventional commits:

- `feat:` - New feature
- `fix:` - Bug fix
- `refactor:` - Code refactoring
- `docs:` - Documentation
- `test:` - Tests
- `chore:` - Maintenance

Example: `feat: Add EXPLAIN COST parsing for cold-start estimation`

---

## Project Structure Reference

```
src/dburnrate/
├── __init__.py           # Version info
├── _compat.py            # Optional import helpers
├── cli/                  # Typer CLI
│   └── main.py
├── core/                 # Models, config, pricing
│   ├── models.py         # Pydantic models
│   ├── config.py         # Settings
│   ├── pricing.py        # DBU rates
│   ├── exchange.py       # Currency conversion
│   ├── exceptions.py    # Custom exceptions
│   └── protocols.py      # Protocol classes
├── parsers/              # Code analysis
│   ├── sql.py           # SQL parsing (sqlglot)
│   ├── pyspark.py       # PySpark analysis
│   ├── notebooks.py     # .ipynb/.dbc parsing
│   └── antipatterns.py  # Anti-pattern detection
├── estimators/           # Cost estimation
│   ├── static.py        # Complexity-based estimation
│   └── whatif.py        # Scenario modeling
├── tables/              # System tables (TODO)
├── forecast/            # Prophet forecasting
└── py.typed             # PEP 561 marker
```

---

## Important Notes

### Current MVP Gaps (Don't Accept as Final)

1. Estimator formula is unvalidated - needs empirical calibration
2. No system tables integration yet
3. No EXPLAIN COST parsing
4. No Delta metadata integration
5. No historical fingerprinting

### Priority Order

1. **Phase 1**: Validate tests/lint pass
2. **Phase 2**: System tables integration (highest ROI)
3. **Phase 3**: EXPLAIN COST parsing
4. **Phase 4-5**: Delta metadata + fingerprinting
5. **Phase 6+**: ML models, multi-cloud, advanced features

---

## Quick Reference Commands

```bash
# Setup
uv sync
uv sync --extra sql  # For sqlglot support

# Testing
uv run pytest -m unit -v
uv run pytest --cov --cov-report=term-missing

# Linting
uv run ruff check src/ tests/
uv run ruff format src/ tests/

# CLI
uv run dburnrate --help
uv run dburnrate estimate "SELECT * FROM table"
```
