# AGENTS.md - LLM Working Rules for dburnrate

> Auto-loaded by Claude Code. Governs all opencode sessions in this repo.
> Roadmap and design: DESIGN.md | Tasks: tasks/*.md

---

## Project At a Glance

**dburnrate** — Python package for pre-execution Databricks cost estimation.
- Stack: Python 3.12, uv, hatchling, pydantic v2, typer, rich, sqlglot
- Source: `src/dburnrate/` | Tests: `tests/unit/` | CLI: `uv run dburnrate`
- Status: Phases 1–3 complete (263 tests passing). Phase 4A (critical bug fixes) is next.

> **March 2026 Audit:** The static estimator formula is mathematically wrong (960× overestimate), the hybrid estimator uses a phantom price ($0.20/DBU matches no real SKU), and EXPLAIN DBU constants are ~7,900× too high. Fix these before wiring the CLI. See `files/00-EXECUTIVE-SUMMARY.md` and DESIGN.md §"Critical Bugs".

---

## Core Principles

1. **Follow the design document** - Major architectural decisions MUST be grounded in DESIGN.md (research, architecture, pricing)
2. **Be critical** - Question assumptions, validate formulas empirically when possible, don't accept unverified estimates
3. **Verify everything** - Run tests, linting, and type checking before marking any work complete
4. **Minimal changes** - Make focused, incremental changes. Avoid scope creep.

---

## Before Starting Any Task

### Required Pre-flight Checks
1. Read DESIGN.md (§"Implementation Roadmap") and `ls tasks/*.md` to understand current priorities
2. Check existing implementation in `src/dburnrate/` before adding new code
3. Look at existing tests in `tests/` for patterns and conventions

### Multi-Step Tasks
Use the TodoWrite tool to track progress. Break complex tasks into smaller, verifiable steps.

---

## Three-Role Development

This repo uses **planner/executor/validator** pattern for AI-assisted development:

| Role | Model | Responsibility |
|------|-------|---------------|
| **Planner** | Anthropic (Claude Opus/Sonnet) | Reads DESIGN.md roadmap, decomposes tasks, writes task specs to `tasks/` |
| **Executor** | Kimi / Minimax / Sonnet | Picks up task files from `tasks/`, implements code, runs tests, writes handoff |
| **Validator** | Planner after executor marks done | Runs benchmark queries, checks estimate accuracy, verifies CLI integration |

### If you are a PLANNER agent:
1. Read DESIGN.md §"Implementation Roadmap" — work phases in order
2. Check `tasks/` for existing in-progress/blocked tasks before creating new ones
3. Create self-contained task files using `tasks/TEMPLATE.md`
4. Tasks must include: file list to read, exact acceptance criteria, test commands
5. Mark tasks `status: todo` — executor picks them up
6. After executor marks `status: done`, verify the handoff notes, update DESIGN.md phase status

### If you are an EXECUTOR agent:
1. List `tasks/` and pick ONE `status: todo` task
2. Update status to `status: in-progress` and add `agent: <your-model-id>`
3. Read ONLY the files listed in the task's `context.files` section
4. Implement, then run the task's `verification.commands` — ALL must pass
5. **Integration check:** run `dburnrate estimate "SELECT 1"` and verify the new signal appears in output (not just unit tests passing)
6. **Benchmark check** (estimation tasks only): run against `tests/benchmarks/` and confirm estimates are within the phase tolerance (Phase 4: 10×, Phase 5: 3×, Phase 6: 2×)
7. Write results to `handoff.result` in the task file
8. Mark `status: done` or `status: blocked` (with reason)
9. Never start a second task until the first is done or blocked

### Parallel execution rules:
- Tasks with non-overlapping file sets can run in parallel
- Tasks with `blocked_by: [task-id]` must wait
- Never modify a file another in-progress task owns (check `context.files`)

### Task file lifecycle (MANDATORY)
- Active tasks: `tasks/<id>.md` (status: todo | in-progress | blocked)
- Completed tasks: rename to `tasks/<id>.md.completed` — **never delete, always rename**
- When marking a task done, the planner renames the file: `mv tasks/<id>.md tasks/<id>.md.completed`
- `ls tasks/*.md` should show only actionable tasks; `ls tasks/*.md.completed` shows history

### Task Files Must Specify (Estimation Tasks)
Every task that touches estimation logic **must** include:
1. The formula to implement (with units)
2. How constants were derived (citation or calculation — no fabricated values)
3. Expected output for ≥3 test inputs with known correct answers
4. Acceptable error bounds (e.g., "within 10× of actual")

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

### Following the Design Document

When implementing features, reference **DESIGN.md**:

- **§"Research Findings"** - EXPLAIN COST, Delta metadata, query fingerprinting, ML accuracy benchmarks
- **§"Architecture & Concepts"** - Complexity weight tables, pricing tables, competitive analysis
- **§"Implementation Roadmap"** - Phase ordering, task file references

Example: When implementing cost estimation, reference the weight table in DESIGN.md §"Complexity scoring model" and the hybrid architecture in §"Hybrid architecture for pre-execution estimation".

---

## Specific Guidelines

### For New Features

1. Check DESIGN.md §"Implementation Roadmap" and `tasks/*.md` for existing plans
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

## Commit Attribution

**All commits must be attributed to the human developer only.**

- Do NOT add `Co-Authored-By: Claude` or any AI model to commit messages
- Do NOT add AI tools as authors or contributors in `pyproject.toml`
- The git author must be a human (`git config user.name` / `user.email`)
- AI-assisted code is normal; AI co-authorship in git history is not appropriate for this repo

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
│   ├── pricing.py        # DBU rates (Azure; AWS/GCP in p4-04)
│   ├── exchange.py       # Currency conversion
│   ├── exceptions.py     # Custom exceptions
│   └── protocols.py      # Protocol classes
├── parsers/              # Code analysis
│   ├── sql.py            # SQL parsing (sqlglot)
│   ├── pyspark.py        # PySpark analysis
│   ├── notebooks.py      # .ipynb/.dbc parsing
│   ├── antipatterns.py   # Anti-pattern detection
│   ├── explain.py        # EXPLAIN COST parser (Phase 3)
│   └── delta.py          # Delta _delta_log reader (Phase 3)
├── estimators/           # Cost estimation
│   ├── static.py         # Complexity-based estimation
│   ├── hybrid.py         # EXPLAIN + history blend (Phase 3)
│   └── whatif.py         # Scenario modeling
├── tables/               # Databricks system tables (Phase 2)
│   ├── connection.py     # REST API client
│   ├── billing.py        # system.billing.*
│   ├── queries.py        # system.query.history + fingerprinting
│   └── compute.py        # system.compute.*
├── forecast/             # Prophet forecasting (post-MVP)
└── py.typed              # PEP 561 marker
```

---

## Important Notes

### Current Status (Phase 4A is next)

- Phases 1–3 complete: 263 unit tests pass, 0 lint errors
- System tables client, billing, query history, compute: implemented (Phase 2)
- EXPLAIN COST parser, Delta log reader, hybrid estimator: implemented (Phase 3)
- **Critical bugs identified (March 2026 audit):** quadratic formula in static estimator, phantom $0.20/DBU price, EXPLAIN constants 7,900× too high, SQL injection in table queries, string-matching anti-pattern detector, shadowed classes in protocols.py
- Phase 4 tasks in `tasks/p4-*.md`: wire CLI, Delta scan sizes, fingerprint lookup, AWS/GCP pricing
- **Missing:** `tables/attribution.py`, `EstimationPipeline` orchestrator, `TableRegistry`, `RuntimeBackend`

### Priority Order

1. **Phase 4A** (next): Fix 7 critical bugs before any CLI wiring — see DESIGN.md §"Phase 4A: Critical Bug Fixes"
2. **Phase 4B** (active): Wire `EstimationPipeline` into CLI, implement attribution, AWS/GCP pricing
3. **Phase 4C**: Enterprise support: `TableRegistry` + `RuntimeBackend`
4. **Phase 4D**: Ship `dburnrate lint` as standalone (ready today, 2 days effort)
5. **Phase 5**: Production hardening (error handling, caching, observability, multi-cloud VM pricing)
6. **Phase 6**: ML cost models (requires calibration data from 4B)
7. **v0.2+**: Notebook aggregation, batch analysis, cost regression, CI/CD gates

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
