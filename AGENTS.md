# AGENTS.md - LLM Working Rules for dburnrate

> This file defines how all LLM agents should work in this repository.

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
