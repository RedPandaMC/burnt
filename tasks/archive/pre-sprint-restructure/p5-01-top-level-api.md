# Task: Top-Level Programmatic API

---

## Metadata

```yaml
id: p5-01-top-level-api
status: todo
phase: 5
priority: critical
agent: ~
blocked_by: [p4a-03-table-registry-runtime-backend]
created_by: planner
```

---

## Context

### Goal

Make `burnt` a first-class programmatic Python library, optimized for Data Engineers. We need to support the three core workflows: Circuit Breaker (`estimate`), Cost-Aware Unit Testing (`estimate_file`), and the Context-Aware Advisor (`advise_current_session`).

### Files to read

```
# Required
src/burnt/__init__.py
src/burnt/estimators/pipeline.py
src/burnt/parsers/antipatterns.py
docs/programmatic-workflows.md
```

### Background

Currently `__init__.py` only exposes the version string. We need to expose the core functions of the package so developers can use them seamlessly without wrestling with internal class instantiation.

---

## Acceptance Criteria

- [ ] Expose `burnt.lint(sql: str)` -> returns list of AntiPattern objects
- [ ] Expose `burnt.lint_file(path: str)` -> reads file, runs lint
- [ ] Expose `burnt.estimate(sql: str, cluster=None, registry=None)` -> returns CostEstimate
- [ ] Expose `burnt.estimate_file(path: str, cluster=None, registry=None)` -> reads file, runs estimate
- [ ] Expose `burnt.advise_current_session()` -> stub for now, raises NotImplementedError
- [ ] Ensure `__all__` is properly defined in `__init__.py`
- [ ] Add unit tests verifying these top-level imports and functions work

---

## Verification

### Commands

```bash
uv run pytest -m unit -v
uv run ruff check src/ tests/
```

### Integration Check

- [ ] Create a small test script `test_api.py` that runs `import burnt; print(burnt.lint("SELECT * FROM a,b"))` and verify it works without throwing import errors.

---

## Handoff

### Result

```yaml
status: todo
```
