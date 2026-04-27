```yaml
id: P5-01-e2e-tests
status: todo
phase: 5
priority: high
agent: ~
blocked_by: [P3-06-cli-implementation, PX-02-sparkmeasure-session]
created_by: planner
```

## Context

### Goal

Create E2E test fixtures and tests that exercise the full `burnt.check()` pipeline from source file → Rust engine → Python enrichment → `CheckResult`, verifying correct rule IDs, line numbers, and exit codes. Tests must run without Spark or Databricks credentials.

### Files to read

```
# Required
src/burnt/_check/__init__.py
src/burnt/cli/main.py
tests/unit/                        (existing test patterns)

# Reference
DESIGN.md §3 (Environments)
tasks/P3/06-cli-implementation.md
```

---

## Acceptance Criteria

### Fixtures (create in `tests/fixtures/e2e/`)

- [ ] `cross_join.py` — contains `df1.crossJoin(df2)` at a known line (e.g. line 5). Expects: BP014 finding, `line_number == 5`, `severity == "warning"`
- [ ] `collect_no_limit.py` — contains `df.collect()` without `.limit()`. Expects: BP008 finding, `severity == "error"`
- [ ] `select_star.sql` — contains `SELECT * FROM orders`. Expects: SQ001 finding, `severity == "warning"`
- [ ] `empty.py` — empty file. Expects: 0 findings, exit 0
- [ ] `syntax_error.py` — syntactically invalid Python (e.g. `def foo(:`). Expects: graceful error message, no Python traceback, exit 2

### Tests (in `tests/unit/test_e2e.py` or `tests/integration/test_e2e.py`)

- [ ] `test_cross_join_detected()` — `burnt.check("cross_join.py")` returns `CheckResult` with BP014 at correct line
- [ ] `test_collect_no_limit_detected()` — BP008 found, severity=error
- [ ] `test_select_star_sql()` — SQ001 found in SQL file
- [ ] `test_empty_file_clean()` — 0 findings, `compute_seconds` is None or 0
- [ ] `test_syntax_error_graceful()` — no uncaught exception, result has 0 findings or a parse error finding
- [ ] `test_cli_exit_code_success()` — subprocess `burnt check empty.py` exits 0
- [ ] `test_cli_exit_code_failure()` — subprocess `burnt check cross_join.py --fail-on warning` exits 1
- [ ] `test_cli_json_output()` — `--output json` produces parseable JSON with expected keys

### CI matrix

- [ ] All tests pass on Python 3.10, 3.11, 3.12 (already in `.azure/pipelines.yml`)

## Verification

```bash
uv run pytest tests/unit/test_e2e.py -v
uv run pytest tests/unit/test_e2e.py -v --tb=short -m "not integration"
```

### Integration Check

- [ ] `burnt check tests/fixtures/e2e/cross_join.py` in a shell (not pytest) shows BP014 in the table output
