```yaml
id: P3-06-cli-implementation
status: todo
phase: 3
priority: high
agent: ~
blocked_by: [PX-01-remove-dead-code, PX-03-cli-rewire, PX-04-sarif-output]
created_by: planner
```

## Context

### Goal

Complete the `burnt check` CLI so it uses the new `_check.run()` pipeline end-to-end, and add the missing flags (`--output sarif`, `--event-log`). Remove `burnt advise` and `burnt tutorial`. Ensure exit codes are correct for CI use.

**Current state (broken):** `cli/main.py check()` calls `detect_antipatterns()` from the old parsers path. This bypasses the Rust engine entirely. The CLI says "300 tests passed" but it is testing the wrong code path.

### Files to read

```
# Required
src/burnt/cli/main.py
src/burnt/_check/__init__.py
src/burnt/display/terminal.py
src/burnt/display/export.py

# Reference
DESIGN.md §12 (CLI), §13 (CI Integration)
tasks/PX/03-cli-rewire.md
tasks/PX/04-sarif-output.md
```

---

## Acceptance Criteria

- [ ] `burnt check <path>` calls `_check.run()` — findings come from `_engine.analyze_file()`, NOT `detect_antipatterns()`
- [ ] `burnt check <path> --output table` renders findings via `display/terminal.py` (default)
- [ ] `burnt check <path> --output json` outputs valid JSON with `rule_id`, `severity`, `line_number`, `message`, `suggestion` fields
- [ ] `burnt check <path> --output sarif` outputs valid SARIF 2.1.0 JSON (see PX-04)
- [ ] `burnt check <path> --event-log ./spark-events/app.log` parses the event log and enriches `CheckResult` before display
- [ ] `burnt check <path> --fail-on error` exits 1 if any error-severity finding is present; exits 0 otherwise
- [ ] `burnt check <path> --max-cost 25` exits 1 if `result.compute_seconds` exceeds threshold (or estimated cost if backend available)
- [ ] `burnt advise` command is removed — `burnt --help` does not list it
- [ ] `burnt tutorial` command is removed — `burnt --help` does not list it
- [ ] `burnt --help` lists: `check`, `rules`, `init`, `doctor`, `cache`
- [ ] Exit code 0 on success, 1 on findings above threshold, 2 on parse/config error

## Verification

```bash
uv run pytest tests/unit/cli/ -v
uv run ruff check src/burnt/cli/main.py

# Smoke tests
burnt check tests/fixtures/e2e/cross_join.py              # exit 0, shows BP014
burnt check tests/fixtures/e2e/cross_join.py --output json | python -m json.tool
burnt check tests/fixtures/e2e/cross_join.py --output sarif | python -m json.tool
burnt check tests/fixtures/e2e/empty.py                   # exit 0, "No issues found"
burnt --help | grep -E "^\s+(check|rules|init|doctor|cache)"
burnt advise 2>&1 | grep -i "no such command"
```

### Integration Check

- [ ] `burnt check src/ --output sarif > burnt.sarif` produces a file that GitHub Code Scanning accepts (validate with `sarif-tools` or schema check)
