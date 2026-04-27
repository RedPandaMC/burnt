```yaml
id: PX-03-cli-rewire
status: todo
phase: X
priority: high
agent: ~
blocked_by: [PX-01-remove-dead-code]
created_by: planner
```

## Context

### Goal

Wire the `burnt check` CLI command to `_check.run()` instead of the old `detect_antipatterns()` path. Currently the CLI bypasses the Rust engine entirely.

### Files to modify

```
# Required
src/burnt/cli/main.py          (check() command — replace detect_antipatterns with _check.run)
src/burnt/_check/__init__.py   (move select/ignore filtering into run())
src/burnt/display/terminal.py  (verify it accepts CheckResult)

# Reference
DESIGN.md §12 (CLI)
tasks/P3/06-cli-implementation.md
```

### Background

The current `cli/main.py check()` function:
1. Calls `detect_antipatterns(source, lang)` from `parsers/antipatterns.py`
2. Applies rule filtering manually in the CLI layer
3. Formats output itself

The correct flow:
1. Call `_check.run(path=path, skip=ignore_list, only=select_list, severity=fail_on, session=_SESSION)`
2. Pass the returned `CheckResult` to `display/terminal.py:to_table()` or `display/export.py` based on `--output`
3. Exit with correct code based on findings

The `--select`/`--ignore`/`--extend-select`/`--extend-ignore` flag resolution logic in the CLI can stay but should feed into `_check.run(skip=..., only=...)`.

---

## Acceptance Criteria

- [ ] `cli/main.py check()` does not import `detect_antipatterns` — remove that import
- [ ] `cli/main.py check()` calls `_check.run(path=path, skip=..., only=..., severity=..., session=_SESSION)`
- [ ] The returned `CheckResult` is rendered via `display/terminal.py:to_table()` for `--output table`
- [ ] The returned `CheckResult` is serialised via `result.to_json()` for `--output json`
- [ ] Exit code logic: 0 = clean, 1 = findings above threshold, 2 = config/parse error
- [ ] `_check.run()` accepts `skip: list[str] | None` and `only: list[str] | None` and applies them (move filtering from CLI into `run()`)
- [ ] `uv run ruff check src/burnt/cli/main.py` passes
- [ ] All existing CLI unit tests pass (update any that test the old path)

## Verification

```bash
# Wire works
burnt check tests/fixtures/e2e/cross_join.py --output table   # shows BP014
burnt check tests/fixtures/e2e/cross_join.py --output json | python -m json.tool
burnt check tests/fixtures/e2e/empty.py                        # exit 0

# Filtering works
burnt check tests/fixtures/e2e/cross_join.py --ignore BP014    # 0 findings
burnt check tests/fixtures/e2e/cross_join.py --select BP014    # only BP014

uv run pytest tests/unit/cli/ -v
```

### Integration Check

- [ ] Findings from `burnt check` now include `line_number` populated (from Rust engine), which was absent in the old `detect_antipatterns` path
