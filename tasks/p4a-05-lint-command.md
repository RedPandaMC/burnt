# Task: Ship `burnt lint` as Standalone CLI Command

---

## Metadata

```yaml
id: p4a-05-lint-command
status: todo
phase: 4D
priority: medium
agent: ~
blocked_by: []
created_by: planner
```

---

## Context

### Goal

Anti-pattern detection works today with zero calibration — it's ready to ship. This task adds a `burnt lint` CLI command that recursively analyzes files and reports anti-patterns, independent of cost estimation accuracy. This is the one feature that can go out now while estimation bugs are fixed.

Note: the anti-pattern detector's string-matching bugs are fixed in `p4a-01`. This task should be done **after** `p4a-01` or in parallel if the executor doesn't need the AST fix (it can run but will have false positives until p4a-01 merges).

### Files to read (executor reads ONLY these)

```
# Required
src/burnt/cli/main.py
src/burnt/parsers/antipatterns.py
src/burnt/parsers/sql.py
src/burnt/parsers/notebooks.py
src/burnt/parsers/pyspark.py
tests/unit/parsers/test_antipatterns.py

# Reference
files/04-FEATURE-ROADMAP.md    # §F4 lint command design + output format
```

### Background

**Desired CLI output** (from `files/04-FEATURE-ROADMAP.md §F4`):

```
$ burnt lint ./queries/

⚠ daily_revenue.sql:12  ORDER BY without LIMIT forces global sort
✗ etl_pipeline.sql:45   collect() without limit() — will OOM on large tables
⚠ etl_pipeline.sql:67   Python UDF has 10-100x overhead vs Pandas UDF

3 issues found (1 error, 2 warnings)
```

**Severity levels:**
- `✗` ERROR — definite anti-pattern (collect without limit, cross join)
- `⚠` WARNING — likely anti-pattern (ORDER BY without LIMIT, Python UDF)
- `ℹ` INFO — informational (Pandas UDF is fine but noted)

**File discovery:**
- Single file: `burnt lint file.sql`
- Directory (recursive): `burnt lint ./queries/`
- Glob: `burnt lint "queries/*.sql"`

**Exit code:** 1 if any ERROR found (CI-compatible), 0 if only warnings/info.

**Output formats:**
- Default: human-readable (above)
- `--format json`: machine-readable for CI

**Supported file types:** `.sql`, `.py`, `.ipynb`, `.dbc`

---

## Acceptance Criteria

- [ ] `burnt lint <path>` CLI command exists
- [ ] Recursively discovers `.sql`, `.py`, `.ipynb`, `.dbc` files in a directory
- [ ] Reports anti-patterns with file path, line number, severity, message
- [ ] Exit code 1 when any ERROR-severity pattern found
- [ ] `--format json` outputs JSON array of findings
- [ ] `--no-recursive` flag to disable directory recursion
- [ ] Works offline (no Databricks connection required)
- [ ] `burnt lint --help` shows usage
- [ ] All public functions have type hints and docstrings
- [ ] `uv run pytest -m unit -v` passes
- [ ] `uv run ruff check src/ tests/` zero errors

---

## Verification

### Commands

```bash
uv run pytest -m unit -v
uv run ruff check src/ tests/
uv run ruff format --check src/ tests/
# Smoke tests
uv run burnt lint --help
echo "SELECT * FROM a CROSS JOIN b" > /tmp/test.sql
uv run burnt lint /tmp/test.sql && echo "should have exited 1" || echo "correctly exited 1"
uv run burnt lint /tmp/test.sql --format json
```

### Expected output

- `burnt lint /tmp/test.sql` exits with code 1 and reports the cross join
- `--format json` outputs valid JSON

---

## Handoff

### Result

[Executor fills this in when done.]

```
status: todo
```

### Blocked reason

Can run in parallel with `p4a-01`. If string-matching anti-pattern fix is not merged, tests may have false positives in comments/literals — acceptable for this task; note in handoff.
