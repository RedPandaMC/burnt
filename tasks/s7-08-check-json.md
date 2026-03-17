# Task: burnt check --output json — Machine-Readable CI Output

---

## Metadata

```yaml
id: s7-08-check-json
status: todo
phase: 7
priority: medium
agent: ~
blocked_by: [s7-04-rust-semantic-core]
created_by: planner
```

---

## Context

### Goal

Add `--output json` support to `burnt check` so CI pipelines can consume lint results
programmatically — e.g., to post inline annotations to GitHub PRs, feed into a
custom dashboard, or fail with a structured error payload for log ingestion.

### Files to read

```
# Required
src/burnt/cli/main.py
src/burnt/linter/rules.py       ← or wherever LintResult / LintViolation is defined
src/burnt/linter/engine.py      ← or equivalent — check what check currently returns

# Reference
tasks/s2-05a-cli-api-redesign.md
tasks/s2-04-ast-lint-rules.md
```

### Background

`burnt check` (renamed from `lint`) currently outputs a human-readable table. The
`--output` flag already has `table|json|text` in its definition (per s2-05a). This
task implements the `json` variant.

**Target JSON schema:**

```json
{
  "burnt_version": "0.1.0",
  "checked_at": "2026-03-13T12:34:56Z",
  "summary": {
    "files_checked": 3,
    "total_violations": 5,
    "errors": 2,
    "warnings": 3,
    "infos": 0
  },
  "violations": [
    {
      "file": "src/jobs/daily_agg.py",
      "line": 42,
      "column": 1,
      "rule_id": "cross_join",
      "severity": "error",
      "message": "Cartesian product detected — add an explicit JOIN condition",
      "suggestion": "Replace with INNER JOIN or LEFT JOIN with ON clause"
    },
    {
      "file": "src/jobs/daily_agg.sql",
      "line": 7,
      "column": 8,
      "rule_id": "select_star",
      "severity": "warning",
      "message": "SELECT * expands all columns — specify columns explicitly",
      "suggestion": "Replace SELECT * with explicit column list"
    }
  ]
}
```

**Why this schema:**
- `file` + `line` + `column` matches GitHub Actions annotation format
- `rule_id` enables downstream filtering (e.g., suppress specific rules in CI)
- `suggestion` gives the CI log enough context to be actionable without opening the file

---

## Part 1: Extend LintViolation model (if not already present)

If `LintViolation` (or equivalent) doesn't already have `suggestion` and `column`
fields, add them. Default `column` to `1` and `suggestion` to `""` if the rule doesn't
provide one.

Do NOT change existing `table` or `text` output — only add `json`.

---

## Part 2: JSON serialization in CLI

In `src/burnt/cli/main.py`, when `--output json`:

```python
import json
import sys
from datetime import datetime, timezone

result_dict = {
    "burnt_version": importlib.metadata.version("burnt"),
    "checked_at": datetime.now(timezone.utc).isoformat(),
    "summary": {
        "files_checked": ...,
        "total_violations": len(violations),
        "errors": sum(1 for v in violations if v.severity == "error"),
        "warnings": sum(1 for v in violations if v.severity == "warning"),
        "infos": sum(1 for v in violations if v.severity == "info"),
    },
    "violations": [
        {
            "file": str(v.file),
            "line": v.line,
            "column": getattr(v, "column", 1),
            "rule_id": v.rule_id,
            "severity": v.severity,
            "message": v.message,
            "suggestion": getattr(v, "suggestion", ""),
        }
        for v in violations
    ],
}
print(json.dumps(result_dict, indent=2))
```

Output goes to stdout. Errors (e.g., file not found) go to stderr even in JSON mode.

---

## Part 3: Exit code behavior

Exit code behavior is controlled by `--fail-on` (already defined in s2-05a), not by
`--output`. These are independent flags. `--output json` does not change when the
command exits non-zero.

---

## Part 4: GitHub Actions annotation format (bonus, if time allows)

If `CI=true` and `GITHUB_ACTIONS=true` env vars are set AND `--output json` is NOT
specified, emit GitHub Actions annotation lines to stderr before the normal table output:

```
::error file=src/jobs/daily_agg.py,line=42,col=1::cross_join: Cartesian product detected
::warning file=src/jobs/daily_agg.sql,line=7,col=8::select_star: SELECT * expands all columns
```

This is a bonus criterion — skip if it significantly increases complexity.

---

## Acceptance Criteria

- [ ] `burnt check ./src/ --output json` produces valid JSON on stdout
- [ ] JSON matches schema above: `burnt_version`, `checked_at`, `summary`, `violations`
- [ ] Each violation has `file`, `line`, `column`, `rule_id`, `severity`, `message`, `suggestion`
- [ ] `summary.errors`, `summary.warnings`, `summary.infos` counts are correct
- [ ] Exit code behavior unchanged by `--output json` (still controlled by `--fail-on`)
- [ ] `--output table` and `--output text` still work (no regression)
- [ ] `jq '.violations[] | select(.severity=="error")' output.json` works (valid JSON)
- [ ] Unit test covers JSON output with at least 2 violations of different severities

---

## Verification

```bash
uv run pytest -m unit -v
uv run ruff check src/ tests/

# JSON output
burnt check ./src/ --output json | python -m json.tool

# Pipe to jq
burnt check ./src/ --output json | jq '.summary'
burnt check ./src/ --output json | jq '.violations[] | select(.severity=="error")'

# Exit code still works
burnt check ./src/ --output json --fail-on error; echo "exit: $?"

# Table still works
burnt check ./src/ --output table
burnt check ./src/ --output text
```

### Integration Check

- [ ] `burnt check --output json` produces parseable JSON with correct violation count
- [ ] Piping to `jq` works without errors

---

## Handoff

### Result

```yaml
status: todo
```

### Blocked reason

Blocked on s2-05a (`burnt check` command must exist with `--output` flag scaffolded).
