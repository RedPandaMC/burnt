# Task: burnt check — CI Output Formats (JSON + SARIF)

---

## Metadata

```yaml
id: s7-07-ci-output-formats
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

Deliver two machine-readable output formats from `burnt check` — a rich JSON schema and SARIF (Static Analysis Results Interchange Format). Both live entirely in the Python CLI layer (`main.py`): the Rust core returns `Vec<Finding>`; Python serialises them. Supersedes and extends `s2-09-check-json` (which only specified a basic JSON schema).

### Files to read

```
# Required
src/burnt/cli/main.py   # output rendering — add json and sarif branches

# Reference
tasks/s2-09-check-json.md     # original JSON spec to extend
tasks/s7-04-rust-semantic-core.md   # Finding dict schema from Rust
```

### Background

**JSON output (extends s2-09)**

The `Finding` dict from `burnt_rs` has keys: `rule_id`, `severity`, `line`, `description`, `suggestion`, `file`. Wrap this in a richer envelope:

```json
{
  "burnt_version": "0.2.0",
  "checked_at": "2026-03-17T12:00:00Z",
  "summary": {
    "files_checked": 12,
    "cells_checked": 47,
    "total_violations": 8,
    "errors": 2,
    "warnings": 5,
    "info": 1,
    "style": 0
  },
  "violations": [
    {
      "file": "src/jobs/daily_agg.py",
      "line": 42,
      "column": 1,
      "rule_id": "cross_join",
      "code": "BP007",
      "severity": "error",
      "message": "CROSS JOIN creates O(n*m) rows",
      "suggestion": "Use INNER JOIN with explicit ON clause",
      "cell_index": null
    },
    {
      "file": "notebooks/etl.ipynb",
      "line": 3,
      "column": 1,
      "rule_id": "select_star",
      "code": "BQ002",
      "severity": "warning",
      "message": "SELECT * without LIMIT returns all rows",
      "suggestion": "Add LIMIT clause or select specific columns",
      "cell_index": 2
    }
  ]
}
```

New fields vs s2-09:
- `code`: the BP/BQ/BNT rule code (from REGISTRY, passed through from Rust)
- `cell_index`: notebook cell index when finding came from a notebook cell (null for plain files)
- `summary.cells_checked`: counts notebook cells, not just files
- `summary.style`: separate count for STYLE-severity findings

**SARIF output (`--output sarif`)**

SARIF 2.1.0 is the format GitHub Code Scanning, VS Code Problems panel, and Azure DevOps consume natively. A minimal valid SARIF document:

```json
{
  "$schema": "https://json.schemastore.org/sarif-2.1.0.json",
  "version": "2.1.0",
  "runs": [
    {
      "tool": {
        "driver": {
          "name": "burnt",
          "version": "0.2.0",
          "informationUri": "https://github.com/your-org/burnt",
          "rules": [
            {
              "id": "BP007",
              "name": "cross_join",
              "shortDescription": { "text": "CROSS JOIN creates O(n*m) rows" },
              "helpUri": "https://github.com/your-org/burnt/wiki/rules/BP007",
              "properties": { "tags": ["performance", "cost"] }
            }
          ]
        }
      },
      "results": [
        {
          "ruleId": "BP007",
          "level": "error",
          "message": { "text": "CROSS JOIN creates O(n*m) rows — Use INNER JOIN with explicit ON clause" },
          "locations": [
            {
              "physicalLocation": {
                "artifactLocation": { "uri": "src/jobs/daily_agg.py", "uriBaseId": "%SRCROOT%" },
                "region": { "startLine": 42, "startColumn": 1 }
              }
            }
          ]
        }
      ]
    }
  ]
}
```

SARIF severity mapping:
- `error` → `"level": "error"`
- `warning` → `"level": "warning"`
- `info` → `"level": "note"`
- `style` → `"level": "none"` (informational only)

**GitHub Actions annotation format (automatic)**

When `CI=true` and `GITHUB_ACTIONS=true` environment variables are set, emit annotation lines to stderr regardless of `--output` mode. These produce inline PR annotations:

```
::error file=src/jobs/daily_agg.py,line=42,col=1,title=BP007 cross_join::CROSS JOIN creates O(n*m) rows
::warning file=src/jobs/daily_agg.sql,line=7,col=1,title=BQ002 select_star::SELECT * without LIMIT
```

```python
def _emit_github_annotations(findings: list[dict]) -> None:
    import os
    if not (os.getenv("CI") and os.getenv("GITHUB_ACTIONS")):
        return
    for f in findings:
        level = f["severity"] if f["severity"] in ("error", "warning") else "notice"
        code = f.get("code", f["rule_id"])
        print(
            f'::{level} file={f["file"]},line={f["line"]},col=1,'
            f'title={code} {f["rule_id"]}::{f["description"]}',
            file=sys.stderr,
        )
```

**CLI flag**

```python
output: str = typer.Option(
    "table", "--output", "-o",
    help="Output format: table|text|json|sarif"
)
```

**`column` field**

Rust `Finding` currently has no `column`. For now default to `1`. Add column tracking to the Rust visitor as a follow-on if needed.

**REGISTRY lookup for `code` field**

The Rust `Finding` should include the `code` field (e.g., `"BP007"`) alongside `rule_id`. Update `Finding.to_pydict()` in s7-04 if not already included.

---

## Acceptance Criteria

- [ ] `burnt check ./src/ --output json` → valid JSON matching schema above; `python -m json.tool` succeeds
- [ ] JSON includes `code`, `cell_index`, `summary.cells_checked`, `summary.style` fields
- [ ] `burnt check ./src/ --output sarif` → valid SARIF 2.1.0; `jq '.runs[0].results | length'` works
- [ ] SARIF `runs[0].tool.driver.rules` lists every rule that fired
- [ ] `--output table` and `--output text` still work (no regression)
- [ ] GitHub Actions annotations emitted to stderr when `CI=true && GITHUB_ACTIONS=true`
- [ ] Exit code behaviour unchanged (controlled by `--fail-on`, not `--output`)
- [ ] `uv run pytest -m unit -v` passes
- [ ] `uv run ruff check src/ tests/` clean

---

## Verification

```bash
uv run pytest -m unit -v
uv run ruff check src/burnt/cli/

# JSON round-trip
burnt check ./src/ --output json | python -m json.tool
burnt check ./src/ --output json | jq '.summary'
burnt check ./src/ --output json | jq '.violations[] | select(.severity=="error")'

# SARIF validation
burnt check ./src/ --output sarif | jq '.runs[0].results | length'
burnt check ./src/ --output sarif | jq '.runs[0].tool.driver.rules[0]'

# GitHub Actions annotations
CI=true GITHUB_ACTIONS=true burnt check ./src/ --output table 2>&1 | grep "^::"

# Exit code unchanged
burnt check ./src/ --output json --fail-on error; echo "exit: $?"
burnt check ./src/ --output sarif --fail-on error; echo "exit: $?"
```

---

## Handoff

### Result

[Executor fills this in when done.]

```yaml
status: todo
```

### Blocked reason

Requires s7-04 so that `burnt_rs.analyze_file()` is the analysis source (findings include `code` field and `cell_index`).
