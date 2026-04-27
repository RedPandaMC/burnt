```yaml
id: PX-04-sarif-output
status: todo
phase: X
priority: high
agent: ~
blocked_by: [PX-03-cli-rewire]
created_by: planner
```

## Context

### Goal

Add SARIF 2.1.0 output format to `display/export.py` and wire it to `--output sarif` in the CLI. SARIF is the standard format for GitHub Code Scanning — it enables inline PR annotations with zero configuration.

### Files to modify

```
# Required
src/burnt/display/export.py    (add report_to_sarif())
src/burnt/cli/main.py          (add sarif to --output choices)

# Reference
DESIGN.md §13 (CI Integration)
tasks/P3/06-cli-implementation.md
tasks/P5/06-ci-examples.md
```

### Background

SARIF 2.1.0 minimal structure for a linting tool:

```json
{
  "$schema": "https://json.schemastore.org/sarif-2.1.0.json",
  "version": "2.1.0",
  "runs": [{
    "tool": {
      "driver": {
        "name": "burnt",
        "version": "0.2.0",
        "informationUri": "https://github.com/redpandamc/burnt",
        "rules": [
          { "id": "BP014", "name": "cross_join", "shortDescription": { "text": "CROSS JOIN creates O(n*m) rows" } }
        ]
      }
    },
    "results": [
      {
        "ruleId": "BP014",
        "level": "warning",
        "message": { "text": "CROSS JOIN creates O(n*m) rows → Use INNER JOIN with explicit ON clause" },
        "locations": [{
          "physicalLocation": {
            "artifactLocation": { "uri": "notebooks/pipeline.py", "uriBaseId": "%SRCROOT%" },
            "region": { "startLine": 42 }
          }
        }]
      }
    ]
  }]
}
```

**SARIF level mapping:** `error` → `"error"`, `warning` → `"warning"`, `info` → `"note"`.

---

## Acceptance Criteria

- [ ] `display/export.py` has `report_to_sarif(result: CheckResult) -> dict` function
- [ ] SARIF output contains `$schema`, `version: "2.1.0"`, `runs[0].tool.driver.name: "burnt"`
- [ ] `runs[0].tool.driver.rules` lists every unique rule ID present in findings
- [ ] Each finding maps to a `runs[0].results[i]` entry with: `ruleId`, `level`, `message.text` (message + suggestion combined), `locations[0].physicalLocation`
- [ ] `locations[0].physicalLocation.region.startLine` = `finding.line_number` (omit `region` if `line_number` is None)
- [ ] `locations[0].physicalLocation.artifactLocation.uri` = file path relative to cwd
- [ ] CLI: `--output sarif` writes SARIF JSON to stdout
- [ ] `burnt check src/ --output sarif | python -m json.tool` exits 0 (valid JSON)
- [ ] Unit test: `report_to_sarif(result_with_2_findings)` → correct structure validated against expected dict

## Verification

```bash
burnt check tests/fixtures/e2e/cross_join.py --output sarif | python -m json.tool > /dev/null
burnt check tests/fixtures/e2e/cross_join.py --output sarif | python -c "
import json, sys
d = json.load(sys.stdin)
assert d['version'] == '2.1.0'
assert d['runs'][0]['tool']['driver']['name'] == 'burnt'
assert len(d['runs'][0]['results']) > 0
assert d['runs'][0]['results'][0]['locations'][0]['physicalLocation']['region']['startLine'] > 0
print('SARIF OK')
"

uv run pytest tests/unit/display/test_sarif.py -v
```

### Integration Check

- [ ] Upload the SARIF file to a GitHub repo using `github/codeql-action/upload-sarif@v3` and confirm findings appear as inline PR annotations
