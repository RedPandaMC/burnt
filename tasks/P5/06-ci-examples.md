```yaml
id: P5-06-ci-examples
status: todo
phase: 5
priority: high
agent: ~
blocked_by: [PX-04-sarif-output, P3-06-cli-implementation]
created_by: planner
```

## Context

### Goal

Produce complete, copy-pasteable CI integration examples that show `burnt check` working as a pre-commit hook, a GitHub Actions lint gate with SARIF upload, a cost budget gate, and a Databricks Asset Bundles pre-deploy step. These examples ARE the documentation for the CLI-first use case.

### Files to create/modify

```
# Create
.github/workflows/burnt-check.yml
.github/workflows/burnt-cost-gate.yml
.pre-commit-config.yaml.example
docs/ci-integration.md

# Modify
DESIGN.md §13 (CI Integration) — add links to doc files
```

---

## Acceptance Criteria

### `.github/workflows/burnt-check.yml` — lint gate with SARIF upload

- [ ] Installs `burnt` with no extras (static analysis only, no Databricks credentials needed)
- [ ] Runs `burnt check` on the notebooks directory
- [ ] Outputs SARIF to a file with `--output sarif`
- [ ] Uploads SARIF to GitHub Code Scanning via `github/codeql-action/upload-sarif@v3`
- [ ] Runs on `pull_request` trigger
- [ ] Annotates PR with inline findings (this happens automatically via Code Scanning + SARIF)

### `.github/workflows/burnt-cost-gate.yml` — cost budget gate

- [ ] Uses `--max-cost` flag to fail the PR if estimated cost exceeds threshold
- [ ] Uses `--fail-on error` to fail on error-severity findings only
- [ ] Includes comment showing how to override with `# burnt: ignore[BP014]`

### `.pre-commit-config.yaml.example` — pre-commit hook

- [ ] Works with `pre-commit run --all-files`
- [ ] Only runs on `.py` and `.sql` files
- [ ] Uses `additional_dependencies: [burnt]` for isolation
- [ ] Includes note: "Remove `.example` suffix and add to `.pre-commit-config.yaml`"

### `docs/ci-integration.md` — narrative documentation

- [ ] Explains the three CI modes: lint gate / cost gate / coaching
- [ ] Explains that no Databricks credentials are needed for static analysis
- [ ] Shows Databricks Asset Bundles pre-deploy hook example:
  ```yaml
  # databricks.yml
  bundle:
    hooks:
      pre-deploy:
        - burnt check ./src/ --fail-on error
  ```
- [ ] Explains SARIF and GitHub Code Scanning integration (what it looks like in a PR)
- [ ] Links to `--output sarif`, `--max-cost`, `--fail-on` CLI docs

### Validation

- [ ] `python -m json.tool < .github/workflows/burnt-check.yml` does not error (valid YAML → valid when parsed)
- [ ] SARIF example output validates against SARIF 2.1.0 schema

## Verification

```bash
# Validate YAML
python -c "import yaml; yaml.safe_load(open('.github/workflows/burnt-check.yml'))"
python -c "import yaml; yaml.safe_load(open('.github/workflows/burnt-cost-gate.yml'))"

# SARIF smoke test
burnt check tests/fixtures/e2e/cross_join.py --output sarif | python -m json.tool > /dev/null
```

### Integration Check

- [ ] Manually run `pre-commit run --all-files` in the repo with the `.example` config renamed — confirms hook fires on `.py` files
