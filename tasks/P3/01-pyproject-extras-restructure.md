# Task: pyproject.toml extras restructure

---

## Metadata

```yaml
id: P3-01-pyproject-extras-restructure
status: done
phase: 3
priority: critical
agent: ~
blocked_by: [P2-01a]
created_by: planner
```

---

## Context

### Goal

Restructure `pyproject.toml` optional extras to match the modular-architecture decision
in `docs/modular-architecture.md` §2.2 and §4. Remove the extras that no longer belong
(`[sql]`, `[spark]`, `[alerts]`), and introduce the new cloud-pricing extras
(`[databricks]`, `[azure-databricks]`, `[aws-databricks]`, `[gcp-databricks]`,
`[onprem-spark]`).

### Files to modify

```
pyproject.toml
```

### Background

**`sparkmeasure` is dropped entirely** — the April 2026 pivot replaces it with the
Spark monitoring REST API. It is removed from core deps and the `[spark]` extra is
removed. No extra gates sparkMeasure.

**`libcst` is not added to core** — `--fix` / `--unsafe-fixes` (P3-04) are implemented
in the Rust engine via `tree-sitter::InputEdit`. No Python-side AST library needed.

**`[sql]` removed** — the Rust engine's tree-sitter SQL coverage replaces `sqlglot`.

**`[alerts]` removed** — Slack/webhook dispatch is an orchestration concern, not burnt's.

**`[notebook]` extra does not exist** — `.dbc` archive parsing ships in core parsers.
HTML output is removed entirely. No jinja2 dependency anywhere.

**`[databricks]`** = workspace API client + system-table reader only (no pricing data).
Every `[*-databricks]` pricing extra declares `[databricks]` as a dependency.

**`[onprem-spark]`** = no external deps; pure config + arithmetic.

### Target `pyproject.toml` extras

```toml
[project.optional-dependencies]
databricks       = ["databricks-sdk>=0.50,<1", "requests>=2.32,<3"]
azure-databricks = ["burnt[databricks]", "azure-mgmt-compute>=33,<34", "azure-mgmt-billing>=6,<7"]
aws-databricks   = ["burnt[databricks]", "boto3>=1.35,<2"]
gcp-databricks   = ["burnt[databricks]", "google-cloud-billing>=1.15,<2"]
onprem-spark     = []
all              = ["burnt[azure-databricks,aws-databricks,gcp-databricks,onprem-spark]"]
```

---

## Acceptance Criteria

- [x] `[project.optional-dependencies]` no longer contains `sql`, `spark`, or `alerts` keys
- [x] `[databricks]` extra: `databricks-sdk>=0.50,<1`, `requests>=2.32,<3`
- [x] `[azure-databricks]` extra: auto-pulls `burnt[databricks]` + Azure pricing deps
- [x] `[aws-databricks]` extra: auto-pulls `burnt[databricks]` + boto3
- [x] `[gcp-databricks]` extra: auto-pulls `burnt[databricks]` + google-cloud-billing
- [x] `[onprem-spark]` extra: empty (no additional deps)
- [x] `[all]` = `burnt[azure-databricks,aws-databricks,gcp-databricks,onprem-spark]`
- [x] No `sparkmeasure`, `libcst`, `jinja2`, `sqlglot`, `slack-sdk` in any dep list
- [x] `uv run ruff check src/` passes (no dead imports from removed extras)

---

## Verification

```bash
uv sync --all-extras
uv run pytest -m unit -v
uv run ruff check src/ tests/

# Verify correct extras
python -c "from importlib.metadata import requires; print([r for r in requires('burnt') or [] if 'databricks' in r])"

# Verify no sparkmeasure/libcst in core
pip show burnt | grep -i "sparkmeasure\|libcst\|sqlglot\|slack" && echo "FAIL" || echo "OK"
```

### Integration Check

- [x] `pip install burnt` does not install databricks-sdk, sparkmeasure, or libcst
- [x] `pip install burnt[azure-databricks]` installs databricks-sdk transitively
- [x] `pip install burnt[onprem-spark]` does NOT install databricks-sdk
