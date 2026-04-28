# Task: pyproject.toml extras restructure

---

## Metadata

```yaml
id: PN-01-pyproject-extras-restructure
status: todo
phase: N
priority: critical
agent: ~
blocked_by: [PX-01-remove-dead-code]
created_by: planner
```

---

## Context

### Goal

Restructure `pyproject.toml` optional extras to match the modular-architecture decision recorded in `docs/modular-architecture.md` §2.2 and §2.3. Remove the extras that no longer belong (`[sql]`, `[spark]`, `[alerts]`), promote `sparkmeasure` and `libcst` to core dependencies, and introduce the new extras (`[notebook]`, `[databricks]` repositioned, `[azure-databricks]`, `[aws-databricks]`, `[gcp-databricks]`, `[onprem-spark]`).

The `[databricks]` extra is repositioned as the workspace API client + system-table reader only (no pricing data lives there). Every `[*-databricks]` pricing extra declares `[databricks]` as a dependency so it is auto-installed transitively. `[onprem-spark]` is fully self-contained (no cloud SDK).

### Files to read

```
# Required
pyproject.toml
docs/modular-architecture.md   §2.1, §2.2, §2.3, §4
src/burnt/__init__.py
src/burnt/runtime/__init__.py

# Reference
DESIGN.md §14 Stack
```

### Background

`sparkmeasure` is promoted to core because runtime metric capture is fundamental to the "honest confidence" pitch — the package's core value is compute-seconds, and that requires sparkMeasure to observe them. Gating it behind `[spark]` was wrong.

`libcst` is promoted to core because `--fix` / `--unsafe-fixes` (PN-04) are core CLI flags, not extras.

`[sql]` is removed because the Rust engine's tree-sitter SQL coverage replaces `sqlglot`.

`[alerts]` is removed because result dispatch (Slack, webhooks) is an orchestration concern, not burnt's job.

The `[all]` extra should reference the new complete set.

---

## Acceptance Criteria

- [ ] `[project.dependencies]` includes `sparkmeasure>=2.0` and `libcst>=1.0,<2`
- [ ] `[project.optional-dependencies]` no longer contains `sql`, `spark`, or `alerts` keys
- [ ] `[notebook]` extra defined: `jinja2>=3,<4` only
- [ ] `[databricks]` extra: `databricks-sdk>=0.50,<1`, `requests>=2.32,<3` (unchanged content, repositioned role)
- [ ] `[azure-databricks]` extra defined: auto-pulls `burnt[databricks]` + any Azure pricing dep (placeholder: `"burnt[databricks]"` only for now; actual pricing deps added in P4)
- [ ] `[aws-databricks]` extra defined: auto-pulls `burnt[databricks]`
- [ ] `[gcp-databricks]` extra defined: auto-pulls `burnt[databricks]`
- [ ] `[onprem-spark]` extra defined: no external deps (pure config + arithmetic, self-contained)
- [ ] `[all]` = `burnt[notebook,databricks,azure-databricks,aws-databricks,gcp-databricks,onprem-spark]`
- [ ] `uv run python -c "import burnt"` succeeds in a clean venv with only core deps
- [ ] `uv run ruff check src/` passes (no dead imports from removed extras)

---

## Verification

### Commands

```bash
uv sync --all-extras
uv run pytest -m unit -v
uv run ruff check src/ tests/
# Verify extras are importable
uv run python -c "from sparkmeasure import StageMetrics; print('sparkmeasure ok')"
uv run python -c "import libcst; print('libcst ok')"
```

### Integration Check

- [ ] `pip install burnt` in a clean venv installs sparkmeasure and libcst as core deps, does not install databricks-sdk
- [ ] `pip install burnt[azure-databricks]` installs databricks-sdk transitively (via `burnt[databricks]`)
- [ ] `pip install burnt[onprem-spark]` does NOT install databricks-sdk

---

## Handoff

### Result

[Executor fills this in when done.]

```yaml
status: todo
```

### Blocked reason

Blocked by PX-01 (remove dead code that imports `alerts`/`sql` extras). Cannot restructure extras until dead imports are gone.
