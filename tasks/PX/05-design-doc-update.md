```yaml
id: PX-05-design-doc-update
status: done
phase: X
priority: high
agent: claude-sonnet-4-6
completed_by: claude-sonnet-4-6
blocked_by: []
created_by: planner
```

## Context

### Goal

Update DESIGN.md, AGENTS.md, README.md, and `pyproject.toml` to reflect the confirmed strategic decisions: Databricks-first, CLI-first, full notebook hygiene, sparkMeasure session, and removal of dead APIs.

### Files to modify

```
DESIGN.md
AGENTS.md
README.md
pyproject.toml
```

---

## DESIGN.md Changes (section by section)

**§1 Product**
- New tagline: "Notebook quality and cost analysis for Databricks. Like ruff, but for your notebooks."
- Lead with CLI example (`burnt check ./notebook.py`), not Python API
- List three modes in priority order: (1) CLI static lint, (2) interactive coaching, (3) CI gate
- Remove "post-development practice-run reviewer" framing

**§2 Philosophy** — replace 8 principles with 7:
1. Databricks-first. Lint rules work without credentials. Cost intelligence requires Databricks.
2. CLI-first. `burnt check` is the product. Python API is a second mode.
3. Full notebook hygiene. Cost + style + structure rules. "ruff for Databricks notebooks."
4. Static + runtime. Static rules always run. sparkMeasure enriches when session is active.
5. Compute seconds over dollars. Actionable everywhere. Backends map to USD.
6. Honest confidence. Distinguish observed data from estimates.
7. Graceful degradation. No creds → 84 rules. Spark session → enrichment. Databricks → dollar estimates.

**§3 Environments** — reorder, CLI goes first:
1. CLI (primary)
2. In-Notebook (coaching mode)
3. CI/CD (gate mode)
4. Connected / `pip install burnt[databricks]` (optional)

**§5 Session Lifecycle** — complete rewrite:
- Replace SparkListener/statusTracker approach with sparkMeasure
- `start_session()` → `StageMetrics(spark).begin()` (or REST fallback)
- `check()` → `metrics.end()` → `metrics.create_df().collect()` → correlate to graph nodes
- Remove all `statusTracker` references

**§7 Rules** — replace stale table with accurate one:

| Category | Prefix | Count | Examples |
|---|---|---|---|
| Performance | BP* | ~18 | collect without limit, crossJoin, repartition(1) |
| SQL quality | SQ*, BQ* | ~10 | SELECT *, NOT IN with NULLs, correlated subquery |
| Delta / Lake | BD* | ~5 | missing ZORDER, VACUUM frequency |
| DLT / SDP | SDP* | ~5 | missing expectation, streaming without key |
| Notebook style | BNT_* | ~3 | generic df name, star import |
| Notebook structure | BB*, BN* | varies | magic in plain Python, deprecated syntax |

Remove the line "No style rules, no generic notebook hygiene."

**§12 CLI** — expand to full section:
- Document all real commands: `check`, `rules`, `init`, `doctor`, `cache`
- Remove `burnt advise`, `burnt tutorial`
- Add `--output sarif`, `--event-log`, `--max-cost` flags

**§13 CI Integration** — add new section with:
- Pre-commit hook YAML
- GitHub Actions lint gate (with SARIF upload)
- GitHub Actions cost gate (`--max-cost`)
- Databricks Asset Bundles pre-deploy hook

**§14 Package Structure** — update to reality:
- Remove `spark/listener.py` entry (gone)
- Add `runtime/sparkmeasure.py` (new)
- Add `runtime/event_log.py` (new, for `--event-log`)
- Remove `intelligence/` (pre-pivot debris)

**§17 Phases** — rewrite roadmap table to match current reality

---

## AGENTS.md Changes

- **Tagline**: "Python pre-execution Databricks cost estimation" → "Notebook quality and cost analysis for Databricks. CLI-first."
- **Python version**: "Python 3.12" → "Python 3.10+"
- **Typing guide**: Remove the `Optional[X]`, `List[X]`, `Dict[str, Any]` rule entirely. Replace with: "Use `X | Y` union syntax and built-in generics (`list[str]`, `dict[str, Any]`) throughout. `from __future__ import annotations` is present in all files."
- **Add rule**: "No dead-code stubs — if a function is not implemented, remove it rather than leaving a broken placeholder."

---

## README.md Changes

- Remove from Python API section: `result.api_json()`, `result.calibrate(job_id=..., run_id=...)`
- Update CLI section: remove `--explain`, `--cluster databricks.yml:prod` (flags that don't exist), align with real flags
- Fix Python badge: change `python-3.10+-blue` to match actual `requires-python` after pyproject.toml fix
- Remove "Requires Databricks connection." — replace with: "Works without credentials (84 lint rules). Add `pip install burnt[databricks]` for cost estimation."
- Remove `burnt check --explain` and `burnt check --explain BP007` (doesn't exist; `burnt rules` is the equivalent)

---

## pyproject.toml Changes

- `description`: "Pre-execution cost estimation for Databricks workloads" → "Notebook quality and cost analysis for Databricks"
- `requires-python`: `">=3.12"` → `">=3.10"`
- `dependencies`: remove `"requests>=2.32,<3"` from core (move to `[databricks]` optional)
- `dependencies`: change `"rich>=13.0,<14"` → `"rich>=13.0"`
- `optional-dependencies`: add `spark = ["sparkmeasure>=2.0"]`
- `optional-dependencies`: add `requests` to `databricks` group
- `all` extra: update to include `spark`

---

## Acceptance Criteria

- [x] DESIGN.md §7 rule table has all 6 categories and correct prefixes
- [x] DESIGN.md §5 describes sparkMeasure as the session mechanism (no SparkListener mention)
- [x] DESIGN.md has a §13 CI Integration section with pre-commit, GitHub Actions, and DABs examples
- [x] DESIGN.md §2 has 7 principles starting with "Databricks-first"
- [x] AGENTS.md typing guide says `X | Y` not `Optional[X]`
- [x] AGENTS.md tagline is updated
- [x] README.md Python API section contains no references to `api_json()`, `calibrate()`, `advise()`
- [x] `pyproject.toml` description does not say "pre-execution"
- [x] `pyproject.toml` `requires-python = ">=3.10"`
- [x] `pyproject.toml` has `spark = ["sparkmeasure>=2.0"]` optional extra
- [x] `requests` is not in core `dependencies`

## Implementation

### Changes Made

- `DESIGN.md` — rewrote §1 (tagline + CLI-first output), §2 (7 principles), §3 (CLI-first environments), §4 (sparkMeasure in arch diagram), §5 (sparkMeasure session lifecycle), §7 (6-category rule table), §8 (Databricks-only backend mapping), §9 (SARIF in export list), §10 (removed burnt.watch()), §11 (removed [watch]/[alert] config blocks), §12 (expanded CLI section), inserted new §13 CI Integration, renumbered §14 Stack, §15 Package Structure (updated file tree), §16 Python API (removed stale methods), §17 Design Principles (7 aligned with §2), §18 Phases (current roadmap)
- `AGENTS.md` — updated tagline, Python version (3.10+), typing guide (X|Y not Optional[X]), added no-dead-code-stubs rule, updated common commands (maturin develop)
- `README.md` — replaced Requires Databricks sentence, removed api_json()/calibrate()/burnt.watch(), fixed CLI section (removed --explain/--cluster), cleaned config examples (removed [watch]/[alert]), updated Architecture section
- `pyproject.toml` — description updated, requires-python lowered to >=3.10, rich constraint loosened to >=13.0, requests moved to databricks extra, added spark = ["sparkmeasure>=2.0"], all extra updated, ruff target-version lowered to py310

## Verification

```bash
grep "sparkMeasure" DESIGN.md
grep "Databricks-first" DESIGN.md
grep "CI Integration" DESIGN.md
grep "Optional\[" AGENTS.md || echo "Clean"
grep "api_json\|calibrate" README.md || echo "Clean"
grep "pre-execution" pyproject.toml || echo "Clean"
python -c "import tomllib; d=tomllib.load(open('pyproject.toml','rb')); print(d['project']['requires-python'])"
```
