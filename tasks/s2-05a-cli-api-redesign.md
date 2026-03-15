# Task: CLI/API Redesign — Full UX Overhaul (v3)

---

## Metadata

```yaml
id: s2-05a-cli-api-redesign
status: done
phase: 2
priority: critical
agent: ~
blocked_by: []
created_by: planner
```

---

## Context

### Goal

Redesign burnt CLI and Python API for clean UX separation:

- **CLI** = static analysis tooling. Zero credentials required. Works offline. CI-friendly.
- **Python API** = runtime cost intelligence. Requires Databricks credentials for live features.

This is the sprint's primary task. Tasks s2-06 through s2-09 are blocked on this one.

### Files to read

```
# Required
src/burnt/__init__.py
src/burnt/cli/main.py
src/burnt/estimators/whatif.py          ← being replaced
src/burnt/estimators/simulation.py      ← partial implementation exists, verify/complete
src/burnt/core/models.py
src/burnt/core/config.py
src/burnt/core/exceptions.py
src/burnt/advisor/report.py
src/burnt/advisor/session.py
src/burnt/whatif/__init__.py

# Reference
tasks/s2-04-ast-lint-rules.md
tests/unit/estimators/test_whatif.py    ← must be updated to new names
```

### Background

**Partial implementation note:** `src/burnt/estimators/simulation.py` was created as a
draft during planning. Read it before deciding whether to complete it or rewrite from
scratch. It should NOT be treated as authoritative — verify each class against the
acceptance criteria below. The old `whatif.py` remains the reference implementation.

**Two user personas:**
1. **Engineer (terminal / CI)** — `burnt check src/` with no credentials. Zero setup.
2. **Analyst (notebook / Python)** — `import burnt; burnt.estimate(sql).simulate()...`

**Config precedence (same for CLI and Python API):**
```
call-time kwargs > .burnt.toml > pyproject.toml [tool.burnt] > env vars (BURNT_*) > defaults
```
Discovery walks upward from CWD toward filesystem root (stops at git root or HOME).

---

## Part 1: CLI Commands (Definitive List)

### Keep / Add

| Command | Description |
|---------|-------------|
| `burnt check <path>` | Anti-pattern detection (renamed from `lint`) |
| `burnt init` | Interactive project setup |
| `burnt tutorial` | Generate `examples/` notebooks |
| `burnt cache show` | List `.burnt/cache/` files + sizes |
| `burnt cache clear` | Remove cache files (prompts confirmation) |
| `burnt rules` | Interactive TUI → updates active config file |
| `burnt advise` | REST-based run analysis (kept in CLI; `--self` removed) |
| `burnt doctor` | Defined in task s2-08 — scaffold stub here |
| `burnt --version` | Flag, not subcommand |

### `burnt check` flags

```bash
burnt check <path>
  --fail-on error|warning|info   # exit code threshold (default: error)
  --output table|json|text       # json detailed in s2-09; implement table|text here
  --ignore-rule <rule_id>        # one-off rule override (can repeat)
```

### `burnt advise` flags (updated)

```bash
burnt advise
  --run-id R
  --statement-id S
  --job-id J
  --job-name N
  --output table|json|text
  # --self flag REMOVED (requires SparkSession — Python API only)
```

### Remove (no deprecated aliases)

- `burnt estimate` → Python API only
- `burnt whatif` → Python API only
- `burnt lint` → replaced by `burnt check`
- `burnt version` (subcommand) → `burnt --version` flag

---

## Part 2: Config Redesign

### Design principles (follow ecosystem standards)

Burnt follows the same conventions as ruff, mypy, and black:

- **TOML only** — not YAML. Python ecosystem standard is TOML.
- **`select`/`ignore` flat lists** — not a per-rule `enabled`+`severity` dict. That is a
  type-checker pattern (Pyright). Linters use flat lists.
- **No per-rule severity override** — severity is fixed in code per rule. Users can only
  enable or disable rules. This matches ruff, mypy, pylint.
- **`fail-on` is the global threshold** — controls at which severity level the exit code
  becomes non-zero. Independent of which rules fire.
- **Per-file ignores via glob** — ruff's `per-file-ignores` pattern.

### Config files supported

**Primary (project-wide):** `pyproject.toml [tool.burnt]`
**Standalone:** `.burnt.toml` (takes precedence over `pyproject.toml` when both exist)

Discovery walks upward from CWD toward filesystem root (stops at git root or HOME):
- `.burnt.toml` found → use it, stop
- `pyproject.toml` with `[tool.burnt]` found → use it, stop
- Neither → use env vars + defaults

### `.burnt.toml` (standalone, no prefix needed)

```toml
[lint]
select = ["ALL"]                         # "ALL" or explicit list of rule IDs
ignore = ["python_udf", "toPandas"]     # disable specific rules
fail-on = "error"                        # info | warning | error (exit code threshold)
exclude = ["tests/", "*.ipynb"]

[lint.per-file-ignores]
"tests/*" = ["select_star", "collect_without_limit"]
"migrations/*" = ["cross_join"]

[cache]
enabled = true
ttl-seconds = 300
```

### `pyproject.toml [tool.burnt]`

```toml
[tool.burnt]
workspace-url = "https://adb-123.azuredatabricks.net"

[tool.burnt.lint]
select = ["ALL"]
ignore = ["python_udf", "toPandas"]
fail-on = "error"
exclude = ["tests/", "*.ipynb"]

[tool.burnt.lint.per-file-ignores]
"tests/*" = ["select_star", "collect_without_limit"]
"migrations/*" = ["cross_join"]

[tool.burnt.cache]
enabled = true
ttl-seconds = 300
```

### Rule IDs

Rule IDs are symbolic names (like mypy error codes, pylint message names):

| Rule ID | Default Severity |
|---------|-----------------|
| `cross_join` | warning |
| `select_star` | error |
| `collect_without_limit` | error |
| `python_udf` | error |
| `toPandas` | error |
| `repartition_one` | warning |
| `order_by_no_limit` | warning |
| `pandas_udf` | warning |
| `count_without_filter` | warning |
| `withColumn_in_loop` | warning |
| `jdbc_incomplete_partition` | error |
| `sdp_prohibited_ops` | error |

`select = ["ALL"]` enables all rules (default). Users disable via `ignore`.
Users can also `select` only specific rules: `select = ["cross_join", "select_star"]`.

Inline suppression (per-line): `# burnt: ignore[cross_join]`

### Config changes in `core/config.py`

- Add nested `lint.select` (list[str], default `["ALL"]`)
- Add nested `lint.ignore` (list[str], default `[]`)
- Add nested `lint.fail_on` (str, default `"error"`)
- Add nested `lint.exclude` (list[str], default `[]`)
- Add nested `lint.per_file_ignores` (dict[str, list[str]], default `{}`)
- Add nested `cache.enabled` (bool, default `True`)
- Add nested `cache.ttl_seconds` (float, default `3600.0`)
- Remove `ignore_rules` flat field
- Add `Settings.from_toml(path: Path) -> Settings` (handles both `.burnt.toml` and `pyproject.toml`)
- Add `Settings.discover(cwd: Path = None) -> tuple[Path | None, Settings]`
  — walks upward from CWD, returns (config_path, merged_settings)
- Add `Settings.merge(*settings: Settings) -> Settings`
  — priority order: first arg wins for each field

### `burnt rules` — dynamic config target

Detects and writes to whichever config is active:
1. `.burnt.toml` exists in CWD or ancestor → write to it
2. `pyproject.toml` has `[tool.burnt]` → write to it
3. Neither found → error: "No config found. Run `burnt init` first."

TUI shows all rules with their default severity and current enabled/disabled state.
Toggling a rule adds/removes it from the `ignore` list in the active config.

### `burnt init` behavior

1. Prompt: "Config format? [pyproject.toml / .burnt.toml]" (default: pyproject.toml if it exists, else .burnt.toml)
2. If pyproject.toml chosen and `[tool.burnt]` already exists → prompt "Overwrite? [y/N]"
3. If `.burnt.toml` chosen and it already exists → prompt "Overwrite? [y/N]"
4. Add `.burnt/cache/` to `.gitignore`
5. Prompt: "Generate examples? [Y/n]" → calls `burnt tutorial`

---

## Part 3: Module Rename — whatif → simulation

```
src/burnt/estimators/whatif.py  →  src/burnt/estimators/simulation.py
src/burnt/whatif/__init__.py    →  update imports to point at simulation
```

### Class renames

| Old | New |
|-----|-----|
| `WhatIfBuilder` | `Simulation` |
| `WhatIfResult` | `SimulationResult` |
| `MultiScenarioResult` | `MultiSimulationResult` |
| `WhatIfModification` | `SimulationModification` |

No public backward-compat aliases. These are pre-release renames.

---

## Part 4: Simulation Builder Redesign

### Internal state change (CRITICAL)

Replace flat attributes (`_photon_query_type`, `_to_serverless`, etc.) with a
per-scenario dict:

```python
@dataclass
class _ScenarioState:
    # All mutable state for one named scenario
    photon_query_type: str | None = None
    target_instance: str | None = None
    target_workers: int | None = None
    use_spot: bool | None = None
    spot_fallback: bool = True
    disable_spot_flag: bool = False
    use_pool: bool = False
    pool_instance_pool_id: str | None = None
    pool_use_spot: bool = False
    pool_min_idle: int = 0
    to_serverless: bool = False
    serverless_utilization: float = 50.0
    extra_multipliers: list[_ExtraMod] = field(default_factory=list)
```

`Simulation` holds:
```python
_unnamed_state: _ScenarioState          # used when no scenario() called
_named_scenarios: dict[str, _ScenarioState]  # used when scenario() is called
_current_scenario: str | None = None   # which scenario is being built
```

### `scenario(name)` pattern

```python
result = (
    estimate.simulate()
    .scenario("Photon").cluster().enable_photon()
    .scenario("Serverless").cluster().to_serverless()
    .scenario("Full").cluster().enable_photon().data_source().to_delta_format()
    .compare()
)  # → MultiSimulationResult
```

**Guard:** if modifications are applied to `_unnamed_state` first, then `scenario()` is
called → raise `ValueError("Cannot mix pre-scenario modifications with named scenarios. Call scenario() first.")`.

### Remove from API

- `Simulation.options()` — removed entirely (IDE handles discovery)
- `.scenarios()` method — removed from ALL sub-contexts (ClusterContext, DataSourceContext, SparkConfigContext)

### `ClusterContext` additions

- `disable_spot()` → symmetric with `use_spot()`. Explicit opt-out.

### `SparkConfigContext` additions (6 new methods)

```python
def with_dynamic_allocation(
    self,
    enabled: bool = True,
    min_executors: int = 0,
    max_executors: int | None = None,
) -> "SparkConfigContext":
    """0.80× cost for bursty workloads. Trade-off: scale-up latency."""

def with_max_partition_bytes_mb(self, mb: int = 128) -> "SparkConfigContext":
    """Controls scan parallelism. No cost multiplier (structural setting)."""

def with_io_cache(self, enabled: bool = True) -> "SparkConfigContext":
    """Databricks disk I/O cache. 0.15× for repeated scan workloads.
    Requires cache-optimized nodes (L-series Azure). No effect on write-heavy jobs."""

def with_delta_optimize_write(self, enabled: bool = True) -> "SparkConfigContext":
    """Delta auto-optimize write. 0.75× for read-heavy workflows. Trade-off: write latency."""

def with_delta_auto_compact(self, enabled: bool = True) -> "SparkConfigContext":
    """Delta auto-compaction. Saves on subsequent reads. Extra compute on write."""

def prefer_sort_merge_join(self, prefer: bool = False) -> "SparkConfigContext":
    """prefer=False favors broadcast joins (0.70× for join-heavy on small dims)."""
```

---

## Part 5: Python API Surface

### `estimate()` — unified, expanded signature

```python
burnt.estimate(
    query: str | Path,
    *,
    cluster: ClusterConfig | None = None,
    sku: str = "ALL_PURPOSE",
    currency: Literal["USD", "EUR"] = "USD",
    language: Literal["sql", "python", "auto"] | None = None,  # None = auto
    registry: Any | None = None,
) -> CostEstimate
```

- `query` accepts SQL string, Python source string, or path to `.sql`/`.py`/`.ipynb`/`.dbc`
- Auto-detects path vs string by type (Path object) or file extension (string ending in .sql etc.)
- `estimate_file()` removed — no alias

### `advise()` — unified

```python
burnt.advise(
    run_id: str | None = None,
    statement_id: str | None = None,
    job_id: str | None = None,
    job_name: str | None = None,
) -> AdvisoryReport
# No args → analyze current SparkSession (requires Databricks runtime)
```

- `advise_current_session()` removed — no deprecated alias

### `__init__.py` export changes

Remove from `__all__`:
- `WhatIfModification`, `WhatIfResult`, `MultiScenarioResult`
- `what_if`, `compare`
- `lint`, `lint_file`
- `estimate_file`
- `advise_current_session`
- `get_cluster_json`

Add to `__all__`:
- `SimulationModification`, `SimulationResult`, `MultiSimulationResult`
- `Simulation`
- `CostBudgetExceeded` (implemented in s2-07)

### `CostEstimate` additions

- `simulate() -> Simulation` — entry point, replaces `what_if()`
- `display() -> None` — defined in s2-06; add stub here that raises NotImplementedError
- `raise_if_exceeds(budget_usd, label)` — defined in s2-07; add stub here

### `ClusterRecommendation` addition

- `to_api_json() -> dict` — replaces top-level `get_cluster_json()`. Returns `self.balanced.to_api_json()`.

### `AdvisoryReport.simulate()`

Currently raises `NotImplementedError`. Implement:
```python
def simulate(self) -> Simulation:
    from burnt.core.models import CostEstimate
    from burnt.estimators.simulation import Simulation
    estimate = CostEstimate(
        estimated_dbu=self.baseline.estimated_cost_usd / 0.55,  # approx reverse
        estimated_cost_usd=self.baseline.estimated_cost_usd,
        confidence="low",
    )
    return Simulation(estimate)
```

Note: use `estimated_cost_usd` from baseline; `estimated_dbu` is approximate.

### Remove from Python API entirely

- `burnt.lint()` — CLI only
- `burnt.check()` — CLI only

---

## Acceptance Criteria

### CLI
- [ ] `burnt check <path> --fail-on error --output table` works
- [ ] `burnt check <path> --output text` works
- [ ] `burnt check <path> --ignore-rule cross_join` skips that rule
- [ ] `burnt init` creates `.burnt.toml` or adds `[tool.burnt]` to `pyproject.toml`, updates `.gitignore`, optionally calls tutorial
- [ ] `burnt tutorial` generates `examples/` with 4 notebooks
- [ ] `burnt cache show` and `burnt cache clear` work
- [ ] `burnt rules` TUI toggles rules and persists to active config (yaml or pyproject)
- [ ] `burnt advise --run-id R` works; `burnt advise --self` removed (raises error)
- [ ] `burnt --version` works as flag; `burnt version` (subcommand) removed
- [ ] `burnt estimate` and `burnt whatif` removed — raise "No such command"
- [ ] `burnt lint` removed — raises "No such command"

### Config
- [ ] `.burnt.toml` loaded automatically from CWD upward (no explicit configure() call)
- [ ] `pyproject.toml [tool.burnt]` loaded as fallback when no `.burnt.toml` found
- [ ] `.burnt.toml` takes precedence over `pyproject.toml` when both exist
- [ ] `lint.select` list (default `["ALL"]`) parsed correctly
- [ ] `lint.ignore` list parsed correctly; rules in `ignore` are skipped
- [ ] `lint.per-file-ignores` dict with glob keys parsed correctly
- [ ] `lint.fail-on` threshold controls exit code
- [ ] `Settings.from_toml(path)` works for both `.burnt.toml` and `pyproject.toml`
- [ ] `Settings.discover(cwd)` walks upward and returns (path, settings)
- [ ] `Settings.merge(*settings)` applies correct priority order
- [ ] `ignore_rules` field removed (no migration shim needed — pre-release)

### Module / Class renames
- [ ] `estimators/whatif.py` renamed/replaced by `simulation.py`
- [ ] `WhatIfBuilder` → `Simulation`; `WhatIfResult` → `SimulationResult`; `MultiScenarioResult` → `MultiSimulationResult`; `WhatIfModification` → `SimulationModification`
- [ ] No public backward-compat aliases exist in `__init__.py`
- [ ] `whatif/__init__.py` imports updated to reference `simulation`

### Simulation builder
- [ ] `scenario(name)` pattern works: multi-scenario without lambdas
- [ ] `Simulation.options()` removed
- [ ] `ClusterContext.disable_spot()` added and works
- [ ] All 6 new SparkConfigContext methods present with correct cost multipliers
- [ ] `.scenarios()` absent from all sub-contexts
- [ ] Mixing pre-scenario mods with `scenario()` raises `ValueError`
- [ ] Internal state uses per-scenario dict (not flat attrs)

### Python API
- [ ] `burnt.estimate(sql_string)` works (string)
- [ ] `burnt.estimate(Path("query.sql"))` works (Path)
- [ ] `burnt.estimate("query.sql")` works (string path)
- [ ] `burnt.estimate(sql, sku="JOBS_COMPUTE", currency="EUR")` accepted
- [ ] `burnt.advise()` with no args attempts current session
- [ ] `advise_current_session` not in `burnt.__all__`
- [ ] `estimate_file` not in `burnt.__all__`
- [ ] `what_if`, `compare`, `get_cluster_json`, `lint`, `lint_file` not in `burnt.__all__`
- [ ] `ClusterRecommendation.to_api_json()` returns dict
- [ ] `AdvisoryReport.simulate()` returns a `Simulation` instance (not NotImplementedError)
- [ ] `CostEstimate.simulate()` method present

### Tests
- [ ] All existing unit tests updated to use new class names (`SimulationResult`, etc.)
- [ ] `tests/unit/estimators/test_whatif.py` updated (imports from simulation module)
- [ ] All unit tests pass: `uv run pytest -m unit -v`
- [ ] Lint passes: `uv run ruff check src/ tests/`

---

## Verification

```bash
uv run pytest -m unit -v
uv run ruff check src/ tests/

# CLI smoke tests
burnt --version
burnt check ./src/
burnt check ./src/ --output text
burnt check ./src/ --fail-on warning
burnt init
burnt tutorial
burnt cache show
burnt cache clear

# Confirm removed commands fail
burnt estimate 2>&1 | grep "No such command"
burnt whatif 2>&1 | grep "No such command"
burnt lint 2>&1 | grep "No such command"

# Python API smoke test
python -c "
import burnt
from pathlib import Path
e = burnt.estimate('SELECT 1')
s = e.simulate()
r = s.cluster().enable_photon().compare()
print(r)
"

# Verify exports
python -c "
import burnt
assert 'what_if' not in dir(burnt)
assert 'estimate_file' not in dir(burnt)
assert 'advise_current_session' not in dir(burnt)
assert 'Simulation' in dir(burnt)
print('exports OK')
"
```

---

## Implementation Order

1. Rename `whatif.py` → `simulation.py`; rename all classes (complete or verify partial implementation)
2. Refactor modification tracking → per-scenario dict (`_ScenarioState`)
3. Add `scenario(name)`; remove `options()`; remove `scenarios()` from sub-contexts
4. Add `ClusterContext.disable_spot()`; add 6 new SparkConfigContext methods
5. Update `CostEstimate`: add `simulate()`, stubs for `display()` and `raise_if_exceeds()`
6. Add `ClusterRecommendation.to_api_json()`; implement `AdvisoryReport.simulate()`
7. Merge `advise_current_session()` into `advise()`; update `session.py`
8. Update `core/config.py`: add `lint.rules`, `cache`, `from_yaml()`, `discover()`, `merge()`
9. Update `__init__.py`: remove old exports; add new exports; update `estimate()` signature
10. Update CLI: add `check`/`init`/`tutorial`/`cache`/`rules` stubs; revise `advise` (remove `--self`); remove `estimate`/`whatif`/`lint`/`version` (subcommand); add `--version` flag
11. Implement `burnt init`, `burnt tutorial`, `burnt cache show/clear`, `burnt rules` TUI
12. Update all tests to use new names and pass

---

## Handoff

### Result

```yaml
status: todo
```

### Blocked reason

Not blocked.
