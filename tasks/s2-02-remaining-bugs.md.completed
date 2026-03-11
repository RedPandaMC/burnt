# Task: Fix All Known Bugs + Add Static Analysis / Testing Tooling

---

## Metadata

```yaml
id: s2-02-remaining-bugs
status: todo
sprint: 2
priority: critical
agent: ~
blocked_by: []
created_by: planner
```

---

## Context

### Goal

Fix all known bugs across the codebase. This supersedes the original 4-bug scope — a full
audit (March 2026) uncovered 39 additional bugs on top of the original 4. All are tracked
here with acceptance criteria.

Also add static analysis and testing tooling (mypy/pyright, bandit, hypothesis, mutmut,
beartype) to catch regressions and prevent this class of bug from recurring.

### Files to read

```
# Original 4 bugs
src/burnt/estimators/static.py        # Bug 8 (SKU inference)
src/burnt/forecast/prophet.py         # Bug 9 (empty stub)
src/burnt/cli/main.py                 # Bug 10 (CLI degradation), Bug F, Bug G, Bug H
src/burnt/tables/attribution.py       # Bug 11 (missing file), Bug N, Bug O

# Audit bugs
src/burnt/estimators/whatif.py        # Bug A, B, C, U, V
src/burnt/core/models.py              # Bug D, Bug E, Bug FF
src/burnt/advisor/session.py          # Bug I, J, K, L, T, Bug GG
src/burnt/advisor/report.py           # Bug M, Bug AA, Bug GG
src/burnt/core/instances.py           # Bug P, Bug DD
src/burnt/runtime/spark_backend.py    # Bug Q
src/burnt/runtime/rest_backend.py     # Bug R
src/burnt/tables/compute.py           # Bug S
src/burnt/estimators/static.py        # Bug W
src/burnt/tables/queries.py           # Bug X, Bug II
src/burnt/parsers/explain.py          # Bug BB, Bug CC
src/burnt/core/config.py              # Bug Z
src/burnt/core/pricing.py             # Bug FF
src/burnt/runtime/auto.py             # Bug EE
src/burnt/burnt/__init__.py           # Bug HH

# Reference
files/01-CRITICAL-CODE-FIXES.md
files/00-EXECUTIVE-SUMMARY.md
DESIGN.md
```

---

## Bug Inventory

### Original 4 Bugs (from March 2026 audit)

**Bug 8 — SKU Inference (static.py):**
`_infer_sku()` uses fragile string matching (`if "Standard_D" in cluster.instance_type`),
misclassifying SQL Warehouses, serverless, DLT pipelines, and AWS/GCP instances.

**Bug 9 — Empty forecast/prophet.py Stub:**
File exists but contains no real implementation.

**Bug 10 — No Graceful CLI Degradation:**
`burnt estimate` crashes with `ImportError` if sqlglot isn't installed instead of
suggesting `uv sync --extra sql`.

**Bug 11 — Missing tables/attribution.py:**
Referenced in DESIGN.md but doesn't exist. Required for calibration, historical cost
lookups, and ML training data.

---

### Audit Bugs (full codebase review)

**Bug A — Dead code after `return` in `whatif.py`**
`src/burnt/estimators/whatif.py` lines 75–93 | **High**
Lines 75–93 are unreachable after the `return` on line 73. The dead block uses a
different (arguably more correct) `savings_pct` denominator than the live one.
Fix: delete lines 75–93 or consolidate into one `return` with the correct formula.

**Bug B — `_apply_modifications()` mutates `self._modifications` on every call**
`src/burnt/estimators/whatif.py` lines 303–494, 239–257 | **High**
Appends to `self._modifications` each call; calling `compare()` twice double-counts
modifications. `_compare_multiple()` copies top-level mods then appends again.
Fix: collect modifications in a local list; don't mutate `self._modifications`.

**Bug C — `DataSourceBuilder`/`SparkConfigBuilder` mutate `_original_estimate`**
`src/burnt/estimators/whatif.py` lines 694–704, 794–804 | **High**
`_apply_datasource_multiplier()` and `_apply_spark_multiplier()` overwrite
`self._parent._original_estimate` eagerly, making `total_savings_pct` always 0.
Fix: apply multipliers at comparison time inside `_apply_modifications()`.

**Bug D — `ClusterRecommendation.comparison_table()` fabricates costs**
`src/burnt/core/models.py` lines 120–122 | **High**
Costs are `dbu_per_hour * 1.0/1.5/2.0` — magic constants with no meaningful unit,
displayed as if they were USD.
Fix: use actual `current_cost_usd` values or remove the cost column.

**Bug E — `CostEstimate._cluster` is not a Pydantic private attribute**
`src/burnt/core/models.py` line 96 | **High**
Leading underscore without `PrivateAttr()` means Pydantic v2 doesn't store it
per-instance; `self._cluster` is always `None`, so `what_if()` always passes `None`
as the cluster to `WhatIfBuilder`.
Fix: `_cluster: ClusterConfig | None = PrivateAttr(default=None)`.

**Bug F — `settings.workspace_url` mutation ignored by pipeline**
`src/burnt/cli/main.py` lines 67–68 | **Medium**
Mutated `settings` is not passed to `EstimationPipeline`; `--workspace-url` flag has
no effect. Silent fallback to offline mode with no error when token is absent.
Fix: `Settings(workspace_url=workspace_url)` in the constructor.

**Bug G — Currency conversion discards pipeline result and re-estimates**
`src/burnt/cli/main.py` lines 107–112 | **High**
When `currency != "USD"`, the superior pipeline result is thrown away and a fresh
static estimate is run. `estimated_cost_usd` is also always recalculated from DBUs,
ignoring `result.estimated_cost_usd`.
Fix: convert the already-computed `result.estimated_cost_usd`; never re-estimate.

**Bug H — `DatabricksClient` passed as `Backend` but doesn't implement the protocol**
`src/burnt/cli/main.py` lines 78–81 | **High**
Missing `get_cluster_config`, `get_recent_queries`, `describe_table`,
`get_session_metrics`. Any tier calling those methods raises `AttributeError`.
Fix: use `create_pipeline()` factory / `auto_backend()` instead.

**Bug I — `execute_sql()` called without `warehouse_id` on REST backend**
`src/burnt/advisor/session.py` lines 442, 483, 522 | **High**
`RestBackend.execute_sql()` raises `ValueError` when `warehouse_id` is `None`.
Fix: accept and thread a `warehouse_id` parameter through the three helper functions.

**Bug J — Dead code: second `if backend is None` check in `advise()`**
`src/burnt/advisor/session.py` lines 124–125 | **Low**
`backend` is guaranteed non-None after line 111; the second check is unreachable.
Fix: remove lines 124–125.

**Bug K — `_fetch_metrics_from_job()` reads non-existent `duration_ms` column**
`src/burnt/advisor/session.py` line 539 | **Medium**
The SQL query doesn't SELECT `duration_ms`; `row.get("duration_ms", 0)` always
returns 0, making `avg_duration_ms` and `duration_variability_pct` always 0.
Fix: add `TIMESTAMPDIFF(MILLISECOND, start_time, end_time) AS duration_ms` to SELECT.

**Bug L — Division by zero in `_project_scenarios()` when `baseline_cost` is 0**
`src/burnt/advisor/session.py` lines 238–239, 273–274, 280–281 | **High**
`savings_pct = ((baseline_cost - cost) / baseline_cost) * 100` raises
`ZeroDivisionError` when `baseline_cost` is 0 (caused by Bug K).
Fix: guard with `if baseline_cost > 0 else 0.0`.

**Bug M — `AdvisoryReport.comparison_table()` inverted savings colour logic**
`src/burnt/advisor/report.py` lines 153–157 | **Medium**
`"green" if scenario.savings_pct < 0` is backwards — negative means more expensive.
Fix: `"green" if scenario.savings_pct > 0 else "red" if scenario.savings_pct < 0 else "black"`.

**Bug N — `_parse_datetime()` strips timezone when truncating microseconds**
`src/burnt/tables/attribution.py` lines 295–299 | **Medium**
`rest[:6]` strips timezone offset embedded after fractional seconds (e.g.
`"123456+00:00"` → `"123456"`), producing naive datetimes that raise `TypeError`
when compared with timezone-aware datetimes.
Fix: `re.sub(r'(\.\d{6})\d*', r'\1', dt_str)`.

**Bug O — `attribute_costs_to_queries()` uses 50-char prefix as fingerprint**
`src/burnt/tables/attribution.py` line 84 | **High**
`query.statement_text[:50]` groups different queries with the same prefix and splits
identical queries with different literals.
Fix: use `fingerprint_sql(query.statement_text)` from `tables/queries.py`.

**Bug P — `fetch_azure_pricing()` computes memory as `vCPUs * 4` (wrong)**
`src/burnt/core/instances.py` lines 386–388 | **Medium**
Incorrect for memory-optimised families (e.g. E8s_v3: 8 vCPUs, 64 GB, not 32 GB).
Fix: query Azure Compute SKUs API for actual memory, or hardcode from
`AZURE_INSTANCE_CATALOG`.

**Bug Q — `SparkBackend.get_session_metrics()` calls non-existent `getJobStatus()` API**
`src/burnt/runtime/spark_backend.py` lines 183–196 | **High**
`status_tracker.getJobStatus()` does not exist in PySpark; `getJobInfo(jobId)`
requires a specific job ID. Also uses unstable `sc._jsc.getExecutorMemoryStatus()`.
Fix: use `getActiveJobIds()` / `getActiveStageIds()`; replace Java internal call.

**Bug R — `RestBackend.execute_sql()` ignores statement execution failure state**
`src/burnt/runtime/rest_backend.py` lines 67–75 | **High**
No check of `response.status.state`; failed statements silently return `[]`.
Fix: raise `RuntimeError` if state is not `SUCCEEDED`.

**Bug S — SQL injection via `start_time`/`end_time` in `get_node_timeline()`**
`src/burnt/tables/compute.py` lines 76–84 | **High**
`cluster_id` is sanitised but `start_time` and `end_time` are interpolated raw.
Fix: validate both against a datetime format pattern before interpolation.

**Bug T — SQL injection via `job_name` and `run_id` in `session.py`**
`src/burnt/advisor/session.py` lines 475–479, 438 | **High**
User-supplied `job_name` and `run_id` interpolated directly into SQL.
Fix: apply `_sanitize_id()` or escape single quotes.

**Bug U — DataSource/SparkConfig mods not propagated to baseline in multi-scenario compare**
`src/burnt/estimators/whatif.py` lines 264, 286–295 | **Medium**
`_top_level_mods_applied` not set by `DataSourceBuilder`/`SparkConfigBuilder`, so
baseline builder omits those modifications, producing an inconsistent comparison.
Fix: set `self._parent._top_level_mods_applied = True` in all affected methods.

**Bug V — `WhatIfBuilder.scenarios()` type hint says `None` return but design expects chaining**
`src/burnt/estimators/whatif.py` lines 189, 204–209 | **Low**
`Callable[[WhatIfBuilder], None]` discards return values; complex scenario lambdas
break silently in combination with Bugs B and C.
Fix: clarify design intent and fix Bugs B and C first.

**Bug W — Static estimator confidence logic is inverted**
`src/burnt/estimators/static.py` lines 77–86 | **Medium**
Simple queries → `"high"` confidence; complex queries → `"medium"`. Should be the
reverse. `profile is None` returns `"medium"` instead of `"low"`.
Fix: complex → `"low"`, moderate → `"medium"`, simple → `"high"`, no profile → `"low"`.

**Bug X — `normalize_sql()` numeric regex corrupts identifiers containing numbers**
`src/burnt/tables/queries.py` line 53 | **Medium**
`re.sub(r"\b\d+(\.\d+)?\b", "?", sql)` mutates identifiers like `table_v2` →
`table_v?`, corrupting fingerprints.
Fix: use a SQL-aware tokenizer or restrict the pattern to avoid word-boundary matches
inside identifiers.

**Bug Y — `forecast/prophet.py` does not validate required DataFrame columns**
`src/burnt/forecast/prophet.py` line 21 | **Low**
Missing `"usage_date"` or `"total_cost"` columns causes a confusing internal Prophet
error rather than a clear `ValueError`.
Fix: validate required columns before calling `rename()`.

**Bug Z — `config.py` `from_toml()` has dead `tomli` fallback**
`src/burnt/core/config.py` lines 26–39 | **Low**
`tomllib` is always available on Python 3.12+; the `tomli` fallback is dead code and
`tomli` is not listed as a dependency.
Fix: remove `tomli` path; use `import tomllib` directly.

**Bug AA — `report.py` double import of the same symbols**
`src/burnt/advisor/report.py` lines 10–12 | **Low**
`ClusterConfig` and `ClusterRecommendation` appear both in `if TYPE_CHECKING:` and
as an unconditional runtime import. The `TYPE_CHECKING` block is redundant.
Fix: remove the `if TYPE_CHECKING:` block.

**Bug BB — `_STATS_PATTERN` with `re.IGNORECASE` breaks size unit lookup**
`src/burnt/parsers/explain.py` lines 43–47 | **Medium**
`re.IGNORECASE` causes the regex to capture `"Mib"` instead of `"MiB"`;
`_SIZE_MULTIPLIERS.get("Mib", 1)` returns 1, underestimating sizes by ~10⁶×.
Fix: remove `re.IGNORECASE` or normalise the captured unit before lookup.

**Bug CC — `_size_to_bytes()` silently treats unknown units as bytes**
`src/burnt/parsers/explain.py` lines 87–90 | **Low**
`_SIZE_MULTIPLIERS.get(unit, 1)` defaults to 1 for unknown units with no warning.
Fix: log a warning or raise for unknown units.

**Bug DD — `get_fresh_pricing()` returns empty dict as "embedded fallback"**
`src/burnt/core/instances.py` lines 421–436 | **Medium**
Claims to fall back to embedded pricing but returns `{}` in both the embedded-source
case and on fetch error.
Fix: return data from `AZURE_INSTANCE_CATALOG`.

**Bug EE — `_get_script_path()` returns internal `burnt` module path, not user's script**
`src/burnt/runtime/auto.py` lines 141–149 | **Medium**
`inspect.stack()` starts from innermost frame; the first `.py` found is always a
`burnt` internal file.
Fix: skip frames whose filenames are inside the `burnt` package directory.

**Bug FF — `VALID_SKUS` and `AZURE_DBU_RATES` keys are completely disjoint**
`src/burnt/core/models.py` line 29 vs `src/burnt/core/pricing.py` lines 7–18 | **High**
`VALID_SKUS` = `{"SQL_ENDPOINT", "DLT", "SERVERLESS", ...}`;
`AZURE_DBU_RATES` = `{"SQL_CLASSIC", "DLT_CORE", "SERVERLESS_JOBS", ...}`.
Any `ClusterConfig` with a valid SKU raises `PricingError` at cost computation time.
Fix: align `VALID_SKUS` with `AZURE_DBU_RATES.keys()`.

**Bug GG — `ComputeScenario` uses invalid SKU `"SERVERLESS"`**
`src/burnt/advisor/session.py` line 271, `src/burnt/advisor/report.py` line 19 | **Medium**
`"SERVERLESS"` is not in `AZURE_DBU_RATES`; `get_dbu_rate("SERVERLESS")` raises
`PricingError`.
Fix: use `"SERVERLESS_JOBS"` or `"SERVERLESS_NOTEBOOKS"`; add validation to
`ComputeScenario`.

**Bug HH — Default `dbu_per_hour=1.5` for `Standard_DS3_v2` is 2× too high**
`src/burnt/burnt/__init__.py` lines 76–79 | **High**
Correct rate is `0.75`; all calls to `burnt.estimate()` without an explicit cluster
overestimate DBU costs by 2×.
Fix: `dbu_per_hour=0.75` or `AZURE_INSTANCE_DBU["Standard_DS3_v2"]`.

**Bug II — `get_query_history()` does not validate `days` as a positive integer**
`src/burnt/tables/queries.py` lines 72–81 | **Low**
`days` is interpolated directly into SQL; a non-integer value produces invalid SQL.
Fix: `if not isinstance(days, int) or days <= 0: raise ValueError(...)`.

**Bug JJ — `whatif.py` `Spec` object has no `dbu_rate` attribute**
`src/burnt/estimators/whatif.py` line 347 | **High**
`Spec.dbu_rate` does not exist; accessing it raises `AttributeError` at runtime.
Fix: use the correct attribute name from the `Spec` model.

**Bug KK — `DataSourceBuilder.compare()` / `SparkConfigBuilder.compare()` return type mismatch**
`src/burnt/estimators/whatif.py` lines 573, 716, 816 | **Medium**
Return type is declared as `WhatIfResult` but the function can return
`WhatIfResult | MultiScenarioResult`; callers expecting `WhatIfResult` will fail.
Fix: widen the return type annotation to `WhatIfResult | MultiScenarioResult`.

**Bug LL — `_lookup_job_id_by_name()` return type declared `str` but can return `None`**
`src/burnt/advisor/session.py` line 497 | **Medium**
Return type annotation is `str` but the function can return `None` when no job is
found. Callers that don't guard against `None` will get `AttributeError`.
Fix: change return type to `str | None` and update all call sites.

---

## Acceptance Criteria

### Original Bug 8: Fix SKU Inference (estimators/static.py)
- [ ] Make SKU an explicit parameter in `ClusterConfig`
- [ ] Remove fragile string matching on `Standard_D` prefixes
- [ ] Add validation for valid SKU values (aligned with Bug FF fix)
- [ ] Update CLI to accept `--sku` flag

### Original Bug 9: Fix Empty forecast/prophet.py Stub
- [ ] Either implement Prophet forecasting or mark clearly as TODO/stub with docs
- [ ] Add column validation if implemented (Bug Y fix included)

### Original Bug 10: Add Graceful CLI Degradation
- [ ] Add try/except around sqlglot import in CLI
- [ ] Show friendly error: "sqlglot required: uv sync --extra sql"

### Original Bug 11: Create tables/attribution.py
- [ ] Implement billing × list_prices join
- [ ] Per-query attribution via warehouse_id + time overlap
- [ ] Lakeflow job-run cost attribution
- [ ] `get_historical_cost(fingerprint)` function for Tier 4 pipeline
- [ ] Use `fingerprint_sql()` not 50-char prefix (Bug O fix included)
- [ ] Fix `_parse_datetime()` timezone stripping (Bug N fix included)
- [ ] Wire into EstimationPipeline

### Audit Bugs: whatif.py (A, B, C, U, V)
- [ ] Remove dead code after `return` on line 73 (Bug A)
- [ ] `_apply_modifications()` uses local list, does not mutate `self._modifications` (Bug B)
- [ ] Multipliers applied at comparison time, not eagerly on `_original_estimate` (Bug C)
- [ ] `_top_level_mods_applied` set by DataSource/SparkConfig methods (Bug U)
- [ ] Scenario callable type hint corrected (Bug V)

### Audit Bugs: core/models.py (D, E, FF)
- [ ] `comparison_table()` uses real cost values, not fabricated multipliers (Bug D)
- [ ] `_cluster` declared with `PrivateAttr()` (Bug E)
- [ ] `VALID_SKUS` aligned with `AZURE_DBU_RATES.keys()` (Bug FF)

### Audit Bugs: cli/main.py (F, G, H)
- [ ] `workspace_url` passed to `Settings()` constructor (Bug F)
- [ ] Currency conversion does not re-estimate; converts existing `estimated_cost_usd` (Bug G)
- [ ] Pipeline uses `create_pipeline()` / `auto_backend()` not raw `DatabricksClient` (Bug H)

### Audit Bugs: advisor/session.py (I, J, K, L, T, GG)
- [ ] `warehouse_id` threaded through `_fetch_metrics_from_history`, `_lookup_job_id_by_name`, `_fetch_metrics_from_job` (Bug I)
- [ ] Redundant `if backend is None` removed (Bug J)
- [ ] `duration_ms` computed from `end_time - start_time` in SQL (Bug K)
- [ ] `_project_scenarios()` guards against zero `baseline_cost` (Bug L)
- [ ] `job_name` and `run_id` sanitised before SQL interpolation (Bug T)
- [ ] `ComputeScenario` uses valid SKU key (Bug GG)

### Audit Bugs: advisor/report.py (M, AA, GG)
- [ ] Savings colour logic corrected (green = cheaper, red = more expensive) (Bug M)
- [ ] Redundant `TYPE_CHECKING` import block removed (Bug AA)

### Audit Bugs: core/instances.py (P, DD)
- [ ] Memory not computed as `vCPUs * 4`; use catalog or correct API (Bug P)
- [ ] `get_fresh_pricing()` returns `AZURE_INSTANCE_CATALOG` data as fallback (Bug DD)

### Audit Bugs: runtime/ (Q, R, EE)
- [ ] `SparkBackend.get_session_metrics()` uses valid PySpark API (Bug Q)
- [ ] `RestBackend.execute_sql()` raises on non-SUCCEEDED state (Bug R)
- [ ] `_get_script_path()` skips burnt internal frames (Bug EE)

### Audit Bugs: tables/ (S, X, II)
- [ ] `start_time`/`end_time` validated before SQL interpolation in `compute.py` (Bug S)
- [ ] `normalize_sql()` numeric regex does not corrupt identifiers (Bug X)
- [ ] `get_query_history()` validates `days` is a positive integer (Bug II)

### Audit Bugs: estimators/static.py (W)
- [ ] Confidence logic corrected: complex → low, moderate → medium, simple → high (Bug W)

### Audit Bugs: parsers/explain.py (BB, CC)
- [ ] `_STATS_PATTERN` does not use `re.IGNORECASE`, or unit is normalised before lookup (Bug BB)
- [ ] Unknown size units log a warning instead of silently returning 1 (Bug CC)

### Audit Bugs: core/ (Z, HH)
- [ ] `config.py` uses `import tomllib` directly; `tomli` fallback removed (Bug Z)
- [ ] Default `dbu_per_hour` for `Standard_DS3_v2` corrected to `0.75` (Bug HH)

### Audit Bugs: forecast/prophet.py (Y)
- [ ] Required DataFrame columns validated before `rename()` (Bug Y)

### Audit Bugs: whatif.py type errors (JJ, KK)
- [ ] `Spec.dbu_rate` replaced with the correct attribute name (Bug JJ)
- [ ] `compare()` return type widened to `WhatIfResult | MultiScenarioResult` (Bug KK)

### Audit Bugs: advisor/session.py return type (LL)
- [ ] `_lookup_job_id_by_name()` return type corrected to `str | None` (Bug LL)

### Testing Tooling
- [ ] **mypy** or **pyright** added to `pyproject.toml` dev dependencies and CI
  - Catches: protocol non-conformance (H), missing PrivateAttr (E), type mismatches (G, V, FF)
  - Run: `uv run mypy src/` or `uv run pyright src/`
- [ ] **bandit** added to dev dependencies and CI
  - Catches: SQL injection (S, T, II, X)
  - Run: `uv run bandit -r src/`
- [ ] **hypothesis** added to dev dependencies; property-based tests written for:
  - `_project_scenarios()` with `baseline_cost=0` (Bug L)
  - `_parse_datetime()` with timezone-embedded fractional seconds (Bug N)
  - `normalize_sql()` with identifiers containing digits (Bug X)
- [ ] **mutmut** added to dev dependencies; mutation score baseline established
  - Catches: inverted logic (M, W), off-by-one, flipped conditions
  - Run: `uv run mutmut run`
- [ ] **beartype** or **typeguard** added for runtime type enforcement in tests
  - Catches: `days` as non-integer (II), invalid SKU strings (FF, GG)

### General Requirements
- [ ] All public functions have type hints and docstrings
- [ ] `uv run pytest -m unit -v` passes
- [ ] `uv run ruff check src/ tests/` zero errors
- [ ] `uv run ruff format --check src/ tests/` passes
- [ ] `uv run mypy src/` (or pyright) passes with zero errors
- [ ] `uv run bandit -r src/` passes with zero high-severity findings

---

## Verification

### Commands

```bash
uv run pytest -m unit -v
uv run ruff check src/ tests/
uv run ruff format --check src/ tests/
uv run mypy src/
uv run bandit -r src/

# Test graceful degradation (uninstall sqlglot temporarily)
uv run burnt estimate "SELECT 1"

# Spot-check critical fixes
uv run python -c "
from burnt.core.models import CostEstimate, ClusterConfig
c = ClusterConfig(instance_type='Standard_DS3_v2', num_workers=2, sku='ALL_PURPOSE')
print('dbu_per_hour:', c.dbu_per_hour)  # should not be 1.5
"
```

### Expected output

- All tests pass
- mypy/pyright: zero errors
- bandit: zero high-severity findings
- CLI shows helpful message when sqlglot unavailable
- Default DS3_v2 cluster uses `dbu_per_hour=0.75`

---

## Handoff

### Result

[Executor fills this in when done.]

```
status: todo
```

### Blocked reason

[If blocked, explain exactly what is missing.]
