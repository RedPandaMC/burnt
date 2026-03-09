# Bug Tracking - burnt

> Status of critical bugs identified in March 2026 audit. Reference: `files/00-EXECUTIVE-SUMMARY.md` and `files/01-CRITICAL-CODE-FIXES.md`

---

## Bug Status Summary

| Bug | Status | Fixed In | Description | Impact |
|-----|--------|----------|-------------|---------|
| 1 | ✅ **Fixed** | `p4a-01-critical-bug-fixes` | Quadratic formula in static estimator (960× overestimate) | All static estimates wrong |
| 2 | ✅ **Fixed** | `p4a-01-critical-bug-fixes` | Phantom $0.20/DBU price (no real SKU matches) | Hybrid estimates use wrong price |
| 3 | ✅ **Fixed** | `p4a-01-critical-bug-fixes` | EXPLAIN DBU constants 7,900× too high | EXPLAIN-based estimates wildly off |
| 4 | ✅ **Fixed** | `p4a-01-critical-bug-fixes` | Historical estimation ignores data volume scaling | Recurring query estimates wrong when data grows |
| 5 | ✅ **Fixed** | `p4a-01-critical-bug-fixes` | SQL injection in system table queries | Security vulnerability |
| 6 | ✅ **Fixed** | `p4a-01-critical-bug-fixes` | Anti-pattern detector uses string matching | False positives/negatives |
| 7 | ✅ **Fixed** | `p4a-01-critical-bug-fixes` | protocols.py shadows core models | Type system confusion |
| 8 | 🔴 **Pending** | `p4a-01b-remaining-bugs` | SKU inference misclassifies compute | Wrong pricing tier used |
| 9 | 🔴 **Pending** | `p4a-01b-remaining-bugs` | forecast/prophet.py empty stub | Missing functionality advertised |
| 10 | 🔴 **Pending** | `p4a-01b-remaining-bugs` | No graceful CLI degradation | Crashes on missing sqlglot |
| 11 | 🔴 **Pending** | `p4a-01b-remaining-bugs` | Missing tables/attribution.py | Calibration & ML training impossible |

---

## Fixed Bugs (Phase 4A - Completed)

### Bug 1: Static Estimator Formula Is Quadratic
**File:** `src/burnt/estimators/static.py`  
**Issue:** `complexity² × cluster_factor / 100` → 960× overestimate for simple GROUP BY.  
**Fix:** Linear throughput model: `(scan_bytes / throughput_bps + shuffle_count × shuffle_overhead) / 3600 × cluster_dbu_per_hour`  
**Task:** `p4a-01-critical-bug-fixes.md.completed`

### Bug 2: Hybrid Estimator Uses Phantom Price
**File:** `src/burnt/estimators/hybrid.py`  
**Issue:** `_NOMINAL_USD_PER_DBU = 0.20` matches no real SKU (ALL_PURPOSE=$0.55, JOBS=$0.30, SQL_SERVERLESS=$0.70).  
**Fix:** Remove constant. Call `get_dbu_rate(sku)` from `core/pricing.py`.  
**Task:** `p4a-01-critical-bug-fixes.md.completed`

### Bug 3: EXPLAIN DBU Constants Are Ungrounded
**File:** `src/burnt/estimators/hybrid.py`  
**Issue:** `_SCAN_DBU_PER_GB = 0.5` is ~7,900× too high. DS3_v2 scans Parquet at ~3.2 GB/s → 1 GB takes ~0.3 s = 0.000063 DBU.  
**Fix:** `_SCAN_DBU_PER_GB = 0.00013` (interim constant).  
**Task:** `p4a-01-critical-bug-fixes.md.completed`

### Bug 4: Historical Estimation Ignores Data Volume Scaling
**File:** `src/burnt/estimators/hybrid.py`  
**Issue:** p50 duration from history is not scaled when current table is larger than historical runs.  
**Fix:** `adjusted_ms = p50_ms × (current_read_bytes / median_historical_read_bytes)`  
**Task:** `p4a-01-critical-bug-fixes.md.completed`

### Bug 5: SQL Injection in System Table Queries
**Files:** `src/burnt/tables/billing.py`, `queries.py`, `compute.py`  
**Issue:** All queries use f-string interpolation.  
**Fix:** Add `_sanitize_id()` validation function.  
**Task:** `p4a-01-critical-bug-fixes.md.completed`

### Bug 6: Anti-Pattern Detector Uses String Matching
**File:** `src/burnt/parsers/antipatterns.py`  
**Issue:** `"CROSS JOIN" in sql.upper()` matches inside comments, string literals.  
**Fix:** Use sqlglot AST (already in `sql.py`).  
**Task:** `p4a-01-critical-bug-fixes.md.completed`

### Bug 7: protocols.py Shadows Core Models
**File:** `src/burnt/core/protocols.py`  
**Issue:** `CostEstimate` and `ParseResult` shadow the real `CostEstimate` in `models.py`.  
**Fix:** Remove placeholder classes, import from `core.models`.  
**Task:** `p4a-01-critical-bug-fixes.md.completed`

---

## Pending Bugs (Phase 4A - To Be Fixed)

### Bug 8: SKU Inference Misclassifies Compute
**File:** `src/burnt/estimators/static.py`  
**Issue:** String-matching on instance type misclassifies SQL Warehouses, serverless, DLT.  
**Impact:** Wrong pricing tier applied to estimates.  
**Fix Required:** Make SKU explicit parameter, remove fragile inference.  
**Task:** `p4a-01b-remaining-bugs.md` (status: todo)

### Bug 9: forecast/prophet.py Is Empty Stub
**File:** `src/burnt/forecast/prophet.py`  
**Issue:** File exists but contains no real implementation.  
**Impact:** Missing functionality advertised in package.  
**Fix Required:** Implement or mark as TODO with clear documentation.  
**Task:** `p4a-01b-remaining-bugs.md` (status: todo)

### Bug 10: No Graceful Degradation in CLI
**File:** `src/burnt/cli/main.py`  
**Issue:** If sqlglot isn't installed, `burnt estimate "SELECT ..."` crashes with ImportError.  
**Impact:** Poor user experience, no helpful guidance.  
**Fix Required:** Add try/except with helpful installation suggestion.  
**Task:** `p4a-01b-remaining-bugs.md` (status: todo)

### Bug 11: Missing tables/attribution.py
**File:** `src/burnt/tables/attribution.py` (doesn't exist)  
**Issue:** Referenced in DESIGN.md but not implemented. Required for calibration.  
**Impact:** Historical cost lookups and ML training impossible.  
**Fix Required:** Create file with billing × list_prices join logic.  
**Task:** `p4a-01b-remaining-bugs.md` (status: todo)

---

## Architecture Gaps (Separate from Bugs)

These are design/implementation gaps rather than bugs:

1. **No Estimation Pipeline Orchestrator** - Missing `EstimationPipeline` class to connect all estimation tiers
2. **No Databricks Runtime Support** - Missing `RuntimeBackend` for in-cluster execution  
3. **DBU-Only Estimates Miss VM Costs** - Classic compute requires VM infrastructure cost addition
4. **No Top-Level Python API** - `import burnt` doesn't expose public functions
5. **Missing TableRegistry** - Enterprise support for governance view environments

> These gaps are tracked in DESIGN.md §"Implementation Roadmap" and respective task files.

---

## Verification

After fixing all bugs, verify with:

```bash
# Unit tests
uv run pytest -m unit -v

# Lint
uv run ruff check src/ tests/
uv run ruff format --check src/ tests/

# Type safety
uv run mypy src/

# Security
uv run bandit -c pyproject.toml -r src/
```

**Accuracy targets:**
- Phase 4: All estimates within **10×** of actual
- Phase 5: Within **3×** of actual  
- Phase 6 (ML): Within **2×** of actual

---

*Last updated: March 2026 | Based on audit: `files/00-EXECUTIVE-SUMMARY.md`*