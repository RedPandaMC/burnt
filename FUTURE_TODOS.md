# dburnrate Implementation Roadmap

> Critical review of MVP implementation and future work required

---

## Critical Review of Current Implementation

### Strengths
- Project structure follows PLAN.md architecture
- SQL parsing with sqlglot works for basic operations (MERGE, JOIN, GROUP BY, WINDOW, etc.)
- PySpark AST analysis for method detection
- CLI with rich output and what-if scenarios (Photon, serverless, resize)
- pyproject.toml has proper lint/coverage/tooling config

### Critical Gaps

1. **Estimator formula is unvalidated** - `complexity * cluster_factor * 0.01` has no empirical grounding
2. **No system tables integration** - Research (RESEARCH.md) shows this is essential: `system.billing.usage`, `system.query.history`
3. **No EXPLAIN COST integration** - Research shows this is the strongest cold-start signal
4. **No Delta metadata** - Research shows `_delta_log` provides exact scan sizes
5. **No query fingerprinting** - Research shows historical matching is highest-ROI approach
6. **Azure-only** - No AWS/GCP pricing
7. **Limited operations detected** - Missing: `COPY INTO`, `OPTIMIZE`, `ZORDER`, streaming tables
8. **No real confidence calibration** - Confidence levels are arbitrary guesses
9. **Tests need verification** - Haven't been run to confirm they pass

---

## Phase 1: Foundation & Validation (Critical)

### 1.1 Run and Fix Tests ✅
- [x] Run existing tests: `uv run pytest -m unit -v` — was selecting 0 (fixed)
- [x] Run linting: `uv run ruff check src/ tests/` — was 29 errors (fixed)
- [x] Run formatting check: `uv run ruff format --check src/ tests/` — clean
- [x] Fix all linting/type errors — 0 errors remaining
- [ ] Add return type hints to all functions (especially parsers/sql.py)
- [ ] Add missing docstrings to reach interrogate coverage targets
- [ ] Run security audit: `uv run bandit -c pyproject.toml -r src/`

### 1.2 Code Quality
- [ ] Add type hints to all public functions
- [ ] Ensure all exceptions are properly caught and raised
- [ ] Add input validation for CLI arguments

---

## Phase 2: System Tables Integration (High Priority)

> Research (RESEARCH.md) shows this is essential for accurate estimation

### 2.1 Billing Integration
- [ ] Implement `src/dburnrate/tables/billing.py`
- [ ] Query `system.billing.usage` for historical DBU consumption
- [ ] Query `system.billing.list_prices` for live pricing
- [ ] Implement cost attribution: `query_duration / total_duration * hourly_cost`

### 2.2 Query History Integration
- [ ] Implement `src/dburnrate/tables/queries.py`
- [ ] Query `system.query.history` for execution metrics
- [ ] Extract: `execution_duration_ms`, `read_bytes`, `read_rows`, `total_task_duration_ms`

### 2.3 Compute Integration
- [ ] Implement `src/dburnrate/tables/compute.py`
- [ ] Query `system.compute.node_types` for DBU/instance mappings
- [ ] Query `system.compute.clusters` for cluster configurations
- [ ] Query `system.compute.node_timeline` for utilization metrics

### 2.4 Databricks Connection
- [ ] Add Databricks REST API client
- [ ] Implement workspace URL + token authentication
- [ ] Add connection pooling and retry logic
- [ ] Add rate limiting for API calls

---

## Phase 3: EXPLAIN COST Integration (High Priority)

> Research shows this is the strongest cold-start signal

### 3.1 EXPLAIN Parsing
- [ ] Implement `src/dburnrate/parsers/explain.py`
- [ ] Parse `EXPLAIN COST` output for `sizeInBytes` and `rowCount`
- [ ] Extract join strategies (BroadcastHashJoin vs SortMergeJoin)
- [ ] Count shuffle operations and operator depth

### 3.2 Statistics Handling
- [ ] Detect statistics completeness (missing/partial/full)
- [ ] Handle tables without ANALYZE gracefully
- [ ] Integrate with Delta metadata for validation

### 3.3 Hybrid Estimation
- [ ] Combine static analysis with EXPLAIN data
- [ ] Weight EXPLAIN estimates higher when statistics available
- [ ] Add confidence boost when both signals agree

---

## Phase 4: Delta Metadata Integration (Medium Priority)

> Provides exact scan sizes without data scanning

### 4.1 Delta Log Parsing
- [ ] Implement `_delta_log` directory parsing
- [ ] Extract per-file statistics: `numRecords`, `size`, `minValues`, `maxValues`, `nullCount`
- [ ] Add `DESCRIBE DETAIL` wrapper

### 4.2 Scan Size Estimation
- [ ] Compute total table size from transaction log
- [ ] Estimate partition-filtered scan sizes
- [ ] Add data volume factor to cost estimation

### 4.3 Data Skipping
- [ ] Use file-level min/max for predicate filter estimation
- [ ] Account for Z-ORDER/Liquid Clustering effectiveness

---

## Phase 5: Historical Fingerprinting (High Priority)

> Research shows this is the highest-ROI approach for recurring workloads

### 5.1 Query Normalization
- [ ] Implement Percona-style normalization (strip comments, normalize whitespace)
- [ ] Replace literals with `?` placeholders
- [ ] Collapse IN-lists to single placeholders
- [ ] Abstract database/schema names

### 5.2 Template Matching
- [ ] Add SHA-256 template hashing
- [ ] Implement exact match → historical p50/p95 lookup
- [ ] Cache template → cost mappings

### 5.3 Similarity Matching
- [ ] Add AST edit distance for near-matches (using sqlglot)
- [ ] Consider embedding-based similarity (CodeBERT) for cold queries

---

## Phase 6: ML Cost Models (Medium Priority)

> Research shows 14-98% accuracy depending on approach

### 6.1 Feature Extraction
- [ ] Extract operator types from plan
- [ ] Extract estimated cardinalities from EXPLAIN
- [ ] Extract table sizes from Delta metadata
- [ ] Extract cluster configuration

### 6.2 Classification Model
- [ ] Implement cost bucket classifier: low/medium/high/very-high
- [ ] Use sklearn for simple model
- [ ] Add training data from historical executions

### 6.3 Advanced Models
- [ ] Consider RAAL/DRAL-inspired resource-aware models
- [ ] Add zero-shot transfer capability

---

## Phase 7: Multi-Cloud Support (Medium Priority)

### 7.1 AWS Support
- [ ] Add AWS Databricks DBU rates
- [ ] Add AWS instance types
- [ ] Add Photon multiplier for AWS (2.9x)

### 7.2 GCP Support
- [ ] Add GCP Databricks DBU rates
- [ ] Add GCP instance types

### 7.3 Refactoring
- [ ] Refactor pricing.py to support cloud selection
- [ ] Add cloud detection from workspace URL

---

## Phase 8: Enhanced Operations Detection (Medium Priority)

### 8.1 SQL Operations
- [ ] Add COPY INTO detection
- [ ] Add OPTIMIZE/ZORDER detection (parse as Command)
- [ ] Add streaming table detection (`CREATE STREAMING TABLE`, `@dp.materialized_view`)
- [ ] Add Liquid Clustering detection (`CLUSTER BY`)

### 8.2 Unity Catalog
- [ ] Add catalog/schema awareness
- [ ] Handle 3-level naming (catalog.schema.table)
- [ ] Add metastore integration

### 8.3 Anti-patterns
- [ ] Expand anti-pattern detection from CONCEPT.md
- [ ] Add severity levels and suggestions

---

## Phase 9: Production Hardening (High Priority)

### 9.1 Error Handling
- [ ] Add comprehensive exception handling
- [ ] Add user-friendly error messages
- [ ] Add error recovery strategies

### 9.2 Performance
- [ ] Add caching for metadata lookups
- [ ] Add connection pooling
- [ ] Implement batch queries for efficiency

### 9.3 Observability
- [ ] Add structured logging
- [ ] Add metrics collection
- [ ] Add debug mode for troubleshooting

---

## Phase 10: CLI Enhancements (Low Priority)

### 10.1 Commands
- [ ] Add `--warehouse-id` flag for SQL warehouses
- [ ] Add `--job-id` flag for job cost estimation
- [ ] Add `--export` for JSON/CSV export
- [ ] Add `--watch` for continuous monitoring
- [ ] Add configuration file support (dburnrate.yaml)

### 10.2 Output
- [ ] Add table visualization with rich
- [ ] Add comparison output for what-if scenarios
- [ ] Add trend charts for forecasting

---

## Post-MVP: Advanced Features

### Forecasting
- [ ] Implement Prophet-based cost forecasting
- [ ] Train per-SKU × workspace models
- [ ] Add business event awareness
- [ ] Add confidence intervals

### DLT/SDP
- [ ] Add DLT pipeline cost estimation
- [ ] Implement tier detection (Core/Pro/Advanced)
- [ ] Add pipeline dependency analysis

### Cluster Optimization
- [ ] Implement cluster right-sizing recommendations
- [ ] Add bottleneck classification (CPU/memory/io)
- [ ] Add instance family recommendations

### CI/CD Integration
- [ ] Add GitHub Actions workflow
- [ ] Add GitLab CI template
- [ ] Add pre-commit hook for cost estimation
- [ ] Add Databricks widget integration

---

## Verification Commands

After each phase, run:

```bash
# All unit tests
uv run pytest -m unit -v

# With coverage
uv run pytest --cov --cov-report=term-missing

# Lint
uv run ruff check src/ tests/
uv run ruff format --check src/ tests/
uv run bandit -c pyproject.toml -r src/

# Docstring coverage
uv run interrogate src/ -v

# Security audit
uv run pip-audit
```

---

## Key Research Documents

- **RESEARCH.md** - Technical feasibility and architecture decisions
- **CONCEPT.md** - Design rationale and competitive landscape
- **PLAN.md** - Implementation plan and phase ordering
