# Phase 5: Integration & Hardening

> End-to-end tests. Dynamic SQL. Error handling. Config validation. Docs. Wheels.

**Duration:** 2 weeks
**Depends on:** Phase 4
**Gate:** 6 E2E tests pass. 4 access levels work. Dynamic SQL → partial output. `pip install burnt` clean. CI examples tested.

---

## Tasks

### P5-01: End-to-End Tests (Week 17)

6 fixtures through full pipeline:

| Fixture | Mode |
|---------|------|
| python_simple.py | Python: read → filter → groupBy → collect |
| python_complex.py | Python: %run across 3 files, 30+ cells |
| sql_simple.sql | SQL: CREATE TABLE AS, GROUP BY, OPTIMIZE |
| sql_merge.sql | SQL: MERGE INTO, cross-cell deps, VACUUM |
| dlt_python.py | DLT: 5 @dlt.table, streaming + MV, expectations |
| dlt_sql.sql | DLT: CREATE STREAMING TABLE, LIVE.ref |

### P5-02: Dynamic SQL Handling (Week 17)

String variable resolution: `table = "c.s.t"` + `spark.sql(f"SELECT FROM {table}")` → resolve. Widget defaults: `dbutils.widgets.text("t", "default")` → use. Unresolvable: BN002 + partial graph.

### P5-03: Error Handling Audit (Week 17)

Every external call: DESCRIBE, system tables, Pipelines API, Clusters API, EXPLAIN, Workspace API, Statement Execution. Each failure → degradation, no traceback.

### P5-04: Access Level Tests (Week 17)

Full → complete. Session → graph + DESCRIBE. REST → graph + REST enrichment. Auth-only → findings + message.

### P5-05: Config Validation (Week 17)

Invalid `burnt.toml` → clear error pointing to bad key. Invalid `[tool.burnt]` in `pyproject.toml` → same error, referencing pyproject.toml location. Missing config file → defaults silently. `pyproject.toml` without `[tool.burnt]` section → ignored, search continues. Conflicting env var and file → priority applied. `burnt check --init` round-trips: generated config loads cleanly. Config from `pyproject.toml` produces identical result as equivalent `burnt.toml`.

### P5-06: CI Examples (Week 18)

GitHub Actions, Azure DevOps, GitLab CI YAML. Credential docs. `burnt.toml` patterns for CI (env vars for secrets, file for rule config).

### P5-07: Packaging (Week 18)

`pyproject.toml` final. `maturin build --release` 4 platforms. `pip install burnt` on 3.10/3.11/3.12. `%pip install burnt` on Databricks.

### P5-08: Documentation (Week 18)

README. DESIGN.md final. AGENTS.md. CHANGELOG.md v2.0.0.

---

## Gate

- [ ] 6 E2E tests pass
- [ ] Dynamic SQL → partial graph
- [ ] No tracebacks
- [ ] 4 access levels correct
- [ ] Config validation: invalid toml → clear error, pyproject.toml parity with burnt.toml
- [ ] CI YAML for 3 platforms
- [ ] `pip install burnt` clean on 3.10+
- [ ] Docs complete
