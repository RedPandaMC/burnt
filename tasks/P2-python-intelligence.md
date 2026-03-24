# Phase 2: Python Intelligence

> Pydantic models, enrichment, estimation, session cost, recommendations, feedback. Pure library — no UI.

**Duration:** 4 weeks
**Depends on:** Phase 1
**Gate:** Unit tests pass. Estimation correct for 3 fixture graphs. Recommendations produce valid API JSON. Feedback calibrates.

---

## Tasks

### P2-01: Pydantic Models (Week 7)

All types: `CostGraph`, `PipelineGraph`, `CostNode`, `CostEdge`, `PipelineTable`, `TableDependency`, `PipelineConfig`, `Expectation`, `Finding`, `CostEstimate`, `SessionCost`, `CheckResult`, `ClusterConfig`. Deserialization from Rust JSON via `model_validate()`.

### P2-02: Environment & Access Level Detection (Week 7)

`DATABRICKS_RUNTIME_VERSION` → notebook. `DATABRICKS_HOST` → connected. Neither → `ConnectionRequired`. Probe system tables → Full. Probe DESCRIBE → Session. REST → REST. Nothing → Auth-only.

### P2-03: SparkSession Integration (Week 7)

Cluster config from SparkContext: instance type, workers, Photon, memory, SKU. Notebook source retrieval. `%run` path resolution.

### P2-04: REST Backend (Week 7–8)

`databricks-sdk` WorkspaceClient. Statement Execution API. Pipelines API. SQL warehouse auto-discovery. TTL caching (Delta 5min, system tables 1hr, configs 10min). ThreadPoolExecutor for concurrent queries.

### P2-05: DABs Bundle Parser (Week 8)

`databricks.yml` → ClusterConfig (jobs) and PipelineConfig (DLT). Target overrides. Notebook-to-job matching by path.

### P2-06: Delta Metadata Enrichment (Week 8)

DESCRIBE DETAIL → `estimated_input_bytes`. Partition pruning. DESCRIBE HISTORY → growth rate. Cache TTL 5min. Missing tables → skip.

### P2-07: DLT Pipeline Enrichment (Week 8)

Pipelines API → PipelineConfig. `pipeline_event_log` → per-table metrics. Streaming batch sizes. Graceful when unavailable.

### P2-08: EXPLAIN COST Enrichment (Week 9)

Notebook only. EXPLAIN → `estimated_output_bytes`. Skip in REST mode.

### P2-09: Scaling Functions (Week 9)

7 functions: Linear, LinearWithCliff, Quadratic, StepFailure, Maintenance, StreamBatch, FullRecompute. Calibration dict by `(operation_kind, instance_family)`. Cluster enrichment fills thresholds.

### P2-10: Cost Estimation (Week 9)

Python/SQL: topological walk, per-node cost, infrastructure, TCO. DLT: per-table walk, batch/full models, 1.12× overhead, tier rates.

### P2-11: Session Cost (Week 10)

Notebook only. Total session time, execution time, idle time. Utilization percentage.

### P2-12: Recommendations (Week 10)

Python/SQL: SKU, instance, Photon, spot, API JSON. DLT: MV → streaming, tier. Session: serverless, auto-termination.

### P2-13: Feedback Loop (Week 10)

`calibrate(job_id, run_id)`: billing → per-node. `calibrate(pipeline_id, update_id)`: event_log → per-table. EMA 0.3/0.7. Store per calibration config (local file or Delta).

### P2-14: Instance Catalog & Pricing (Week 10)

Azure catalog (23 VMs). DLT tier rates. `get_dbu_rate(sku, dlt_tier)`.

---

## Gate

- [ ] Pydantic deserialization works (3 modes)
- [ ] Access levels detected
- [ ] Enrichment passes work independently
- [ ] 7 scaling functions
- [ ] Estimation correct for 3 fixture graphs
- [ ] Session cost computed
- [ ] Recommendations produce valid JSON
- [ ] Feedback calibrates and persists
