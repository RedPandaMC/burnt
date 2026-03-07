# Task: Research ML cost model approaches for Spark/Databricks

---

## Metadata

```yaml
id: p6-00-research-ml-models
status: todo
phase: 6
priority: medium
agent: ~
blocked_by: []
created_by: planner
```

---

## Context

### Goal

Research the academic and production ML approaches for Spark query cost estimation. Produce `docs/ml-cost-model-research.md` with concrete design decisions for p6-01 (feature extraction) and p6-02 (classification model).

### Files to read

```
DESIGN.md                          # §"ML models achieve 14–98% accuracy"
src/dburnrate/parsers/explain.py   # ExplainPlan model — available features
src/dburnrate/parsers/delta.py     # DeltaTableInfo — table size features
src/dburnrate/core/models.py       # ClusterConfig — cluster features
src/dburnrate/tables/queries.py    # QueryRecord — training data source
```

### Research areas

1. **Cleo (Microsoft SIGMOD 2020)**
   - Architecture: collection of specialized subgraph models + meta-model ensemble
   - 14% median error at operator-subgraph level
   - What features does it use? What's transferable to our ExplainPlan model?

2. **Twitter IC2E 2021**
   - Raw SQL text features → classification into resource buckets
   - 97.9% CPU accuracy, 97% memory accuracy, ~200ms inference
   - Cost bucket approach: why classification beats regression for this problem
   - What are the bucket boundaries they used?

3. **RAAL/DRAL (ICDE 2022-2024)**
   - First Spark-specific learned cost models
   - Resource-awareness: executors, memory, cores as features (RAAL)
   - Data-aware features via unsupervised learning (DRAL)
   - How to incorporate cluster config (ClusterConfig) as features

4. **Cold-start and few-shot**
   - Zero-shot graph neural networks (Hilprecht & Binnig, VLDB 2022) — too heavy for MVP
   - Few-shot fine-tuning: with 10–100 historical queries, accuracy improves dramatically
   - Bao (SIGMOD 2021): Thompson sampling — relevant for warehouse-level optimization

5. **Counterpoint**
   - Heinrich et al. SIGMOD 2025: traditional models often outperform learned models on plan selection
   - Implication: start simple (GradientBoosting), don't over-engineer

6. **sklearn model selection**
   - `GradientBoostingClassifier` vs `RandomForestClassifier` vs `HistGradientBoostingClassifier`
   - Inference time requirement: <300ms
   - Training data size: expect 100–10,000 historical queries in `system.query.history`
   - Model serialization: `joblib.dump()` / `joblib.load()`

7. **Feature engineering from our data**
   - From `ExplainPlan`: operator count by type, total_size_bytes, shuffle_count, join_strategies, broadcast_joins, max_cardinality
   - From `DeltaTableInfo`: table_count, total_size_bytes, avg_file_count, partition_count
   - From `ClusterConfig`: num_workers, dbu_per_hour, photon_enabled (→ bool), instance_family (one-hot encoded)
   - From `QueryRecord`: historical p50_duration_ms (when available as training label)

8. **Cost buckets**
   - Define 4 buckets based on DBU: low (<0.1), medium (0.1–1.0), high (1.0–10.0), very_high (>10.0)
   - Or use duration-based buckets: <30s, 30s–5min, 5min–30min, >30min
   - Recommendation: which to use and why

---

## Acceptance Criteria

- [ ] `docs/ml-cost-model-research.md` created covering all 8 areas
- [ ] Each area ends with "Recommendation for dburnrate" section
- [ ] Feature vector specification: exact list of features, types, and encoding
- [ ] Cost bucket boundaries defined with rationale
- [ ] sklearn model choice justified with inference time estimate
- [ ] Training data schema: what columns from `system.query.history` are used as labels
- [ ] No code changes — research only

---

## Verification

```bash
ls -la docs/ml-cost-model-research.md
wc -l docs/ml-cost-model-research.md
```

---

## Handoff

### Result

[Executor: fill in after completion]

### Blocked reason

[If blocked, explain here]
