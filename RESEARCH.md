# Pre-execution cost estimation for Databricks: architectures beyond static analysis

**Databricks has no BigQuery-style dry-run API, but a powerful hybrid architecture is achievable** by combining Spark's `EXPLAIN COST` plans, Delta Lake transaction log metadata, query fingerprinting against `system.query.history`, and ML models trained on execution plan features. The critical insight from researching how Netflix, Uber, and Airbnb handle this at scale: no organization does pure pre-execution cost estimation from scratch — every production system relies on a blend of static plan analysis for cold-start queries and historical execution data for recurring workloads. Databricks' DBU-based billing model (time × compute) makes this fundamentally harder than BigQuery's per-byte pricing, requiring a multi-signal approach rather than a single bytes-processed estimate.

---

## EXPLAIN COST provides rich statistics without executing anything

Spark SQL's `EXPLAIN COST <query>` generates the full optimized logical plan with **per-operator `sizeInBytes` and `rowCount` estimates** without reading a single byte of data. This is the strongest pre-execution signal available on the platform. Five EXPLAIN modes exist — `SIMPLE`, `EXTENDED`, `COST`, `CODEGEN`, and `FORMATTED` — with `COST` being the key mode for estimation. A typical output annotates every operator:

```
Aggregate [s_store_sk], Statistics(sizeInBytes=20.0 B, rowCount=1)
+- Join Inner, Statistics(sizeInBytes=30.8 MB, rowCount=1.62E+6)
   +- Relation parquet, Statistics(sizeInBytes=134.6 GB, rowCount=2.88E+9)
```

The physical plan additionally reveals **join strategies** (BroadcastHashJoin vs. SortMergeJoin vs. ShuffledHashJoin), shuffle operations, pushed-down filters, and AQE markers. Programmatic access works through `spark.sql("EXPLAIN COST ...")` returning a DataFrame, or through the lower-level `df._jdf.queryExecution().optimizedPlan().stats()` in PySpark, which exposes `sizeInBytes`, `rowCount`, and per-column `attributeStats` as structured objects.

**Accuracy is the critical caveat.** With fresh column statistics from `ANALYZE TABLE`, CBO estimates show ~1.5× error factors — acceptable for cost estimation. Without statistics, errors reach **1000× or more**. Databricks Runtime 16.0+ now reports statistics completeness directly in EXPLAIN output, flagging tables with `missing`, `partial`, or `full` statistics. Predictive Optimization (GA since 2025) automatically runs ANALYZE on Unity Catalog managed tables, meaning statistics freshness is increasingly maintained without manual intervention.

**Comparison to competitors**: BigQuery's `dryRun: true` returns exact `totalBytesProcessed` that maps directly to cost at $5/TB on-demand — a single API call yields a dollar estimate. Snowflake's EXPLAIN shows estimated partition pruning (`assignedPartitions`, `assignedBytes`) but no dollar cost; per-query cost must be retroactively computed from `QUERY_HISTORY`. Databricks sits between these: richer plan-level detail than either competitor but **no direct DBU-to-dollar mapping** from EXPLAIN output. The Statement Execution API (`/api/2.0/sql/statements/`) has no `dry_run` parameter — the workaround is submitting `EXPLAIN COST <query>` as the SQL statement, which executes the EXPLAIN (not the query) and returns the plan text.

---

## Delta transaction logs are a goldmine for scan-size estimation

Delta Lake's `_delta_log` directory stores per-file metadata that provides **exact** table-level statistics without any data scanning. Each `add` action in the transaction log contains four statistics per file: `numRecords` (exact row count), `minValues` and `maxValues` (per-column, for the first 32 columns by default), `nullCount` (per-column), and `size` (exact file size in bytes). This metadata is collected automatically during writes with zero additional cost.

`DESCRIBE DETAIL tablename` surfaces the aggregate view instantly: **`sizeInBytes`** (total table size from summing all active file sizes), **`numFiles`**, `partitionColumns`, and `lastModified` — all derived from the transaction log, not from filesystem listing. For partition-level analysis, the `delta-rs` Python library provides `get_add_actions(flatten=True)` returning a DataFrame with per-file path, size_bytes, num_records, and partition values — enabling exact computation of partition-filtered scan sizes.

The key distinction is between **two separate statistics systems** in Databricks. Delta data-skipping statistics (file-level min/max/null/record counts) are automatic and always current. Query optimizer statistics (distinct_count, histograms, avg_col_len) require `ANALYZE TABLE` and are stored in the metastore catalog. For scan-size estimation, Delta metadata alone is highly accurate. For **join cardinality estimation**, it falls short — distinct value counts and distribution histograms are essential for predicting join output sizes, and these require ANALYZE. Data-skipping estimates from file-level min/max are conservative (no false negatives but possible false positives), with effectiveness depending heavily on data clustering from Z-ORDER or Liquid Clustering.

**Practical architecture implication**: Use `DESCRIBE DETAIL` for instant table-level size, parse `add` actions for partition-level sizes, and rely on file-level min/max for predicate filter estimation. Reserve ANALYZE-derived statistics for join cardinality where accuracy matters most.

---

## Query fingerprinting turns history into prediction

The most practical path for **recurring queries** (the "historical-warm" case) is normalizing SQL into structural templates, hashing them, and looking up historical execution costs from `system.query.history`. The normalization pipeline follows the Percona `pt-fingerprint` pattern: strip comments, normalize whitespace, replace all literals with `?`, collapse IN-lists to single placeholders, abstract database names. The resulting canonical string is SHA-256 hashed into a `template_id`.

Databricks' `system.query.history` table (Public Preview) provides rich per-query metrics including **`total_duration_ms`**, **`read_bytes`**, **`read_rows`**, **`read_files`**, **`produced_rows`**, `spill_to_disk_bytes`, `total_task_duration_ms`, and cache hit indicators. Joining with `system.billing.usage` on warehouse_id and time overlap enables proportional cost attribution: `(query_duration / total_warehouse_duration_in_hour) × hourly_DBU_cost × list_price`.

Three similarity-matching tiers handle progressively less-familiar queries:

- **Exact fingerprint match** (highest confidence): Same normalized template → use historical p50/p95 costs, adjusted by input data size ratio
- **AST edit distance** (high confidence): SQLGlot parses SQL into ASTs; its Change Distilling algorithm computes structural distance. Queries within a similarity threshold share cost characteristics
- **Embedding-based similarity** (medium confidence): CodeBERT or `all-mpnet-base-v2` maps SQL to dense vectors; cosine similarity finds k-nearest neighbors in embedding space for novel queries

Uber's Spark Analysers system demonstrates this at **100K+ Spark applications/day**. Their architecture uses Spark Event Listeners pushing plan hashes to Kafka, consumed by Flink-based analyzers that detect anti-patterns (duplicate plans, excessive partition scans) and auto-generate JIRA tickets with resource consumption data over 180-day windows. Their fingerprinting uses two complementary hashes: a "semantic hash" (stable within a single application run) and a "plain hash" (stable across runs for the same query).

---

## ML models achieve 14–98% accuracy depending on granularity

Research from 2019–2025 reveals a clear accuracy-coverage tradeoff for learned cost models. **Microsoft's Cleo system** (SIGMOD 2020) found that even with perfect cardinalities, traditional cost models show **258% median error** and 0.04 Pearson correlation on big-data workloads. Their learned approach achieves **14% median error** for operator-subgraph models (per-recurring-query-pattern) and **42% median error** for general operator-level models. The key architectural insight: learn a **large collection of smaller specialized models** with a meta-model that selects and ensembles predictions.

**Twitter's system** (IC2E 2021) takes a radically different approach — using raw SQL text features (no plan parsing) with historical logs, framing cost prediction as **classification into resource buckets** rather than regression. This achieves **97.9% accuracy** for CPU prediction and **97% for memory**, with ~200ms inference time per query. The power-law distribution of query costs makes classification more practical than exact point estimates.

For Spark-specific models, **RAAL** (ICDE 2022) is the first learned cost model designed for Spark SQL. Its key innovation is **resource-awareness** — incorporating executor count, memory, and cores as features alongside plan structure, since the same query costs dramatically different amounts on different cluster configurations. The architecture combines LSTM-processed plan node embeddings with adaptive attention over resource features. Its successor **DRAL** (2024) adds data-aware features using unsupervised learning to handle changing data distributions.

The **cold-start problem** has strong solutions. Zero-shot cost models (Hilprecht & Binnig, VLDB 2022) use graph neural networks with transferable features (table size, operator type, estimated cardinality) pre-trained across multiple databases, achieving competitive accuracy with **zero training queries** on the target database. Few-shot fine-tuning with just 10–100 queries dramatically improves accuracy. Bao (SIGMOD 2021 Best Paper) uses Thompson sampling for natural exploration-exploitation balance.

A sobering counterpoint: Heinrich et al. (SIGMOD 2025) found that traditional PostgreSQL cost models **often still outperform** learned models on actual plan selection tasks, despite learned models showing lower estimation errors. The lesson: cost estimation accuracy alone doesn't guarantee better optimization decisions.

---

## Unity Catalog metadata has critical gaps for cost estimation

The `information_schema.tables` view contains only structural metadata (name, type, format, owner, timestamps) — **no table size, row count, or statistics**. Similarly, `information_schema.columns` provides schema information (data types, positions, nullability) but **no cardinality estimates, distribution info, or null counts**. The Unity Catalog REST API (`/api/2.1/unity-catalog/tables/{full_name}`) returns schema, properties, and storage location but **no size or statistics** — a gap confirmed by practitioners attempting to monitor 60,000+ tables at scale.

The practical metadata access hierarchy for cost estimation is:

1. **`DESCRIBE DETAIL`** — Best lightweight source: `sizeInBytes` and `numFiles` from Delta log, no scan needed
2. **`DESCRIBE TABLE EXTENDED ... AS JSON`** — Returns `statistics.num_rows` and `statistics.size_in_bytes` if previously collected by ANALYZE or Predictive Optimization
3. **`information_schema.columns`** + type-based width estimation — Theoretical row width from data types, combinable with sizeInBytes for approximate row count
4. **Predictive Optimization history** — `system.storage.predictive_optimization_operations_history` reveals whether ANALYZE has recently run
5. **Lakehouse Monitoring profile tables** — Richest source (distinct_count, quantiles, distributions) but requires explicit setup and serverless compute cost

**There is no `pg_class`-equivalent system table** that passively records table sizes across all tables. This means any cost estimation system must either call `DESCRIBE DETAIL` per-table or maintain its own metadata cache populated from Delta logs.

---

## Serverless billing makes per-query cost prediction structurally hard

Serverless SQL warehouses bill on **DBU/hour × uptime** with per-second granularity, not per-query. Each warehouse size has a fixed burn rate (X-Small = 6 DBU/hr, Small = 12 DBU/hr, etc.) regardless of query complexity. Running more queries does not increase DBU charges unless autoscaling adds clusters. The serverless per-DBU rate (~$0.70 on AWS US) bundles infrastructure costs — higher than Pro (~$0.55) or Classic (~$0.22) but offset by 2–6 second startup times enabling aggressive auto-stop.

**No pre-execution cost estimation API exists anywhere in the Databricks platform.** The Intelligent Workload Management (IWM) system internally predicts query resource requirements for autoscaling decisions, but these predictions are not exposed via any API. The `system.billing.usage` table records warehouse-level DBU consumption at hourly granularity with no per-query cost field. The `system.query.history` table has rich per-query execution metrics but no DBU or cost columns.

The only viable per-query cost approximation is duration-proportional attribution: join query history with billing on `warehouse_id` + time overlap, then allocate `(query_total_duration / sum_all_query_durations_in_hour) × hourly_cost`. For Jobs Compute (non-serverless), cost maps more directly to `cluster_uptime × instance_cost + DBU_cost`, making estimation somewhat simpler since cluster configuration is known ahead of time.

---

## A practical hybrid architecture for pre-execution estimation

No company has publicly described a fully mature pre-execution cost estimation system. Netflix, Uber, Airbnb, and LinkedIn all invest more in **post-execution cost attribution** than pre-execution prediction. However, synthesizing their approaches with the available technical capabilities yields a viable four-tier architecture:

**Tier 1 — Instant metadata lookup (< 100ms)**. For any query, immediately fetch table sizes via cached `DESCRIBE DETAIL` results or a Delta log metadata service. Compute total input data volume across all referenced tables. Apply partition pruning estimates using file-level partition values. This handles the "how much data will be scanned?" question for cold-start queries.

**Tier 2 — EXPLAIN plan analysis (< 2 seconds)**. Submit `EXPLAIN COST <query>` via the Statement Execution API. Parse the returned plan for root-level `sizeInBytes`/`rowCount`, join strategies, shuffle count, and operator tree depth. Combine CBO estimates with Tier 1 Delta metadata for cross-validation. This provides the structural complexity signal — number of shuffles, broadcast vs. sort-merge joins, and aggregation depth.

**Tier 3 — Historical fingerprint matching (< 500ms)**. Normalize and hash the query into a template_id. Look up historical executions in a pre-built index over `system.query.history`. For exact matches, return p50/p95 cost with confidence intervals, adjusted for current input data size. For near-matches, use AST edit distance or embedding similarity. This is the highest-accuracy path for recurring workloads and handles the warm-start case.

**Tier 4 — ML model prediction (< 300ms)**. For cold-start queries with no historical match, feed extracted features (operator types, estimated cardinalities from EXPLAIN, table sizes from Delta metadata, cluster configuration) into a learned cost model. Following Twitter's approach, classify into cost buckets (low/medium/high/very-high) rather than predicting exact values. Use Cleo's hierarchical strategy: specialized models for recognized query patterns, general operator-level models as fallback, zero-shot transfer for completely novel workloads.

**Compute type differentiation**: For SQL Warehouses, cost is proportional to query duration on a fixed-rate warehouse — estimate duration, multiply by DBU rate. For Jobs Compute, cost depends on cluster configuration (instance types × count × duration) — estimate duration given known cluster spec. For Interactive clusters, cost attribution is shared across concurrent users — estimate the query's resource share using `total_task_duration_ms` relative to cluster capacity.

The tools ecosystem supports this: **Unravel** provides knowledge-graph-based optimization built natively on System Tables (25–35% sustained savings), **Sync Computing's Gradient** uses ML on Spark eventlogs for cluster configuration optimization (37–55% savings), and **Definity.ai** offers inline pipeline profiling with CI/CD validation for cost regression detection. Databricks' own **Predictive Optimization** increasingly automates statistics freshness, reducing the manual ANALYZE TABLE burden that undermines CBO accuracy.

---

## Conclusion

The most actionable finding is that **Spark's EXPLAIN COST combined with Delta transaction log metadata provides a viable cold-start estimation foundation** that requires no query execution. EXPLAIN exposes per-operator statistics including estimated data sizes and row counts, while Delta logs provide exact file-level sizes, row counts, and partition maps. Together, these can estimate scan volumes with high accuracy for single-table queries and reasonable accuracy for joins when ANALYZE TABLE statistics exist.

For warm-start estimation, **query fingerprinting against `system.query.history`** is the highest-ROI approach — Uber's production system proves this works at 100K+ daily Spark applications. The Percona-style normalization → hash → historical lookup pipeline is straightforward to implement and yields the most accurate predictions for recurring workloads, which typically constitute 70–80% of production queries.

The genuine gap is **Databricks' lack of a native pre-execution cost API**. Unlike BigQuery's single-call `dryRun: true` → estimated bytes → dollar cost, Databricks requires assembling multiple signals (EXPLAIN plan, Delta metadata, historical billing, cluster configuration) and applying a custom cost model. This gap is structural: DBU-based billing that depends on compute type, cluster size, and execution time is fundamentally harder to predict than BigQuery's bytes-processed model. Any cost estimation system for Databricks will necessarily be approximate and multi-signal, which makes the hybrid architecture described above not just a pragmatic choice but the only viable one.