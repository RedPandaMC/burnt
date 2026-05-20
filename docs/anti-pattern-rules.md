# Anti-Pattern Rules Reference

`burnt check` performs graph-DSL static analysis on `.sql`, `.py`, and notebook
files, detecting patterns that cause excessive DBU consumption, driver OOM, or
non-deterministic pipeline behaviour. Every rule is defined as a graph-DSL
S-expression pattern over the resolved graph — see `docs/dsl-reference.md`.

---

## Severity Levels

| Level | Meaning |
|-------|---------|
| `error` | High-impact; fails CI when `fail-on = "error"` (default) |
| `warning` | Important but not always blocking |
| `info` | Low-signal advisory |

---

## Rule Index

<!-- rule count: 111 — regenerate with: python3 scripts/gen_rule_index.py -->

111 rules are active by default (`select = ["ALL"]`).

| Code | Severity | Language | Tags | Description |
|------|----------|----------|------|-------------|
| [BB001](#bb001) | warning | notebook | notebook,cost,governance | Cluster tag missing from notebook configuration |
| [BC001](#bc001) | warning | python | pyspark,sql,correctness,config | spark.sql.legacy.* flag papers over real compatibility bugs instead of fixing them |
| [BC002](#bc002) | error | python | delta,correctness,config | Bypassing the 7-day VACUUM retention safety check has caused production data loss |
| [BC003](#bc003) | warning | python | delta,correctness,config | Enabling Delta schema auto-merge at session scope is not recommended for production; use per-write mergeSchema instead |
| [BC005](#bc005) | warning | python | delta,performance,config | Disabling optimizeWrite causes small-file proliferation on high-churn Delta tables |
| [BC006](#bc006) | info | python | delta,performance,config | Disabling Delta low-shuffle merge causes unnecessary full shuffle on MERGE operations |
| [BD001](#bd001) | warning | sql | delta,maintenance,sql | VACUUM called more frequently than needed |
| [BD002](#bd002) | info | sql | delta,performance,sql | Large Delta table missing ZORDER optimization |
| [BD010](#bd010) | warning | python | pyspark,delta,write,performance | mode('overwrite') on a Delta table replaces the entire table — use replaceWhere to scope the overwrite to a partition |
| [BD013](#bd013) | warning | python | pyspark,delta,write | Writing analytical tables as CSV or JSON lacks schema enforcement, ACID transactions, and time travel |
| [BD014](#bd014) | info | python | pyspark,delta,write,databricks | Writing as Parquet on Databricks foregoes Delta Lake ACID transactions, schema enforcement, and time travel |
| [BD015](#bd015) | warning | python | pyspark,delta,write | saveAsTable() without an explicit .format('delta') may write in the default Hive format on some clusters |
| [BD016](#bd016) | warning | python | pyspark,delta,loop,performance | .write inside a loop creates one small file per iteration and bypasses Delta's optimized bulk-write path |
| [BD020](#bd020) | info | sql | delta,performance,sql | OPTIMIZE without a WHERE clause rewrites the entire table — expensive on large tables |
| [BD021](#bd021) | warning | sql | delta,performance,sql | MERGE INTO without a partition column in the ON condition causes a full table scan on every merge |
| [BD022](#bd022) | info | sql | delta,performance,sql | WHEN MATCHED THEN UPDATE SET * without an AND condition updates unchanged rows, causing unnecessary rewrites |
| [BD026](#bd026) | info | sql | delta,performance,sql | CONVERT TO DELTA leaves small files from the source format — run OPTIMIZE afterward |
| [BD032](#bd032) | warning | sql | delta,performance,sql | More than 4 Liquid Clustering keys reduces clustering effectiveness and increases write overhead |
| [BJ001](#bj001) | warning | python | pyspark,join,correctness | Join on columns of differing types — implicit cast may silently change values or degrade performance |
| [BJ002](#bj002) | warning | python | pyspark,join,correctness | Self-joining a DataFrame without aliasing produces ambiguous column references that cause AnalysisException |
| [BJ004](#bj004) | warning | python | pyspark,join,correctness | Passing a column name as a string to .join() produces an ambiguous column reference when both DataFrames have that column |
| [BN001](#bn001) | info | notebook | notebook,structure | Notebook missing run directive or target |
| [BN002](#bn002) | warning | notebook | notebook,security,sql | Dynamic SQL construction detected - potential SQL injection risk |
| [BN003](#bn003) | error | notebook | notebook,structure,correctness | Circular run directive detected - notebook would call itself |
| [BNT-A01](#bnt-a01) | warning | python | pyspark,style,correctness | .toDF(*new_names) renames by position and breaks silently if the column count changes |
| [BNT-A02](#bnt-a02) | info | python | pyspark,style,readability | Multiple chained .select(col('a').alias('b')) calls for renaming — consolidate into a single .toDF() or .withColumnsRenamed() |
| [BNT-A03](#bnt-a03) | info | python | pyspark,style,readability | Long chains of .when().when().when()... are hard to read and maintain |
| [BNT-A04](#bnt-a04) | warning | python | pyspark,performance,rdd | df.rdd.map() deserialises rows to Python objects and loses all Spark/Photon optimisations |
| [BNT-C01](#bnt-c01) | warning | python | python,style,correctness | df['col'] or df.col outside a join can cause stale reference bugs after withColumn |
| [BNT-I01](#bnt-i01) | error | python | python,import,style | from pyspark.sql.functions import * shadows Python built-ins (max, min, sum, map, round) |
| [BNT-N01](#bnt-n01) | info | python | python,style,naming | Variable named df/df1-df9 is too generic — hinders readability |
| [BO001](#bo001) | warning | python | pyspark,observability,logging | print() of a Spark action result triggers a full job and discards the data — use logging or display() instead |
| [BO002](#bo002) | info | python | pyspark,observability,debugging | .show() triggers a full Spark job and should not remain in production pipelines |
| [BO003](#bo003) | info | python | pyspark,observability,debugging | .explain() is a debugging tool that should not remain in production pipelines |
| [BP001](#bp001) | info | python | python,style,readability | Cell has no comments |
| [BP002](#bp002) | info | python | python,style,readability | Line exceeds 120 characters |
| [BP003](#bp003) | warning | python | python,databricks,magic | Databricks magic (# MAGIC) in plain Python file |
| [BP004](#bp004) | warning | python | python,databricks,magic | Deprecated Databricks magic syntax used |
| [BP005](#bp005) | info | notebook | notebook,style,structure | Cell missing type declaration |
| [BP006](#bp006) | info | notebook | notebook,structure,maintainability | Large notebook detected (>50 cells) - consider splitting |
| [BP007](#bp007) | info | notebook | notebook,style,documentation | Notebook has no markdown cells for documentation |
| [BP008](#bp008) | error | python | pyspark,memory,driver-bound | collect() without limit() can OOM the driver |
| [BP010](#bp010) | error | python | pyspark,udf,performance | Python UDF has 10-100x overhead vs Pandas UDF |
| [BP011](#bp011) | error | python | pyspark,memory,driver-bound | toPandas() brings the entire DataFrame to the driver |
| [BP012](#bp012) | warning | python | pyspark,partitioning,shuffle | repartition(1) forces all data through a single partition |
| [BP013](#bp013) | warning | sql | sql,performance,sort | ORDER BY without LIMIT sorts the entire result set |
| [BP014](#bp014) | warning | python | pyspark,join,shuffle | CROSS JOIN creates O(n*m) rows |
| [BP015](#bp015) | info | python | pyspark,udf,arrow | pandas_udf still serialises data via Arrow — consider Spark-native functions |
| [BP016](#bp016) | warning | python | pyspark,performance,full-scan | count() without filter scans the entire table |
| [BP020](#bp020) | warning | python | pyspark,catalyst,loop | .withColumn() inside a loop causes O(n²) Catalyst plan analysis |
| [BP021](#bp021) | error | python | pyspark,jdbc,performance | JDBC read missing required partition options — reads entire table on single thread |
| [BP022](#bp022) | error | python | pyspark,sdp,dlt | Prohibited operation inside Spark Declarative Pipeline function |
| [BP023](#bp023) | warning | python | pyspark,window,shuffle | Window.orderBy() without .partitionBy() causes global sort |
| [BP030](#bp030) | warning | python | pyspark,memory,caching | .cache() with no .unpersist() in the same scope — potential memory leak |
| [BP031](#bp031) | info | python | pyspark,memory,caching | .cache() on a DataFrame used only once adds overhead with no benefit |
| [BP032](#bp032) | warning | python | pyspark,performance,caching | Same DataFrame has 2+ action calls without .cache() — plan executed multiple times |
| [BP040](#bp040) | warning | python | pyspark,performance,shuffle,config,aqe | spark.sql.shuffle.partitions hardcoded to 200 disables AQE partition coalescing |
| [BP041](#bp041) | warning | python | pyspark,performance,shuffle,config,aqe | Disabling AQE removes runtime skew handling, partition coalescing and SMJ→BHJ conversion |
| [BP042](#bp042) | info | python | pyspark,join,aqe,broadcast | Explicit broadcast() hints can backfire — AQE automatically broadcasts small tables when spark.sql.autoBroadcastJoinThreshold is set |
| [BP044](#bp044) | warning | python | pyspark,performance,shuffle,config,aqe | Disabling AQE skew join handling prevents automatic splitting of skewed sort-merge join tasks |
| [BP045](#bp045) | warning | python | pyspark,performance,shuffle,config,aqe | Disabling AQE partition coalescing forfeits the principal AQE benefit: small post-shuffle task amalgamation |
| [BP046](#bp046) | info | python | pyspark,performance,config,aqe | Setting autoBroadcastJoinThreshold to -1 disables all automatic broadcast joins, forcing every dimension join to shuffle |
| [BP050](#bp050) | warning | python | pyspark,performance,correctness,io | inferSchema=True forces Spark to read the entire file twice — a 2× I/O multiplier on large CSV/JSON |
| [BP051](#bp051) | warning | python | pyspark,performance,delta,io | mergeSchema=true reads metadata from every file to compute the union schema — kills planning time on large partitioned tables |
| [BP052](#bp052) | warning | python | pyspark,streaming,schema,performance | readStream on JSON/CSV/Avro without an explicit schema triggers schema inference on every micro-batch restart |
| [BP053](#bp053) | warning | python | pyspark,performance,schema | Reading CSV or JSON without an explicit schema forces Spark to scan the data to infer types |
| [BP060](#bp060) | warning | python | pyspark,cache,filter,performance | .filter() applied after .cache() forces a full scan of the cached data; cache the filtered DataFrame instead |
| [BP070](#bp070) | info | python | pyspark,performance,shuffle | Chained identical operations (.distinct().distinct() or .sort().sort()) waste a full shuffle stage |
| [BP071](#bp071) | warning | python | pyspark,correctness,performance | dropDuplicates() without a subset deduplicates on all columns, which is rarely intended and always expensive |
| [BP072](#bp072) | info | python | pyspark,groupby,filter,performance | .filter() after .agg() is equivalent to SQL HAVING — using filter on post-agg columns is correct but this is a style flag |
| [BP073](#bp073) | warning | python | pyspark,orderby,shuffle,performance | .orderBy() before a shuffle operation (groupBy/join/repartition) is discarded — the shuffle destroys the sort order |
| [BP074](#bp074) | info | python | pyspark,withColumn,performance | Multiple chained .withColumn() calls each add a Project node — use .withColumns({}) to add all columns in one step |
| [BP080](#bp080) | warning | python | pyspark,pandas,performance | Mixing pyspark.pandas and pandas in the same file causes implicit .to_pandas() / .to_spark() conversions |
| [BP081](#bp081) | warning | python | pyspark,pandas,performance | .to_pandas() followed by .to_spark() materialises the entire DataFrame to the driver and back — an expensive round-trip |
| [BP090](#bp090) | warning | python | pyspark,join,correctness,determinism | monotonically_increasing_id() used as a join key — IDs are not stable across recomputation and differ between runs |
| [BP091](#bp091) | warning | python | pyspark,cache,correctness,determinism | current_timestamp() or now() inside a cached DataFrame returns the evaluation time, not query time — cached values become stale |
| [BP092](#bp092) | warning | python | pyspark,correctness | rand()/randn() without a seed produces non-reproducible results across runs and rerenders |
| [BP093](#bp093) | info | python | pyspark,correctness,style | uuid() generates a different value on every recomputation of the same lineage — results differ across retries and rerenders |
| [BP094](#bp094) | warning | python | pyspark,join,partition,correctness,determinism | input_file_name() used as a partition or join key — file names vary by run and cluster, making results non-deterministic |
| [BP100](#bp100) | warning | python | pyspark,udf,photon,databricks | Python UDFs disable Photon acceleration — each row serialized to Python and back |
| [BP101](#bp101) | warning | python | pyspark,performance,rdd,photon | Accessing .rdd on a DataFrame drops out of the optimised DataFrame/Photon path into the Python RDD API |
| [BP102](#bp102) | info | python | pyspark,photon,xml,databricks | from_xml() and to_xml() are not supported by Photon — the query falls back to the non-Photon engine |
| [BP110](#bp110) | error | python | pyspark,streaming,broadcast,correctness | broadcast() applied to a streaming DataFrame is not supported and causes a StreamingQueryException at runtime |
| [BP111](#bp111) | warning | python | pyspark,performance,cache | StorageLevel.MEMORY_ONLY evicts partitions under memory pressure — lost partitions are recomputed from scratch |
| [BP112](#bp112) | warning | python | pyspark,collect,performance,driver | .toJSON().collect() converts every row to a JSON string and pulls all data to the driver — use .toPandas() or write to storage instead |
| [BQ001](#bq001) | warning | sql | sql,correctness,null-safety | NOT IN (subquery) silently returns empty result when the subquery contains NULLs |
| [BQ002](#bq002) | warning | sql | sql,performance,dedup | UNION deduplicates rows with an expensive sort — use UNION ALL if duplicates are acceptable |
| [BQ003](#bq003) | info | sql | sql,performance,aggregation | COUNT(DISTINCT col) requires full shuffle and sort — expensive at scale |
| [BQ004](#bq004) | error | sql | sql,performance,subquery | Correlated subquery references outer columns — Spark may execute as a nested loop join |
| [BS001](#bs001) | error | python | pyspark,streaming,correctness | writeStream without checkpointLocation loses all progress on restart — the stream reprocesses from the beginning |
| [BS002](#bs002) | warning | python | pyspark,streaming,databricks | readStream without .trigger() runs in micro-batch mode at full speed — add a trigger interval to control cost and latency |
| [BS003](#bs003) | warning | python | pyspark,streaming,performance | Event-time aggregation (groupBy(window(...))) without withWatermark causes unbounded state accumulation |
| [BS004](#bs004) | info | python | pyspark,streaming,correctness | foreachBatch without txnAppId/txnVersion idempotency options may cause duplicate writes on retry |
| [BS005](#bs005) | warning | python | pyspark,streaming,performance | outputMode('complete') recomputes and rewrites the entire result table on every trigger — only valid with aggregation |
| [BS006](#bs006) | warning | python | pyspark,streaming,delta,join | Stream-static join with a non-Delta static side does not automatically pick up updates to the static table |
| [BT001](#bt001) | info | python | pyspark,testing,hygiene | spark.createDataFrame([...]) with an inline list is a test fixture pattern — avoid in production pipelines |
| [BT002](#bt002) | error | python | security,credentials,governance | Hardcoded AWS access keys or Databricks tokens found in source |
| [BT003](#bt003) | error | python | security,credentials,jdbc | JDBC URL with embedded password found in source code |
| [BU001](#bu001) | warning | sql | unity-catalog,governance,sql | Two-part table name (schema.table) omits the Unity Catalog catalog prefix and resolves to the default catalog |
| [BU002](#bu002) | warning | python | unity-catalog,governance,delta | hive_metastore references in new code prevent migration to Unity Catalog and break cross-workspace portability |
| [BU003](#bu003) | error | python | unity-catalog,governance,security | Hardcoded DBFS or cloud storage paths are deprecated on Databricks — both DBFS root and mounts are being phased out |
| [BU004](#bu004) | error | python | unity-catalog,governance,security | dbutils.fs.mount() creates DBFS mounts which are deprecated in Unity Catalog workspaces |
| [SDP001](#sdp001) | warning | python | sdp,data-quality,declarative | SDP table missing data quality expectation |
| [SDP002](#sdp002) | warning | python | sdp,dlt,incremental | SDP incremental table defined without a primary key |
| [SDP003](#sdp003) | warning | python | sdp,streaming,schema | Streaming source missing explicit schema — schema inference on streams is unreliable |
| [SDP004](#sdp004) | warning | python | sdp,dlt,performance | Materialized view defined without incremental strategy |
| [SDP005](#sdp005) | info | python | sdp,dlt,documentation | SDP table missing comment metadata |
| [SDP006](#sdp006) | warning | python | sdp,dlt,incremental,performance | Materialized view defined without incremental strategy |
| [SQ001](#sq001) | warning | sql | sql,performance,select-star | SELECT * without LIMIT |
| [SQ002](#sq002) | warning | sql | sql,join,cartesian | CROSS JOIN without explicit condition |
| [SQ003](#sq003) | error | sql | sql,join,cartesian | Cartesian product detected |

---

## PySpark Rules

### BP008

| | |
|---|---|
| **Severity** | `error` |
| **Tags** | `pyspark`, `memory`, `driver-bound` |

`.collect()` without a preceding `.limit()` or `.take()` pulls the entire
DataFrame into driver memory, causing OOM on production-scale data.

```python
# flagged
results = df.filter(F.col("date") == "2025-01-01").collect()

# fixed
results = df.filter(F.col("date") == "2025-01-01").limit(1000).collect()
```

---

### BP010

| | |
|---|---|
| **Severity** | `error` |
| **Tags** | `pyspark`, `udf`, `performance` |

Python UDFs (`@udf`) serialize each row between the JVM and the Python
interpreter — 10–100× slower than native column expressions; prevents Photon.

```python
# flagged
@udf("string")
def clean_name(name): return name.strip().title()

# fixed: native Spark expression
df = df.withColumn("clean_name", F.initcap(F.trim(F.col("name"))))
```

---

### BP011

| | |
|---|---|
| **Severity** | `error` |
| **Tags** | `pyspark`, `memory`, `driver-bound` |

`.toPandas()` materialises the entire DataFrame on the driver and disables
all Spark optimisations from that point forward.

```python
# flagged
df_pd = spark.table("orders").toPandas()

# fixed: push aggregation into Spark, convert only the small result
summary = spark.table("orders").groupBy("region").agg(F.sum("amount")).toPandas()
```

---

### BP012

| | |
|---|---|
| **Severity** | `warning` |
| **Tags** | `pyspark`, `partitioning`, `shuffle` |

`.repartition(1)` collapses all data into a single partition, creating a
single-task bottleneck that negates cluster parallelism.

```python
# flagged
df.repartition(1).write.parquet("s3://bucket/output/")

# fixed
df.coalesce(8).write.parquet("s3://bucket/output/")
```

---

### BP015

| | |
|---|---|
| **Severity** | `info` |
| **Tags** | `pyspark`, `udf`, `arrow` |

Pandas UDFs cross the JVM–Python boundary even though they use Arrow
serialization. Prefer native Spark column expressions where possible.

---

### BP016

| | |
|---|---|
| **Severity** | `warning` |
| **Tags** | `pyspark`, `performance`, `full-scan` |

`.count()` without a preceding `.filter()`, `.where()`, or `.groupBy()`
triggers a full table scan. Delta statistics do not short-circuit `.count()`.

```python
# flagged
total = spark.table("events").count()

# fixed
recent = spark.table("events").filter(F.col("date") >= "2025-01-01").count()
```

---

### BP014

| | |
|---|---|
| **Severity** | `warning` |
| **Tags** | `pyspark`, `join`, `shuffle` |

Joining two large DataFrames without a broadcast hint causes a full shuffle
sort-merge join, often the dominant cost in a job.

---

### BP020

| | |
|---|---|
| **Severity** | `warning` |
| **Tags** | `pyspark`, `catalyst`, `loop` |

Each `.withColumn()` call appends a `Project` node to the Catalyst logical
plan. Calling it in a loop with N columns creates N nested nodes — plan
compilation can take minutes for 50+ columns.

```python
# flagged
for col_name, expr in transformations.items():
    df = df.withColumn(col_name, expr)

# fixed
df = df.select("*", *[expr.alias(name) for name, expr in transformations.items()])
```

---

### BP021

| | |
|---|---|
| **Severity** | `error` |
| **Tags** | `pyspark`, `jdbc`, `performance` |

A JDBC read without `partitionColumn`, `numPartitions`, `lowerBound`, and
`upperBound` uses a single thread, ignoring cluster parallelism.

```python
# flagged
df = spark.read.format("jdbc").option("url", url).option("dbtable", "orders").load()

# fixed
df = (spark.read.format("jdbc")
    .option("url", url).option("dbtable", "orders")
    .option("partitionColumn", "order_id")
    .option("lowerBound", "1").option("upperBound", "10000000")
    .option("numPartitions", "50").load())
```

---

### BP022

| | |
|---|---|
| **Severity** | `error` |
| **Tags** | `pyspark`, `sdp`, `dlt` |

Action calls (`.write`, `.collect`, `.show`, `.display`) inside functions
decorated with `@dlt.table` cause non-deterministic execution and can
deadlock the pipeline.

```python
# flagged
@dlt.table
def processed_orders():
    n = spark.table("raw.orders").count()   # action inside SDP
    return spark.table("raw.orders").filter(F.col("status") == "valid")

# fixed
@dlt.table
def processed_orders():
    return spark.table("raw.orders").filter(F.col("status") == "valid")
```

---

### BP023

| | |
|---|---|
| **Severity** | `warning` |
| **Tags** | `pyspark`, `window`, `shuffle` |

`Window.orderBy()` without `partitionBy()` performs a global sort across all
data, requiring a full shuffle to a single partition.

```python
# flagged
w = Window.orderBy("ts")

# fixed
w = Window.partitionBy("user_id").orderBy("ts")
```

---

### BP030–BP032 (Caching)

| Code | Severity | Issue |
|------|----------|-------|
| BP030 | warning | `.cache()` on a DataFrame that is never reused — wastes memory |
| BP031 | info | `.cache()` without a matching `.unpersist()` — memory leak risk |
| BP032 | warning | Re-computing a DataFrame that was already cached |

---

### BNT-I01

| | |
|---|---|
| **Severity** | `error` |
| **Tags** | `python`, `import`, `style` |

`from pyspark.sql.functions import *` shadows Python built-ins (`max`, `min`,
`sum`, `map`, `round`), causing silent bugs that are hard to trace.

```python
# flagged
from pyspark.sql.functions import *

# fixed
from pyspark.sql import functions as F
```

---

### BNT-C01

| | |
|---|---|
| **Severity** | `warning` |
| **Tags** | `python`, `style`, `correctness` |

`df['col']` column references can become stale after a `.withColumn()` call
because they resolve at plan creation time. `F.col('col')` resolves at
evaluation time.

---

### BNT-N01

| | |
|---|---|
| **Severity** | `info` |
| **Tags** | `python`, `style`, `naming` |

Generic DataFrame variable names (`df`, `df1`, `df2`) hinder readability in
code reviews and debugging.

---

## SQL Rules

### SQ001

| | |
|---|---|
| **Severity** | `warning` |
| **Tags** | `sql`, `performance`, `select-star` |

`SELECT *` prevents column pruning — the optimizer cannot skip reading
unneeded columns — and returns all columns to the caller.

```sql
-- flagged
SELECT * FROM large_events WHERE date = '2025-01-01'

-- fixed
SELECT event_id, user_id, event_type FROM large_events WHERE date = '2025-01-01'
```

---

### SQ002 / SQ003

| Code | Severity | Issue |
|------|----------|-------|
| SQ002 | warning | Implicit cross join (comma syntax `FROM a, b`) |
| SQ003 | error | Explicit `CROSS JOIN` |

Both produce O(n × m) output rows. On large tables this exhausts shuffle
memory and causes spill or OOM.

```sql
-- flagged (SQ003)
SELECT a.id, b.value FROM orders a CROSS JOIN products b

-- fixed
SELECT a.id, b.value FROM orders a INNER JOIN products b ON a.product_id = b.id
```

---

### BP013

| | |
|---|---|
| **Severity** | `warning` |
| **Tags** | `sql`, `performance`, `sort` |

`ORDER BY` without `LIMIT` forces a full global sort — all data shuffled to
one reducer and sorted in memory.

```sql
-- flagged
SELECT user_id, total FROM summary ORDER BY total DESC

-- fixed
SELECT user_id, total FROM summary ORDER BY total DESC LIMIT 100
```

---

### BQ001

| | |
|---|---|
| **Severity** | `warning` |
| **Tags** | `sql`, `correctness`, `null-safety` |

`NOT IN (subquery)` returns an empty result when the subquery contains any
`NULL` value. This is a silent correctness bug, not a performance issue.

```sql
-- flagged
SELECT * FROM orders WHERE customer_id NOT IN (SELECT id FROM blocked_customers)

-- fixed: NULL-safe
SELECT * FROM orders WHERE NOT EXISTS (
    SELECT 1 FROM blocked_customers WHERE id = orders.customer_id
)
-- or: filter NULLs in the subquery
SELECT * FROM orders WHERE customer_id NOT IN (
    SELECT id FROM blocked_customers WHERE id IS NOT NULL
)
```

---

### BQ002

| | |
|---|---|
| **Severity** | `warning` |
| **Tags** | `sql`, `performance`, `dedup` |

`SELECT DISTINCT` requires a full shuffle sort. Prefer `GROUP BY` when
aggregations are involved, or `ROW_NUMBER()` when deduplication logic is complex.

---

### BQ003

| | |
|---|---|
| **Severity** | `info` |
| **Tags** | `sql`, `performance`, `aggregation` |

`COUNT(DISTINCT col)` requires a full shuffle and sort. At large scale,
`approx_count_distinct()` is orders of magnitude faster with ~2% error.

```sql
-- flagged (at scale)
SELECT COUNT(DISTINCT user_id) FROM events

-- consider
SELECT approx_count_distinct(user_id) FROM events
```

---

### BQ004

| | |
|---|---|
| **Severity** | `error` |
| **Tags** | `sql`, `performance`, `subquery` |

`NOT IN (SELECT ...)` with a correlated subquery re-executes the subquery
for every outer row. Rewrite as `NOT EXISTS` or a left anti-join.

---

## Delta Rules

### BD001

| | |
|---|---|
| **Severity** | `warning` |
| **Tags** | `delta`, `maintenance`, `sql` |

`VACUUM` called more frequently than the recommended retention window
increases maintenance overhead without benefit for typical workloads.

---

### BD002

| | |
|---|---|
| **Severity** | `info` |
| **Tags** | `delta`, `performance`, `sql` |

Large Delta tables without a `ZORDER BY` clause on high-cardinality filter
columns miss data-skipping optimisation.

---

## SDP / DLT Rules

### SDP001

| | |
|---|---|
| **Severity** | `warning` |
| **Tags** | `sdp`, `data-quality`, `declarative` |

DLT table without a data quality expectation (`@dlt.expect`, `@dlt.expect_or_drop`).
Unchecked pipelines silently propagate bad data.

---

### SDP002

| | |
|---|---|
| **Severity** | `warning` |
| **Tags** | `sdp`, `dlt`, `incremental` |

DLT materialized view defined without an incremental strategy. Large static
tables reprocess all data on every pipeline run.

---

### SDP003

| | |
|---|---|
| **Severity** | `warning` |
| **Tags** | `sdp`, `streaming`, `schema` |

Streaming DLT source without an explicit schema definition. Schema inference
on streaming sources can fail or change between runs.

---

### SDP004

| | |
|---|---|
| **Severity** | `warning` |
| **Tags** | `sdp`, `dlt`, `performance` |

`dlt.apply_changes()` without a checkpoint configuration will reprocess all
change data on restart.

---

### SDP005

| | |
|---|---|
| **Severity** | `info` |
| **Tags** | `sdp`, `dlt`, `documentation` |

DLT table missing a `comment` — reduces discoverability in the Unity Catalog.

---

## Notebook Rules

### BB001

| | |
|---|---|
| **Severity** | `warning` |
| **Tags** | `notebook`, `cost`, `governance` |

Notebook without a cost annotation comment. Required for cost attribution in
multi-team environments.

---

### BN001–BN003

| Code | Severity | Issue |
|------|----------|-------|
| BN001 | info | Missing notebook header cell |
| BN002 | warning | SQL credentials hard-coded in a notebook cell |
| BN003 | error | Unterminated cell reference (`%run`) |

---

### BP005–BP007

| Code | Severity | Issue |
|------|----------|-------|
| BP005 | info | Notebook cell missing a title comment |
| BP006 | info | Excessive cell nesting depth |
| BP007 | info | Notebook missing markdown documentation cells |

---

### BP001–BP004 (Python style)

| Code | Severity | Issue |
|------|----------|-------|
| BP001 | info | Cell without any comments |
| BP002 | info | Line exceeds 120 characters |
| BP003 | warning | Databricks `# MAGIC` marker in a plain `.py` file |
| BP004 | warning | Deprecated magic syntax (`# MAGIC run`, `# MAGIC sql`) |

---

## Disabling Rules

### Via config file

```toml
# .burnt.toml
[lint]
ignore = ["BP008"]           # exact ID
ignore = ["BP"]              # prefix — all BP rules
ignore = ["performance"]     # tag — all performance-tagged rules
ignore = ["ALL"]             # disable everything
```

### Via CLI flag

```bash
# exact ID
burnt check ./src/ --ignore BP008

# prefix
burnt check ./src/ --ignore BP

# tag
burnt check ./src/ --ignore performance

# multiple patterns
burnt check ./src/ --ignore BP008 --ignore driver-bound
```

### Via inline comment (single line)

```python
df.collect()  # burnt: ignore[BP008]
df.collect()  # burnt: ignore[BP008, driver-bound]
df.collect()  # burnt: ignore
```

### Via standalone comment (suppresses the next line)

```python
# burnt: ignore[BP008]
df.collect()
```

### Via file-level comment

```python
# burnt: ignore-file[pyspark]       # suppress tag
# burnt: ignore-file[BP008, BP011]  # suppress specific rules
# burnt: ignore-file                 # suppress everything
```

### Per-file via config

```toml
[lint.per-file-ignores]
"notebooks/explore*.py" = ["performance", "BP008"]
"migrations/*.sql"      = ["BQ*"]
```

---

## Output Formats

```bash
burnt check ./src/                   # default: rich table
burnt check ./src/ --output text     # plain text, one line per finding
burnt check ./src/ --output json     # JSON array for CI integration
```

JSON output schema per finding:

```json
{
  "file": "src/jobs/daily_agg.py",
  "rule": "BP008",
  "severity": "error",
  "description": "collect() without limit() can OOM the driver",
  "suggestion": "Add .limit(n).collect() or use .take(n)"
}
```

---

## Fail Threshold

```bash
burnt check ./src/ --fail-on error    # default: fail on error only
burnt check ./src/ --fail-on warning  # also fail on warnings
burnt check ./src/ --fail-on info     # fail on any finding
```

Or in config:

```toml
[lint]
fail-on = "warning"
```
