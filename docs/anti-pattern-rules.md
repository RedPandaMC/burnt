# Anti-Pattern Rules Reference

`burnt check` performs AST-based static analysis on `.sql` and `.py` files,
detecting patterns that cause excessive DBU consumption, driver OOM, or
non-deterministic pipeline behaviour.

The engine operates in three tiers:

- **Tier 1 — Pattern rules**: tree-sitter AST queries; fast, structural matches.
- **Tier 2 — Context rules**: loop detection, naming patterns, flow analysis.
- **Tier 3 — Dataflow rules**: cross-statement cache lifecycle and reuse tracking.

---

## Severity Levels

| Level | Meaning |
|-------|---------|
| `error` | High-impact; fails CI when `fail-on = "error"` (default) |
| `warning` | Important but not always blocking |
| `info` | Low-signal advisory |

---

## Rule Index

43 rules are active by default (`select = ["ALL"]`).

| Code | Severity | Language | Tags | Description |
|------|----------|----------|------|-------------|
| [BB001](#bb001) | warning | notebook | notebook, cost, governance | Notebook without cost annotation |
| [BD001](#bd001) | warning | sql | delta, maintenance, sql | VACUUM called too frequently |
| [BD002](#bd002) | info | sql | delta, performance, sql | Missing ZORDER on large Delta table |
| [BN001](#bn001) | info | notebook | notebook, structure | Missing notebook header cell |
| [BN002](#bn002) | warning | notebook | notebook, security, sql | SQL credentials in notebook cell |
| [BN003](#bn003) | error | notebook | notebook, structure, correctness | Unterminated cell reference |
| [BNT-C01](#bnt-c01) | warning | python | python, style, correctness | df['col'] bracket reference (stale ref risk) |
| [BNT-I01](#bnt-i01) | error | python | python, import, style | `from pyspark.sql.functions import *` |
| [BNT-N01](#bnt-n01) | info | python | python, style, naming | Generic DataFrame variable name (`df`, `df1`) |
| [BP001](#bp001) | info | python | python, style, readability | Cell without comments |
| [BP002](#bp002) | info | python | python, style, readability | Line exceeds 120 characters |
| [BP003](#bp003) | warning | python | python, databricks, magic | Databricks `# MAGIC` in plain Python file |
| [BP004](#bp004) | warning | python | python, databricks, magic | Deprecated magic syntax |
| [BP005](#bp005) | info | notebook | notebook, style, structure | Notebook cell missing title |
| [BP006](#bp006) | info | notebook | notebook, structure, maintainability | Excessive cell nesting |
| [BP007](#bp007) | info | notebook | notebook, style, documentation | Notebook missing markdown documentation |
| [BP008](#bp008) | error | python | pyspark, memory, driver-bound | `.collect()` without `.limit()` |
| [BP010](#bp010) | error | python | pyspark, udf, performance | Python UDF (`@udf`) — row-at-a-time |
| [BP011](#bp011) | error | python | pyspark, memory, driver-bound | `.toPandas()` on full DataFrame |
| [BP012](#bp012) | warning | python | pyspark, partitioning, shuffle | `.repartition(1)` collapses parallelism |
| [BP013](#bp013) | warning | sql | sql, performance, sort | ORDER BY without LIMIT |
| [BP014](#bp014) | warning | python | pyspark, join, shuffle | Broadcast join threshold exceeded |
| [BP015](#bp015) | info | python | pyspark, udf, arrow | Pandas UDF — crosses JVM boundary |
| [BP016](#bp016) | warning | python | pyspark, performance, full-scan | `.count()` without filter (full table scan) |
| [BP020](#bp020) | warning | python | pyspark, catalyst, loop | `.withColumn()` inside a loop (O(n²) plan) |
| [BP021](#bp021) | error | python | pyspark, jdbc, performance | JDBC read missing partition options |
| [BP022](#bp022) | error | python | pyspark, sdp, dlt | Action call inside SDP/DLT function |
| [BP023](#bp023) | warning | python | pyspark, window, shuffle | `Window.orderBy()` without `partitionBy()` |
| [BP030](#bp030) | warning | python | pyspark, memory, caching | `.cache()` on a DataFrame never reused |
| [BP031](#bp031) | info | python | pyspark, memory, caching | `.cache()` without explicit `.unpersist()` |
| [BP032](#bp032) | warning | python | pyspark, performance, caching | Re-computing an already cached DataFrame |
| [BQ001](#bq001) | warning | sql | sql, correctness, null-safety | `NOT IN` with NULLs silently returns empty |
| [BQ002](#bq002) | warning | sql | sql, performance, dedup | `DISTINCT` forcing full shuffle |
| [BQ003](#bq003) | info | sql | sql, performance, aggregation | `COUNT(DISTINCT col)` at scale |
| [BQ004](#bq004) | error | sql | sql, performance, subquery | Correlated `NOT IN (SELECT ...)` subquery |
| [SDP001](#sdp001) | warning | python | sdp, data-quality, declarative | DLT table without expectation |
| [SDP002](#sdp002) | warning | python | sdp, dlt, incremental | DLT table without incremental strategy |
| [SDP003](#sdp003) | warning | python | sdp, streaming, schema | Streaming DLT source without schema |
| [SDP004](#sdp004) | warning | python | sdp, dlt, performance | DLT apply_changes without checkpoint |
| [SDP005](#sdp005) | info | python | sdp, dlt, documentation | DLT table missing comment |
| [SQ001](#sq001) | warning | sql | sql, performance, select-star | `SELECT *` without LIMIT |
| [SQ002](#sq002) | warning | sql | sql, join, cartesian | Implicit cross join (comma syntax) |
| [SQ003](#sq003) | error | sql | sql, join, cartesian | Explicit `CROSS JOIN` |

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
