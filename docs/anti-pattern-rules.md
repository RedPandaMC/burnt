# Anti-Pattern Rules Reference

`burnt check` performs AST-based static analysis on `.sql` and `.py` files,
detecting patterns that cause excessive DBU consumption, driver OOM, or
non-deterministic pipeline behaviour.

---

## How It Works

- **SQL files** (`.sql`): parsed with [sqlglot](https://sqlglot.com/) for structural analysis
- **PySpark files** (`.py`): parsed with Python's `ast` module; method chains and decorator patterns are walked via a custom `NodeVisitor`
- **Language detection**: automatic from file extension (`.py` → `pyspark`, `.sql` → `sql`)

---

## Severity Levels

| Level | Symbol | Meaning |
|-------|--------|---------|
| `error` | `✗` | High-impact pattern; fails CI when `fail-on = "error"` (default) |
| `warning` | `⚠` | Important but not always blocking |
| `info` | `ℹ` | Low-signal advisory |

---

## Registered Rules

The 12 rules below are active by default (`select = ["ALL"]`) and can be
individually toggled via `burnt rules`, `--ignore-rule`, or the `ignore` config key.

---

### `cross_join`

| | |
|---|---|
| **Severity** | `warning` |
| **Language** | SQL |

Detects an explicit `CROSS JOIN` in the query AST.

A cross join produces O(n × m) output rows. On large tables this exhausts
shuffle memory and causes spill or OOM.

```sql
-- flagged
SELECT a.id, b.value FROM orders a CROSS JOIN products b
```

```sql
-- fixed
SELECT a.id, b.value FROM orders a INNER JOIN products b ON a.product_id = b.id
```

*Suggestion: Use INNER JOIN with explicit ON clause*

---

### `select_star`

| | |
|---|---|
| **Severity** | `error` |
| **Language** | SQL |

Detects `SELECT *` in a query that has no `LIMIT` clause.

`SELECT *` without `LIMIT` prevents column pruning — the optimizer cannot skip
reading unneeded columns — and returns all rows to the caller. On wide or large
tables this dramatically increases scan bytes and driver memory.

```sql
-- flagged
SELECT * FROM large_events_table WHERE date = '2025-01-01'
```

```sql
-- fixed
SELECT event_id, user_id, event_type FROM large_events_table WHERE date = '2025-01-01'
```

*Suggestion: Add LIMIT clause or select specific columns*

---

### `order_by_no_limit`

| | |
|---|---|
| **Severity** | `warning` |
| **Language** | SQL |

Detects an `ORDER BY` clause in a query that has no `LIMIT` clause.

A global sort requires all data to be shuffled to a single reducer and sorted
in memory. Without `LIMIT` the full sorted dataset must materialise — the most
common cause of long-running SQL Warehouse queries.

```sql
-- flagged
SELECT user_id, total_spend FROM user_summary ORDER BY total_spend DESC
```

```sql
-- fixed
SELECT user_id, total_spend FROM user_summary ORDER BY total_spend DESC LIMIT 100
```

*Suggestion: Add LIMIT or remove ORDER BY if not needed*

---

### `drop_table_deprecated`

| | |
|---|---|
| **Severity** | `warning` |
| **Language** | SQL, PySpark |

Detects a `DROP TABLE` statement (SQL AST node or string match in PySpark).

A `DROP TABLE` + `CREATE TABLE` pair has a window where the table does not
exist, causing downstream readers to fail. On Delta tables it also discards
the transaction log and all history.

```sql
-- flagged
DROP TABLE IF EXISTS staging.orders;
CREATE TABLE staging.orders AS SELECT ...;
```

```sql
-- fixed
CREATE OR REPLACE TABLE staging.orders AS SELECT ...;
```

*Suggestion: Use CREATE OR REPLACE TABLE or TRUNCATE TABLE*

---

### `sdp_pivot_prohibited`

| | |
|---|---|
| **Severity** | `error` |
| **Language** | SQL, PySpark |

Detects a `PIVOT` clause in SQL, or the string `PIVOT` in PySpark source.

The `PIVOT` clause is not supported in Spark Declarative Pipelines (DLT).
Including it causes pipeline materialisation to fail at parse time.

```sql
-- flagged
SELECT * FROM sales
PIVOT (SUM(amount) FOR quarter IN ('Q1', 'Q2', 'Q3', 'Q4'))
```

```sql
-- fixed: use conditional aggregation
SELECT product_id,
  SUM(CASE WHEN quarter = 'Q1' THEN amount END) AS Q1,
  SUM(CASE WHEN quarter = 'Q2' THEN amount END) AS Q2
FROM sales GROUP BY product_id
```

*Suggestion: Use alternative transformation pattern*

---

### `collect_without_limit`

| | |
|---|---|
| **Severity** | `error` |
| **Language** | PySpark |

Detects a call to `.collect()` not preceded by `.limit()` or `.take()` in the
same method chain.

`.collect()` pulls the entire DataFrame to the driver. On production-scale
data this causes driver OOM and kills the cluster.

```python
# flagged
results = df.filter(F.col("date") == "2025-01-01").collect()
```

```python
# fixed
results = df.filter(F.col("date") == "2025-01-01").limit(1000).collect()
# or
results = df.filter(F.col("date") == "2025-01-01").take(1000)
```

*Suggestion: Add .limit(n).collect() or use .take()*

---

### `python_udf`

| | |
|---|---|
| **Severity** | `error` |
| **Language** | PySpark |

Detects a function decorated with `@udf` (but not `@pandas_udf`).

Python UDFs serialize each row from the JVM to the Python interpreter and back.
This row-at-a-time overhead is 10–100× slower than native Spark column
expressions and prevents Photon acceleration entirely.

```python
# flagged
@udf("string")
def clean_name(name):
    return name.strip().title()
```

```python
# fixed: vectorized Pandas UDF
@pandas_udf("string")
def clean_name(names: pd.Series) -> pd.Series:
    return names.str.strip().str.title()

# better: native Spark function
df = df.withColumn("clean_name", F.initcap(F.trim(F.col("name"))))
```

*Suggestion: Use @pandas_udf for vectorized operations*

---

### `toPandas`

| | |
|---|---|
| **Severity** | `error` |
| **Language** | PySpark |

Detects any call to `.toPandas()`.

Like `.collect()`, `.toPandas()` materialises the full DataFrame in driver
memory. It also disables all Photon and Spark optimisations from that point
forward.

```python
# flagged
df_pd = spark.table("orders").toPandas()
summary = df_pd.groupby("region")["amount"].sum()
```

```python
# fixed: push the aggregation into Spark, then convert the small result
summary = (
    spark.table("orders")
    .groupBy("region")
    .agg(F.sum("amount").alias("total"))
    .toPandas()   # safe: result is already small
)
```

*Suggestion: Use Koalas/Pandas API on Spark or filter first*

---

### `repartition_one`

| | |
|---|---|
| **Severity** | `warning` |
| **Language** | PySpark |

Detects `.repartition(1)` (literal integer argument `1`).

This collapses all data into a single partition, eliminating cluster
parallelism and creating a single-task bottleneck that negates the benefit of
a multi-node cluster.

```python
# flagged
df.repartition(1).write.parquet("s3://bucket/output/")
```

```python
# fixed: use coalesce — reduces partitions without a full shuffle
df.coalesce(8).write.parquet("s3://bucket/output/")
```

*Suggestion: Use larger partition count or remove*

---

### `pandas_udf`

| | |
|---|---|
| **Severity** | `warning` |
| **Language** | PySpark |

Detects a function decorated with `@pandas_udf`.

Pandas UDFs use Apache Arrow for serialization (far more efficient than
row-at-a-time Python UDFs) but still cross the JVM–Python boundary and
prevent Photon acceleration. Native Spark column expressions require no
serialization at all.

```python
# flagged
@pandas_udf("double")
def normalize(values: pd.Series) -> pd.Series:
    return (values - values.mean()) / values.std()
```

```python
# consider: window function avoids the UDF entirely
w = Window.partitionBy("group")
df = df.withColumn("normalized",
    (F.col("value") - F.mean("value").over(w)) / F.stddev("value").over(w)
)
```

*Suggestion: Check if F.transform(), F.aggregate(), or column expressions can replace this UDF*

---

### `count_without_filter`

| | |
|---|---|
| **Severity** | `warning` |
| **Language** | PySpark |

Detects `.count()` not preceded by `.filter()`, `.where()`, `.groupBy()`, or
`.groupby()` in the same chain.

An unfiltered `.count()` on a large Delta table triggers a full table scan.
Delta statistics do not short-circuit `.count()` unless the fast path is
active and chosen by the query planner.

```python
# flagged
total = spark.table("events").count()
```

```python
# fixed: filter first
recent = spark.table("events").filter(F.col("date") >= "2025-01-01").count()

# alternative: approximate count avoids a full scan
approx = spark.table("events").select(F.approx_count_distinct("user_id")).first()[0]
```

*Suggestion: Add .filter()/.where() before .count() to reduce scanned rows, or use approx_count_distinct() for estimates*

---

### `withColumn_in_loop`

| | |
|---|---|
| **Severity** | `warning` |
| **Language** | PySpark |

Detects `.withColumn()` or `.withColumnRenamed()` called inside a `for` or
`while` loop.

Each `.withColumn()` call appends a new `Project` node to the logical plan.
Calling it in a loop with N columns creates N nested `Project` nodes. For
large N (50+ columns) plan compilation takes minutes and can exhaust driver
heap.

```python
# flagged
for col_name, expr in transformations.items():
    df = df.withColumn(col_name, expr)
```

```python
# fixed: build all columns in a single select
df = df.select(
    "*",
    *[expr.alias(col_name) for col_name, expr in transformations.items()]
)
```

*Suggestion: Combine transformations before the loop or use foldLeft*

---

### `jdbc_incomplete_partition`

| | |
|---|---|
| **Severity** | `error` |
| **Language** | PySpark |

Detects a JDBC read (`.format("jdbc")` or `.jdbc()`) that does not provide
all four required partitioning options: `partitionColumn`, `numPartitions`,
`lowerBound`, `upperBound`.

A JDBC read without partitioning options uses a single thread for all data,
ignoring cluster parallelism and causing driver memory pressure.

```python
# flagged
df = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", "orders").load()
```

```python
# fixed
df = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "orders")
    .option("partitionColumn", "order_id")
    .option("lowerBound", "1")
    .option("upperBound", "10000000")
    .option("numPartitions", "50")
    .load()
)
```

*Suggestion: Add partitionColumn, numPartitions, lowerBound, and upperBound options*

---

### `sdp_prohibited_ops`

| | |
|---|---|
| **Severity** | `error` |
| **Language** | PySpark |

Detects action calls (`collect`, `count`, `toPandas`, `save`, `saveAsTable`,
`start`, `toTable`) inside functions decorated with `@dlt.table`,
`@dlt.materialized_view`, or `@dlt.temporary_view`.

DLT executes dataset functions lazily. Triggering actions inside an SDP
function causes non-deterministic execution and can deadlock the pipeline.

```python
# flagged
@dlt.table
def processed_orders():
    df = spark.table("raw.orders")
    n = df.count()   # action inside SDP function
    return df.filter(F.col("status") == "valid")
```

```python
# fixed: no actions inside the decorated function
@dlt.table
def processed_orders():
    return spark.table("raw.orders").filter(F.col("status") == "valid")
```

*Suggestion: Remove this operation from SDP pipeline code*

---

## Additional Detected Rules

The following rules are generated by the detection engine but are not yet
registered in the CLI rule registry. They appear when detected but cannot be
individually disabled via `ignore` or `burnt rules` until they are registered
in a future sprint.

### `sdp_side_effects`

**Severity:** `warning` | **Language:** PySpark

Detects `print(`, `global `, or `nonlocal ` statements in PySpark source.
Inside SDP pipeline functions these cause non-deterministic behaviour; outside
SDP they are a style advisory.

*Suggestion: Remove print statements and avoid global variables*

---

## Disabling Rules

### Via config file (permanent, all runs)

```toml
# .burnt.toml
[lint]
ignore = ["cross_join", "pandas_udf"]
```

### Per-file suppression

```toml
[lint.per-file-ignores]
"migrations/*.sql"       = ["select_star"]
"notebooks/explore*.py"  = ["toPandas", "collect_without_limit"]
```

### Via CLI flag (single run)

```bash
burnt check ./src/ --ignore-rule cross_join --ignore-rule pandas_udf
```

`--ignore-rule` can be repeated and is merged with any `ignore` list from the
config file.

### Via `burnt rules` (interactive TUI)

```bash
burnt rules
```

Opens an interactive terminal UI showing all 12 registered rules with their
current enabled/disabled status. Toggle rules by entering their number. Changes
are written directly to the active config file.

---

## Fail Threshold

By default `burnt check` exits with code 1 only when an `error`-severity rule
triggers. To also fail on warnings:

```bash
burnt check ./src/ --fail-on warning
```

Or in config:

```toml
[lint]
fail-on = "warning"   # info | warning | error
```

Setting `fail-on = "info"` causes the command to fail on any finding at all.

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
  "rule": "collect_without_limit",
  "severity": "error",
  "description": "collect() without limit() can OOM the driver",
  "suggestion": "Add .limit(n).collect() or use .take()"
}
```
