"""Rule registry — single source of truth for all burnt lint rules.

Rule code convention:
  BP  — PySpark API patterns (performance/cost)
  BQ  — SQL anti-patterns (cost)
  BD  — Delta Lake patterns
  BB  — Databricks platform patterns
  BNT — Style & naming guidelines
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Literal


class Severity(StrEnum):
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"
    STYLE = "style"


class Confidence(StrEnum):
    """How likely the rule fires on intentional production code.

    HIGH   — universally bad; very low false-positive rate
    MEDIUM — contextually bad; may be intentional in production
    LOW    — suspicious; flag for review
    """

    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


@dataclass(frozen=True)
class Rule:
    id: str                                       # symbolic: 'collect_without_limit'
    code: str                                     # prefixed:  'BP001'
    severity: Severity
    confidence: Confidence
    language: Literal["sql", "pyspark", "all"]
    description: str
    suggestion: str
    fixable: bool = False


# ---------------------------------------------------------------------------
# Registry — all rules indexed by symbolic id
# ---------------------------------------------------------------------------

_ALL_RULES: list[Rule] = [
    # ------------------------------------------------------------------
    # BP — PySpark cost/performance rules
    # ------------------------------------------------------------------
    Rule(
        id="collect_without_limit",
        code="BP001",
        severity=Severity.ERROR,
        confidence=Confidence.HIGH,
        language="pyspark",
        description="collect() without limit() can OOM the driver",
        suggestion="Add .limit(n).collect() or use .take()",
    ),
    Rule(
        id="select_star",
        code="BP002",
        severity=Severity.ERROR,
        confidence=Confidence.HIGH,
        language="sql",
        description="SELECT * without LIMIT returns all rows and prevents column pruning",
        suggestion="Add LIMIT clause or select specific columns",
    ),
    Rule(
        id="python_udf",
        code="BP003",
        severity=Severity.ERROR,
        confidence=Confidence.HIGH,
        language="pyspark",
        description="Python UDF has 10-100x overhead vs Pandas UDF",
        suggestion="Use @pandas_udf for vectorized operations",
    ),
    Rule(
        id="toPandas",
        code="BP004",
        severity=Severity.ERROR,
        confidence=Confidence.HIGH,
        language="pyspark",
        description="toPandas() brings all data to driver",
        suggestion="Use Koalas/Pandas API on Spark or filter first",
    ),
    Rule(
        id="repartition_one",
        code="BP005",
        severity=Severity.WARNING,
        confidence=Confidence.MEDIUM,
        language="pyspark",
        description="repartition(1) causes single partition bottleneck",
        suggestion="Use larger partition count or remove",
    ),
    Rule(
        id="order_by_no_limit",
        code="BP006",
        severity=Severity.WARNING,
        confidence=Confidence.HIGH,
        language="sql",
        description="ORDER BY without LIMIT forces global sort",
        suggestion="Add LIMIT or remove ORDER BY if not needed",
    ),
    Rule(
        id="cross_join",
        code="BP007",
        severity=Severity.WARNING,
        confidence=Confidence.MEDIUM,
        language="all",
        description="CROSS JOIN creates O(n*m) rows",
        suggestion="Use INNER JOIN with explicit ON clause",
    ),
    Rule(
        id="pandas_udf",
        code="BP008",
        severity=Severity.WARNING,
        confidence=Confidence.MEDIUM,
        language="pyspark",
        description="Pandas UDF has Arrow serialization overhead; prefer native Spark functions",
        suggestion="Check if F.transform(), F.aggregate(), or column expressions can replace this UDF",
    ),
    Rule(
        id="count_without_filter",
        code="BP009",
        severity=Severity.WARNING,
        confidence=Confidence.MEDIUM,
        language="pyspark",
        description="count() on unfiltered DataFrame scans all records",
        suggestion="Add .filter()/.where() before .count() to reduce scanned rows",
    ),
    Rule(
        id="withColumn_in_loop",
        code="BP010",
        severity=Severity.WARNING,
        confidence=Confidence.HIGH,
        language="pyspark",
        description=".withColumn() inside a loop causes O(n²) Catalyst plan analysis",
        suggestion="Use .withColumns() (Spark 3.3+) or a single .select() statement",
    ),
    Rule(
        id="jdbc_incomplete_partition",
        code="BP011",
        severity=Severity.ERROR,
        confidence=Confidence.HIGH,
        language="pyspark",
        description="JDBC read missing required partition options — reads entire table on single thread",
        suggestion="Add partitionColumn, numPartitions, lowerBound, and upperBound options",
    ),
    Rule(
        id="sdp_prohibited_ops",
        code="BP012",
        severity=Severity.ERROR,
        confidence=Confidence.HIGH,
        language="pyspark",
        description="Prohibited operation inside Spark Declarative Pipeline function",
        suggestion="Remove this operation from SDP pipeline code",
    ),
    Rule(
        id="window_without_partition_by",
        code="BP013",
        severity=Severity.WARNING,
        confidence=Confidence.MEDIUM,
        language="pyspark",
        description="Window.orderBy() without .partitionBy() causes global sort",
        suggestion="Add .partitionBy() before .orderBy() or use .orderBy().limit()",
    ),
    Rule(
        id="drop_table_deprecated",
        code="BP014",
        severity=Severity.WARNING,
        confidence=Confidence.MEDIUM,
        language="all",
        description="DROP TABLE followed by CREATE can cause data loss",
        suggestion="Use CREATE OR REPLACE TABLE or TRUNCATE TABLE",
    ),
    Rule(
        id="sdp_pivot_prohibited",
        code="BP015",
        severity=Severity.ERROR,
        confidence=Confidence.HIGH,
        language="all",
        description="PIVOT clause is not supported in Spark Declarative Pipelines",
        suggestion="Use alternative transformation pattern",
    ),
    Rule(
        id="sdp_side_effects",
        code="BP016",
        severity=Severity.WARNING,
        confidence=Confidence.HIGH,
        language="pyspark",
        description="Side effects in SDP functions can cause non-deterministic behavior",
        suggestion="Remove print statements and avoid global variables",
    ),
    Rule(
        id="python_udf_builtin_replacement",
        code="BP017",
        severity=Severity.WARNING,
        confidence=Confidence.HIGH,
        language="pyspark",
        description="UDF uses a function that has a built-in Spark equivalent",
        suggestion="Replace UDF with the equivalent native Spark function",
    ),
    Rule(
        id="pandas_udf_builtin_replacement",
        code="BP018",
        severity=Severity.WARNING,
        confidence=Confidence.HIGH,
        language="pyspark",
        description="Pandas UDF uses a function that has a built-in Spark equivalent",
        suggestion="Replace Pandas UDF with the equivalent native Spark function",
    ),
    # ------------------------------------------------------------------
    # BP02x — New cost-semantic PySpark rules
    # ------------------------------------------------------------------
    Rule(
        id="cache_without_unpersist",
        code="BP020",
        severity=Severity.WARNING,
        confidence=Confidence.MEDIUM,
        language="pyspark",
        description=".cache() with no .unpersist() in the same scope — potential memory leak",
        suggestion="Call .unpersist() when the cached DataFrame is no longer needed",
    ),
    Rule(
        id="repartition_before_write",
        code="BP021",
        severity=Severity.WARNING,
        confidence=Confidence.MEDIUM,
        language="pyspark",
        description=".repartition(n) immediately before .write causes an extra shuffle",
        suggestion="Use .coalesce(n) to reduce partitions without a full shuffle",
    ),
    Rule(
        id="schema_inference_on_read",
        code="BP022",
        severity=Severity.WARNING,
        confidence=Confidence.HIGH,
        language="pyspark",
        description=".read.csv()/.json() without .schema() triggers a full scan to infer types",
        suggestion="Provide an explicit schema with .schema(StructType(...))",
    ),
    Rule(
        id="count_for_emptiness_check",
        code="BP023",
        severity=Severity.WARNING,
        confidence=Confidence.HIGH,
        language="pyspark",
        description="df.count() > 0 scans the entire DataFrame to check emptiness",
        suggestion="Use df.isEmpty or df.limit(1).count() instead",
    ),
    Rule(
        id="single_use_cache",
        code="BP024",
        severity=Severity.INFO,
        confidence=Confidence.LOW,
        language="pyspark",
        description=".cache() on a DataFrame used only once adds overhead with no benefit",
        suggestion="Remove .cache() if the DataFrame is only used in one action",
    ),
    Rule(
        id="show_left_in",
        code="BP025",
        severity=Severity.WARNING,
        confidence=Confidence.MEDIUM,
        language="pyspark",
        description=".show() in production code triggers a full plan stage",
        suggestion="Remove .show() from production code; use logging or observability tools",
    ),
    Rule(
        id="iterating_over_collect",
        code="BP026",
        severity=Severity.ERROR,
        confidence=Confidence.HIGH,
        language="pyspark",
        description="for row in df.collect() brings all data to driver and iterates row-by-row",
        suggestion="Use DataFrame transformations; avoid driver-side iteration",
    ),
    Rule(
        id="join_without_how",
        code="BP027",
        severity=Severity.INFO,
        confidence=Confidence.HIGH,
        language="pyspark",
        description=".join(df, key) without how= uses implicit inner join",
        suggestion="Add how='inner' (or the intended join type) as a keyword argument",
    ),
    Rule(
        id="non_equi_join",
        code="BP028",
        severity=Severity.WARNING,
        confidence=Confidence.MEDIUM,
        language="pyspark",
        description="Join condition with >, <, or != prevents hash join and forces sort-merge or nested loop",
        suggestion="Restructure to an equi-join where possible",
    ),
    Rule(
        id="udf_in_filter",
        code="BP029",
        severity=Severity.ERROR,
        confidence=Confidence.HIGH,
        language="pyspark",
        description=".filter(udf(...)) defeats predicate pushdown to the data source",
        suggestion="Rewrite the filter using native Spark column expressions",
    ),
    Rule(
        id="spark_sql_f_string",
        code="BP030",
        severity=Severity.ERROR,
        confidence=Confidence.HIGH,
        language="pyspark",
        description="spark.sql(f'...') or spark.sql('...'.format(...)) bypasses the plan cache and risks SQL injection",
        suggestion="Use parameterized SQL: spark.sql('SELECT * FROM {t}', t=df) or build Column expressions",
    ),
    Rule(
        id="repeated_actions_no_cache",
        code="BP031",
        severity=Severity.WARNING,
        confidence=Confidence.MEDIUM,
        language="pyspark",
        description="Same DataFrame has 2+ action calls without .cache() — plan executed multiple times",
        suggestion="Call .cache() before the first action and .unpersist() afterward",
    ),
    # ------------------------------------------------------------------
    # BQ — SQL cost/correctness rules
    # ------------------------------------------------------------------
    Rule(
        id="not_in_with_nulls",
        code="BQ001",
        severity=Severity.WARNING,
        confidence=Confidence.HIGH,
        language="sql",
        description="NOT IN (subquery) silently returns empty result when the subquery contains NULLs",
        suggestion="Use NOT EXISTS or add WHERE col IS NOT NULL to the subquery",
    ),
    Rule(
        id="union_instead_of_union_all",
        code="BQ002",
        severity=Severity.WARNING,
        confidence=Confidence.HIGH,
        language="sql",
        description="UNION without ALL forces a full dedup sort across the combined result",
        suggestion="Use UNION ALL if duplicates are acceptable; add explicit DISTINCT only when needed",
    ),
    Rule(
        id="count_distinct_at_scale",
        code="BQ003",
        severity=Severity.INFO,
        confidence=Confidence.MEDIUM,
        language="sql",
        description="COUNT(DISTINCT col) requires full shuffle and sort — expensive at scale",
        suggestion="Consider approx_count_distinct() for large datasets where exact count is not required",
    ),
    Rule(
        id="correlated_subquery",
        code="BQ004",
        severity=Severity.ERROR,
        confidence=Confidence.HIGH,
        language="sql",
        description="Correlated subquery references outer columns — Spark may execute as a nested loop join",
        suggestion="Rewrite as a join or use window functions",
    ),
    Rule(
        id="implicit_type_cast_join",
        code="BQ005",
        severity=Severity.WARNING,
        confidence=Confidence.MEDIUM,
        language="sql",
        description="Join on columns of different types forces implicit cast — prevents hash join",
        suggestion="Cast columns to the same type before joining",
    ),
    Rule(
        id="leading_wildcard_like",
        code="BQ006",
        severity=Severity.WARNING,
        confidence=Confidence.HIGH,
        language="sql",
        description="LIKE '%...' with leading wildcard prevents data skipping and index use",
        suggestion="Use full-text search or restructure to avoid leading wildcards",
    ),
    Rule(
        id="division_without_zero_guard",
        code="BQ007",
        severity=Severity.WARNING,
        confidence=Confidence.MEDIUM,
        language="sql",
        description="x / y without NULLIF guard causes runtime error on zero denominator",
        suggestion="Use NULLIF(y, 0) in the denominator: x / NULLIF(y, 0)",
    ),
    # ------------------------------------------------------------------
    # BNT-I — Import style rules
    # ------------------------------------------------------------------
    Rule(
        id="star_import_pyspark_functions",
        code="BNT-I01",
        severity=Severity.ERROR,
        confidence=Confidence.HIGH,
        language="pyspark",
        description="from pyspark.sql.functions import * shadows Python built-ins (max, min, sum, map, round)",
        suggestion="Use: from pyspark.sql import functions as F",
    ),
    Rule(
        id="non_canonical_functions_alias",
        code="BNT-I02",
        severity=Severity.WARNING,
        confidence=Confidence.HIGH,
        language="pyspark",
        description="pyspark.sql.functions imported with alias other than 'F'",
        suggestion="Use the universal convention: import pyspark.sql.functions as F",
    ),
    Rule(
        id="non_canonical_types_alias",
        code="BNT-I03",
        severity=Severity.STYLE,
        confidence=Confidence.MEDIUM,
        language="pyspark",
        description="pyspark.sql.types imported with alias other than 'T'",
        suggestion="Use: from pyspark.sql import types as T",
    ),
    # ------------------------------------------------------------------
    # BNT-C — Column reference rules
    # ------------------------------------------------------------------
    Rule(
        id="df_bracket_or_dot_reference",
        code="BNT-C01",
        severity=Severity.WARNING,
        confidence=Confidence.MEDIUM,
        language="pyspark",
        description="df['col'] or df.col outside a join can cause stale reference bugs after withColumn",
        suggestion="Use F.col('col') which resolves at evaluation time",
    ),
    Rule(
        id="selectexpr_in_production",
        code="BNT-C02",
        severity=Severity.INFO,
        confidence=Confidence.LOW,
        language="pyspark",
        description=".selectExpr() embeds logic in opaque strings that static analysis cannot inspect",
        suggestion="Prefer the Column API (.select(F.col(...))) for lintable code",
    ),
    Rule(
        id="expression_join_duplicate_cols",
        code="BNT-C03",
        severity=Severity.WARNING,
        confidence=Confidence.HIGH,
        language="pyspark",
        description=".join(other, left['key'] == right['key']) creates duplicate key columns causing AnalysisException",
        suggestion="Use string key: .join(other, 'key', how='inner') to auto-deduplicate",
    ),
    Rule(
        id="join_missing_how_keyword",
        code="BNT-C04",
        severity=Severity.WARNING,
        confidence=Confidence.HIGH,
        language="pyspark",
        description=".join() without how= keyword uses implicit inner join — intent unclear",
        suggestion="Add how='inner' (or the intended join type) as a keyword argument",
    ),
    # ------------------------------------------------------------------
    # BNT-N — Naming conventions
    # ------------------------------------------------------------------
    Rule(
        id="generic_dataframe_variable_name",
        code="BNT-N01",
        severity=Severity.STYLE,
        confidence=Confidence.HIGH,
        language="pyspark",
        description="Variable named df/df1-df9 is too generic — hinders readability",
        suggestion="Use a descriptive name: orders_df, customers, filtered_events",
    ),
    Rule(
        id="aggregation_without_alias",
        code="BNT-N02",
        severity=Severity.WARNING,
        confidence=Confidence.HIGH,
        language="pyspark",
        description="Aggregation without .alias() produces unusable auto-names like 'avg(salary)'",
        suggestion="Add .alias('avg_salary') to every aggregation expression",
    ),
    Rule(
        id="non_snake_case_alias",
        code="BNT-N03",
        severity=Severity.STYLE,
        confidence=Confidence.HIGH,
        language="pyspark",
        description="Column alias uses camelCase or special characters instead of snake_case",
        suggestion="Use snake_case aliases: .alias('first_name') not .alias('firstName')",
    ),
    Rule(
        id="with_column_renamed_prefer_alias",
        code="BNT-N04",
        severity=Severity.STYLE,
        confidence=Confidence.MEDIUM,
        language="pyspark",
        description=".withColumnRenamed() is verbose; prefer .alias() inside .select()",
        suggestion="Use df.select(F.col('old').alias('new'), ...) instead",
    ),
    # ------------------------------------------------------------------
    # BNT-M — Method chaining style
    # ------------------------------------------------------------------
    Rule(
        id="backslash_chain_continuation",
        code="BNT-M01",
        severity=Severity.WARNING,
        confidence=Confidence.HIGH,
        language="pyspark",
        description="Backslash line continuation in method chain — trailing whitespace silently breaks code",
        suggestion="Wrap the chain in parentheses and use leading dots",
    ),
    Rule(
        id="excessive_chain_length",
        code="BNT-M02",
        severity=Severity.WARNING,
        confidence=Confidence.LOW,
        language="pyspark",
        description="Method chain longer than 5 calls reduces readability",
        suggestion="Break at logical boundaries using intermediate variables",
    ),
    Rule(
        id="consecutive_with_column_chain",
        code="BNT-M03",
        severity=Severity.WARNING,
        confidence=Confidence.HIGH,
        language="pyspark",
        description="More than 3 consecutive .withColumn() calls causes O(n²) Catalyst plan analysis",
        suggestion="Use .withColumns({...}) (Spark 3.3+) or a single .select() with all expressions",
    ),
    Rule(
        id="with_column_in_loop_bnt",
        code="BNT-M04",
        severity=Severity.ERROR,
        confidence=Confidence.HIGH,
        language="pyspark",
        description=".withColumn() inside a for/while body causes O(n²) Catalyst plan bloat",
        suggestion="Use .withColumns({c: expr for c in cols}) or build all expressions first",
    ),
    # ------------------------------------------------------------------
    # BNT-S — Schema management
    # ------------------------------------------------------------------
    Rule(
        id="schema_inference_enabled",
        code="BNT-S01",
        severity=Severity.WARNING,
        confidence=Confidence.HIGH,
        language="pyspark",
        description=".option('inferSchema','true') triggers a full scan to infer types — fails silently on schema drift",
        suggestion="Provide an explicit schema with .schema(StructType(...))",
    ),
    Rule(
        id="global_auto_merge_schema",
        code="BNT-S02",
        severity=Severity.WARNING,
        confidence=Confidence.HIGH,
        language="pyspark",
        description="spark.conf.set autoMerge.enabled causes unintended schema changes across all write operations",
        suggestion="Use .option('mergeSchema','true') on individual write operations instead",
    ),
    Rule(
        id="cache_with_delta_table",
        code="BNT-S03",
        severity=Severity.WARNING,
        confidence=Confidence.MEDIUM,
        language="pyspark",
        description=".cache()/.persist() on a Delta table read is unnecessary — Delta uses disk I/O cache",
        suggestion="Remove .cache(); use Delta's native disk cache (cache-optimized node types)",
    ),
    # ------------------------------------------------------------------
    # BNT-SP — SparkSession management
    # ------------------------------------------------------------------
    Rule(
        id="sparksession_in_transform",
        code="BNT-SP1",
        severity=Severity.WARNING,
        confidence=Confidence.HIGH,
        language="pyspark",
        description="SparkSession.builder.getOrCreate() inside a transformation function couples infrastructure to logic",
        suggestion="Create SparkSession in main() or conftest.py; inject as a parameter",
    ),
    Rule(
        id="conf_set_in_transform",
        code="BNT-SP2",
        severity=Severity.WARNING,
        confidence=Confidence.MEDIUM,
        language="pyspark",
        description="spark.conf.set() inside transformation logic — configuration belongs in cluster/job settings",
        suggestion="Move conf.set() to cluster configuration, spark-submit flags, or job definitions",
    ),
    Rule(
        id="shuffle_partitions_in_code",
        code="BNT-J01",
        severity=Severity.WARNING,
        confidence=Confidence.MEDIUM,
        language="pyspark",
        description="spark.sql.shuffle.partitions set in application code — hard to tune without code changes",
        suggestion="Set shuffle partitions in cluster/job configuration or via AQE",
    ),
    # ------------------------------------------------------------------
    # BNT-T — Testing patterns
    # ------------------------------------------------------------------
    Rule(
        id="mixed_io_and_transform",
        code="BNT-T01",
        severity=Severity.WARNING,
        confidence=Confidence.MEDIUM,
        language="pyspark",
        description="Function contains both spark.read and .write — untestable without mocking I/O",
        suggestion="Separate I/O (main.py) from transformation logic (transforms/)",
    ),
    Rule(
        id="collect_comparison_in_test",
        code="BNT-T02",
        severity=Severity.WARNING,
        confidence=Confidence.HIGH,
        language="pyspark",
        description=".collect() in assert/comparison in tests is fragile — order-dependent, misses type mismatches",
        suggestion="Use chispa.assert_frame_equal() or pyspark.testing.assertDataFrameEqual()",
    ),
    Rule(
        id="sparksession_per_test_function",
        code="BNT-T03",
        severity=Severity.WARNING,
        confidence=Confidence.HIGH,
        language="pyspark",
        description="SparkSession.builder inside a test_* function creates a 5-15s session per test",
        suggestion="Use @pytest.fixture(scope='session') for SparkSession in conftest.py",
    ),
    # ------------------------------------------------------------------
    # BNT-Q — SQL vs DataFrame API
    # ------------------------------------------------------------------
    Rule(
        id="spark_sql_fstring_injection",
        code="BNT-Q01",
        severity=Severity.ERROR,
        confidence=Confidence.HIGH,
        language="pyspark",
        description="spark.sql(f'...') bypasses the plan cache and risks SQL injection",
        suggestion="Use parameterized SQL: spark.sql('SELECT * FROM {t}', t=df)",
    ),
    Rule(
        id="create_temp_view_in_production",
        code="BNT-Q02",
        severity=Severity.WARNING,
        confidence=Confidence.MEDIUM,
        language="pyspark",
        description="createOrReplaceTempView introduces session-global mutable state that breaks test isolation",
        suggestion="Use Spark 3.4+ parameterized SQL or pass DataFrames directly",
    ),
    # ------------------------------------------------------------------
    # BNT-W — Window function correctness
    # ------------------------------------------------------------------
    Rule(
        id="window_missing_frame_spec",
        code="BNT-W01",
        severity=Severity.WARNING,
        confidence=Confidence.HIGH,
        language="pyspark",
        description="Window.orderBy() without .rowsBetween()/.rangeBetween() uses default frame — silent correctness difference",
        suggestion="Always specify .rowsBetween(Window.unboundedPreceding, Window.currentRow) explicitly",
    ),
    Rule(
        id="window_empty_partition",
        code="BNT-W02",
        severity=Severity.WARNING,
        confidence=Confidence.MEDIUM,
        language="pyspark",
        description="Window.orderBy() without .partitionBy() causes global sort — all data on one executor",
        suggestion="Add .partitionBy() or confirm global ordering is intentional",
    ),
    Rule(
        id="first_last_without_ignorenulls",
        code="BNT-W03",
        severity=Severity.WARNING,
        confidence=Confidence.HIGH,
        language="pyspark",
        description="F.first()/F.last() without ignorenulls=True has inconsistent behavior across Spark versions",
        suggestion="Use F.first(col, ignorenulls=True) or F.last(col, ignorenulls=True)",
    ),
    # ------------------------------------------------------------------
    # BNT-L — Null and join style
    # ------------------------------------------------------------------
    Rule(
        id="empty_string_instead_of_null",
        code="BNT-L01",
        severity=Severity.WARNING,
        confidence=Confidence.MEDIUM,
        language="pyspark",
        description="F.lit('') or F.lit('NA') used instead of null breaks IS NULL checks and aggregations",
        suggestion="Use F.lit(None).cast(t) to represent missing values",
    ),
    Rule(
        id="right_join_prefer_left",
        code="BNT-L02",
        severity=Severity.STYLE,
        confidence=Confidence.LOW,
        language="pyspark",
        description="Right joins make it harder to reason about which DataFrame drives the result",
        suggestion="Swap the DataFrames and use a left join instead",
    ),
    # ------------------------------------------------------------------
    # BNT-D — Databricks platform guidelines
    # ------------------------------------------------------------------
    Rule(
        id="missing_3level_namespace",
        code="BNT-D01",
        severity=Severity.WARNING,
        confidence=Confidence.MEDIUM,
        language="all",
        description="Table reference missing Unity Catalog three-level namespace (catalog.schema.table)",
        suggestion="Use full catalog.schema.table references for Unity Catalog compatibility",
    ),
    Rule(
        id="hardcoded_catalog_schema_name",
        code="BNT-D02",
        severity=Severity.WARNING,
        confidence=Confidence.MEDIUM,
        language="pyspark",
        description="Hardcoded catalog/schema name in spark.table() — breaks across environments",
        suggestion="Define catalog/schema as pipeline configuration parameters, not code literals",
    ),
    Rule(
        id="path_based_read_prefer_table",
        code="BNT-D03",
        severity=Severity.INFO,
        confidence=Confidence.LOW,
        language="pyspark",
        description="Path-based Delta read bypasses Unity Catalog access control and table statistics",
        suggestion="Use spark.table('catalog.schema.table') or spark.read.table() instead",
    ),
    Rule(
        id="python_udf_native_exists",
        code="BNT-D04",
        severity=Severity.WARNING,
        confidence=Confidence.HIGH,
        language="pyspark",
        description="Python UDF wraps an operation that has a native Spark function equivalent",
        suggestion="Use the native Spark function — avoids serialization and enables Photon acceleration",
    ),
    Rule(
        id="debug_call_in_production",
        code="BNT-D05",
        severity=Severity.WARNING,
        confidence=Confidence.HIGH,
        language="pyspark",
        description=".display(), .show(), or .count() left in production code triggers a plan stage",
        suggestion="Remove debug calls before committing; use structured logging",
    ),
    # ------------------------------------------------------------------
    # BNT-P — Streaming patterns
    # ------------------------------------------------------------------
    Rule(
        id="streaming_await_termination",
        code="BNT-P01",
        severity=Severity.WARNING,
        confidence=Confidence.HIGH,
        language="pyspark",
        description=".awaitTermination() is unnecessary in Databricks Jobs — the job layer manages the query lifecycle",
        suggestion="Remove .awaitTermination() from Databricks Job notebooks",
    ),
    # ------------------------------------------------------------------
    # BNT-A — Type annotations
    # ------------------------------------------------------------------
    Rule(
        id="missing_dataframe_type_annotation",
        code="BNT-A01",
        severity=Severity.WARNING,
        confidence=Confidence.MEDIUM,
        language="pyspark",
        description="Transformation function missing DataFrame type annotation — reduces IDE support",
        suggestion="Add parameter and return type annotations: def transform(df: DataFrame) -> DataFrame",
    ),
    Rule(
        id="missing_transform_docstring",
        code="BNT-A02",
        severity=Severity.INFO,
        confidence=Confidence.LOW,
        language="pyspark",
        description="Transformation function missing docstring",
        suggestion="Add a one-line docstring explaining the purpose and key transformations",
    ),
]

REGISTRY: dict[str, Rule] = {rule.id: rule for rule in _ALL_RULES}
