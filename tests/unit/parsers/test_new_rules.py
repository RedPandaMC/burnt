"""Tests for Phase 1-4 new rules and bug fixes.

Covers:
- Phase 1: correctness bug fixes (JDBC, window state, false positives, silent rules)
- Phase 2: registry, inline suppression, P1 style rules
- Phase 3: BP020-BP031 cost rules, BQ001-BQ007 SQL rules
- Phase 4: remaining BNT-* style rules
"""

from burnt.parsers.antipatterns import (
    _detect_pyspark_antipatterns,
    _detect_sql_antipatterns,
)
from burnt.parsers.registry import REGISTRY, Confidence, Rule, Severity

# ===========================================================================
# Phase 1 — Correctness Bug Fixes
# ===========================================================================


class TestMissingSeveritiesNowFire:
    """Rules that were previously emitted but silently dropped now reach the output."""

    def test_drop_table_deprecated_fires_sql(self):
        sql = "DROP TABLE my_table"
        patterns = _detect_sql_antipatterns(sql)
        assert any(p.name == "drop_table_deprecated" for p in patterns)

    def test_sdp_pivot_prohibited_fires_sql(self):
        sql = "SELECT * FROM (SELECT year, product, amount FROM sales) PIVOT (SUM(amount) FOR year IN (2021, 2022))"
        patterns = _detect_sql_antipatterns(sql)
        assert any(p.name == "sdp_pivot_prohibited" for p in patterns)

    def test_window_without_partition_by_fires(self):
        code = "window = Window.orderBy('timestamp')"
        patterns = _detect_pyspark_antipatterns(code)
        assert any(p.name == "window_without_partition_by" for p in patterns)

    def test_python_udf_builtin_replacement_fires(self):
        code = """
@udf
def uppercase_string(s):
    return s.upper()
"""
        patterns = _detect_pyspark_antipatterns(code)
        assert any(p.name == "python_udf_builtin_replacement" for p in patterns)

    def test_pandas_udf_builtin_replacement_fires(self):
        code = """
@pandas_udf
def lowercase_series(s):
    return s.lower()
"""
        patterns = _detect_pyspark_antipatterns(code)
        assert any(p.name == "pandas_udf_builtin_replacement" for p in patterns)

    def test_sdp_side_effects_print_fires(self):
        code = """
@dp.table
def my_table():
    print("debug")
    return spark.table('src')
"""
        patterns = _detect_pyspark_antipatterns(code)
        assert any(p.name == "sdp_side_effects" for p in patterns)


class TestJdbcDetectionFix:
    """JDBC rule now fires correctly on multi-line chains."""

    def test_jdbc_multiline_chain_fires(self):
        code = """
df = (
    spark.read
    .format("jdbc")
    .option("url", "jdbc:postgresql://host/db")
    .load()
)
"""
        patterns = _detect_pyspark_antipatterns(code)
        assert any(p.name == "jdbc_incomplete_partition" for p in patterns)

    def test_jdbc_with_all_options_no_fire(self):
        code = """
df = (
    spark.read
    .format("jdbc")
    .option("url", "jdbc:postgresql://host/db")
    .option("partitionColumn", "id")
    .option("numPartitions", "10")
    .option("lowerBound", "1")
    .option("upperBound", "10000")
    .load()
)
"""
        patterns = _detect_pyspark_antipatterns(code)
        assert not any(p.name == "jdbc_incomplete_partition" for p in patterns)

    def test_jdbc_single_line_chain_fires(self):
        code = 'df = spark.read.format("jdbc").option("url", "jdbc:...").load()'
        patterns = _detect_pyspark_antipatterns(code)
        assert any(p.name == "jdbc_incomplete_partition" for p in patterns)


class TestWindowStateBleedingFix:
    """Window state no longer bleeds across multiple Window specs."""

    def test_two_windows_first_partitioned_second_not(self):
        code = """
w1 = Window.partitionBy('dept').orderBy('salary')
w2 = Window.orderBy('date')
"""
        patterns = _detect_pyspark_antipatterns(code)
        # w2 has no partitionBy — should fire
        # w1 has partitionBy — should NOT fire
        window_patterns = [p for p in patterns if p.name == "window_without_partition_by"]
        assert len(window_patterns) == 1

    def test_window_with_partition_no_fire(self):
        code = "w = Window.partitionBy('dept').orderBy('salary')"
        patterns = _detect_pyspark_antipatterns(code)
        assert not any(p.name == "window_without_partition_by" for p in patterns)

    def test_dataframe_orderby_no_false_positive(self):
        """df.orderBy() must NOT trigger window_without_partition_by."""
        code = "result = df.orderBy('date')"
        patterns = _detect_pyspark_antipatterns(code)
        assert not any(p.name == "window_without_partition_by" for p in patterns)


class TestFalsePositiveStringMatchFix:
    """String-based detection replaced with AST detection — no more comment false positives."""

    def test_drop_table_in_comment_no_fire(self):
        code = "# DROP TABLE not supported here\ndf = spark.table('foo')"
        patterns = _detect_pyspark_antipatterns(code)
        assert not any(p.name == "drop_table_deprecated" for p in patterns)

    def test_pivot_in_comment_no_fire(self):
        code = "# PIVOT is expensive\ndf = spark.table('foo')"
        patterns = _detect_pyspark_antipatterns(code)
        assert not any(p.name == "sdp_pivot_prohibited" for p in patterns)

    def test_print_outside_sdp_no_fire(self):
        code = "print('hello world')"
        patterns = _detect_pyspark_antipatterns(code)
        assert not any(p.name == "sdp_side_effects" for p in patterns)

    def test_sdp_side_effects_global_fires(self):
        code = """
@dp.table
def my_table():
    global state
    return spark.table('src')
"""
        patterns = _detect_pyspark_antipatterns(code)
        assert any(p.name == "sdp_side_effects" for p in patterns)


# ===========================================================================
# Phase 2 — Registry, Inline Suppression, P1 Style Rules
# ===========================================================================


class TestRegistry:
    def test_all_original_rules_present(self):
        original_rules = [
            "collect_without_limit", "select_star", "python_udf", "toPandas",
            "repartition_one", "order_by_no_limit", "cross_join", "pandas_udf",
            "count_without_filter", "withColumn_in_loop", "jdbc_incomplete_partition",
            "sdp_prohibited_ops",
        ]
        for rule_id in original_rules:
            assert rule_id in REGISTRY, f"Missing rule: {rule_id}"

    def test_new_rules_present(self):
        new_rules = [
            "drop_table_deprecated", "sdp_pivot_prohibited", "sdp_side_effects",
            "window_without_partition_by", "python_udf_builtin_replacement",
            "pandas_udf_builtin_replacement",
        ]
        for rule_id in new_rules:
            assert rule_id in REGISTRY, f"Missing rule: {rule_id}"

    def test_bq_rules_present(self):
        bq_rules = [
            "not_in_with_nulls", "union_instead_of_union_all", "count_distinct_at_scale",
            "leading_wildcard_like", "division_without_zero_guard",
        ]
        for rule_id in bq_rules:
            assert rule_id in REGISTRY, f"Missing BQ rule: {rule_id}"

    def test_bnt_rules_present(self):
        bnt_rules = [
            "star_import_pyspark_functions", "expression_join_duplicate_cols",
            "aggregation_without_alias", "window_missing_frame_spec",
        ]
        for rule_id in bnt_rules:
            assert rule_id in REGISTRY, f"Missing BNT rule: {rule_id}"

    def test_rule_has_required_fields(self):
        for rule_id, rule in REGISTRY.items():
            assert isinstance(rule, Rule)
            assert rule.id == rule_id
            assert rule.code
            assert rule.severity in Severity
            assert rule.confidence in Confidence
            assert rule.language in {"sql", "pyspark", "all"}
            assert rule.description
            assert rule.suggestion

    def test_all_codes_unique(self):
        codes = [r.code for r in REGISTRY.values()]
        assert len(codes) == len(set(codes)), "Duplicate rule codes found"


class TestInlineSuppression:
    def test_suppression_on_same_line(self):
        code = "df.crossJoin(date_spine)  # burnt: ignore[cross_join]"
        patterns = _detect_pyspark_antipatterns(code)
        assert not any(p.name == "cross_join" for p in patterns)

    def test_suppression_only_for_named_rule(self):
        code = "results = df.collect()  # burnt: ignore[cross_join]"
        patterns = _detect_pyspark_antipatterns(code)
        # collect_without_limit should still fire — suppression is for cross_join only
        assert any(p.name == "collect_without_limit" for p in patterns)

    def test_no_suppression_fires_normally(self):
        code = "results = df.collect()"
        patterns = _detect_pyspark_antipatterns(code)
        assert any(p.name == "collect_without_limit" for p in patterns)

    def test_multiple_rules_suppressed_on_same_line(self):
        code = "df.toPandas()  # burnt: ignore[toPandas, collect_without_limit]"
        patterns = _detect_pyspark_antipatterns(code)
        assert not any(p.name == "toPandas" for p in patterns)


class TestP1StyleRules:
    def test_star_import_fires(self):
        code = "from pyspark.sql.functions import *"
        patterns = _detect_pyspark_antipatterns(code)
        assert any(p.name == "star_import_pyspark_functions" for p in patterns)

    def test_canonical_import_no_fire(self):
        code = "from pyspark.sql import functions as F"
        patterns = _detect_pyspark_antipatterns(code)
        assert not any(p.name == "star_import_pyspark_functions" for p in patterns)

    def test_non_canonical_alias_fires(self):
        code = "from pyspark.sql import functions as func"
        patterns = _detect_pyspark_antipatterns(code)
        assert any(p.name == "non_canonical_functions_alias" for p in patterns)

    def test_canonical_f_alias_no_fire(self):
        code = "from pyspark.sql import functions as F"
        patterns = _detect_pyspark_antipatterns(code)
        assert not any(p.name == "non_canonical_functions_alias" for p in patterns)

    def test_expression_join_fires(self):
        code = "result = orders.join(users, orders['user_id'] == users['user_id'])"
        patterns = _detect_pyspark_antipatterns(code)
        assert any(p.name == "expression_join_duplicate_cols" for p in patterns)

    def test_string_join_no_fire(self):
        code = "result = orders.join(users, 'user_id', how='inner')"
        patterns = _detect_pyspark_antipatterns(code)
        assert not any(p.name == "expression_join_duplicate_cols" for p in patterns)

    def test_join_missing_how_fires(self):
        code = "result = orders.join(users, 'user_id')"
        patterns = _detect_pyspark_antipatterns(code)
        assert any(p.name == "join_missing_how_keyword" for p in patterns)

    def test_join_with_how_no_fire(self):
        code = "result = orders.join(users, 'user_id', how='inner')"
        patterns = _detect_pyspark_antipatterns(code)
        assert not any(p.name == "join_missing_how_keyword" for p in patterns)


# ===========================================================================
# Phase 3 — New Cost-Semantic Rules (BP + BQ)
# ===========================================================================


class TestNewPySparkCostRules:
    def test_show_left_in_fires(self):
        code = "df.show()"
        patterns = _detect_pyspark_antipatterns(code)
        assert any(p.name == "show_left_in" for p in patterns)

    def test_iterating_over_collect_fires(self):
        code = """
for row in df.collect():
    process(row)
"""
        patterns = _detect_pyspark_antipatterns(code)
        assert any(p.name == "iterating_over_collect" for p in patterns)

    def test_iterating_non_collect_no_fire(self):
        code = """
for item in items:
    process(item)
"""
        patterns = _detect_pyspark_antipatterns(code)
        assert not any(p.name == "iterating_over_collect" for p in patterns)

    def test_spark_sql_fstring_fires(self):
        code = 'spark.sql(f"SELECT * FROM {table}")'
        patterns = _detect_pyspark_antipatterns(code)
        assert any(p.name == "spark_sql_f_string" for p in patterns)

    def test_spark_sql_literal_no_fire(self):
        code = "spark.sql('SELECT * FROM my_table')"
        patterns = _detect_pyspark_antipatterns(code)
        assert not any(p.name == "spark_sql_f_string" for p in patterns)

    def test_schema_inference_option_fires(self):
        code = 'df = spark.read.option("inferSchema", "true").csv("path")'
        patterns = _detect_pyspark_antipatterns(code)
        assert any(p.name == "schema_inference_enabled" for p in patterns)

    def test_schema_inference_on_read_csv_fires(self):
        code = 'df = spark.read.csv("path")'
        patterns = _detect_pyspark_antipatterns(code)
        assert any(p.name == "schema_inference_on_read" for p in patterns)

    def test_schema_inference_on_read_with_schema_no_fire(self):
        code = 'df = spark.read.schema(my_schema).csv("path")'
        patterns = _detect_pyspark_antipatterns(code)
        assert not any(p.name == "schema_inference_on_read" for p in patterns)

    def test_aggregation_without_alias_fires(self):
        code = "df.groupBy('dept').agg(F.avg('salary'))"
        patterns = _detect_pyspark_antipatterns(code)
        assert any(p.name == "aggregation_without_alias" for p in patterns)

    def test_window_missing_frame_spec_fires(self):
        code = "w = Window.orderBy('date')"
        patterns = _detect_pyspark_antipatterns(code)
        assert any(p.name == "window_missing_frame_spec" for p in patterns)

    def test_first_without_ignorenulls_fires(self):
        code = "F.first('col')"
        patterns = _detect_pyspark_antipatterns(code)
        assert any(p.name == "first_last_without_ignorenulls" for p in patterns)

    def test_first_with_ignorenulls_no_fire(self):
        code = "F.first('col', ignorenulls=True)"
        patterns = _detect_pyspark_antipatterns(code)
        assert not any(p.name == "first_last_without_ignorenulls" for p in patterns)


class TestNewSQLCostRules:
    def test_union_without_all_fires(self):
        sql = "SELECT id FROM a UNION SELECT id FROM b"
        patterns = _detect_sql_antipatterns(sql)
        assert any(p.name == "union_instead_of_union_all" for p in patterns)

    def test_union_all_no_fire(self):
        sql = "SELECT id FROM a UNION ALL SELECT id FROM b"
        patterns = _detect_sql_antipatterns(sql)
        assert not any(p.name == "union_instead_of_union_all" for p in patterns)

    def test_count_distinct_fires(self):
        sql = "SELECT COUNT(DISTINCT user_id) FROM events"
        patterns = _detect_sql_antipatterns(sql)
        assert any(p.name == "count_distinct_at_scale" for p in patterns)

    def test_leading_wildcard_like_fires(self):
        sql = "SELECT * FROM users WHERE name LIKE '%smith'"
        patterns = _detect_sql_antipatterns(sql)
        assert any(p.name == "leading_wildcard_like" for p in patterns)

    def test_trailing_wildcard_like_no_fire(self):
        sql = "SELECT * FROM users WHERE name LIKE 'smith%'"
        patterns = _detect_sql_antipatterns(sql)
        assert not any(p.name == "leading_wildcard_like" for p in patterns)

    def test_division_without_zero_guard_fires(self):
        sql = "SELECT revenue / costs FROM metrics"
        patterns = _detect_sql_antipatterns(sql)
        assert any(p.name == "division_without_zero_guard" for p in patterns)

    def test_division_literal_denominator_no_fire(self):
        sql = "SELECT revenue / 100 FROM metrics"
        patterns = _detect_sql_antipatterns(sql)
        assert not any(p.name == "division_without_zero_guard" for p in patterns)


# ===========================================================================
# Phase 4 — Remaining BNT-* Style Rules
# ===========================================================================


class TestRemainingStyleRules:
    def test_selectexpr_fires(self):
        code = "df.selectExpr('col + 1 as result')"
        patterns = _detect_pyspark_antipatterns(code)
        assert any(p.name == "selectexpr_in_production" for p in patterns)

    def test_with_column_renamed_fires(self):
        code = "df.withColumnRenamed('old_name', 'new_name')"
        patterns = _detect_pyspark_antipatterns(code)
        assert any(p.name == "with_column_renamed_prefer_alias" for p in patterns)

    def test_consecutive_withcolumn_chain_fires(self):
        code = "df.withColumn('a', F.lit(1)).withColumn('b', F.lit(2)).withColumn('c', F.lit(3)).withColumn('d', F.lit(4))"
        patterns = _detect_pyspark_antipatterns(code)
        assert any(p.name == "consecutive_with_column_chain" for p in patterns)

    def test_short_withcolumn_chain_no_fire(self):
        code = "df.withColumn('a', F.lit(1)).withColumn('b', F.lit(2))"
        patterns = _detect_pyspark_antipatterns(code)
        assert not any(p.name == "consecutive_with_column_chain" for p in patterns)

    def test_create_temp_view_fires(self):
        code = "df.createOrReplaceTempView('my_view')"
        patterns = _detect_pyspark_antipatterns(code)
        assert any(p.name == "create_temp_view_in_production" for p in patterns)

    def test_empty_string_lit_fires(self):
        code = "df.withColumn('x', F.lit(''))"
        patterns = _detect_pyspark_antipatterns(code)
        assert any(p.name == "empty_string_instead_of_null" for p in patterns)

    def test_lit_none_no_fire(self):
        code = "df.withColumn('x', F.lit(None))"
        patterns = _detect_pyspark_antipatterns(code)
        assert not any(p.name == "empty_string_instead_of_null" for p in patterns)

    def test_shuffle_partitions_in_code_fires(self):
        code = 'spark.conf.set("spark.sql.shuffle.partitions", "200")'
        patterns = _detect_pyspark_antipatterns(code)
        assert any(p.name == "shuffle_partitions_in_code" for p in patterns)

    def test_await_termination_fires(self):
        code = "query.awaitTermination()"
        patterns = _detect_pyspark_antipatterns(code)
        assert any(p.name == "streaming_await_termination" for p in patterns)

    def test_global_auto_merge_schema_fires(self):
        code = 'spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")'
        patterns = _detect_pyspark_antipatterns(code)
        assert any(p.name == "global_auto_merge_schema" for p in patterns)
