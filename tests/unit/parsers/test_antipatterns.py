from burnt.parsers.antipatterns import (
    AntiPattern,
    Severity,
    _detect_pyspark_antipatterns,
    _detect_sql_antipatterns,
    detect_antipatterns,
)


class TestDetectAntiPatterns:
    def test_detect_sql_antipatterns(self):
        patterns = detect_antipatterns("SELECT * FROM users", "sql")
        assert isinstance(patterns, list)

    def test_detect_pyspark_antipatterns(self):
        patterns = detect_antipatterns("df.collect()", "pyspark")
        assert isinstance(patterns, list)

    def test_detect_unsupported_language(self):
        patterns = detect_antipatterns("some code", "scala")
        assert patterns == []


class TestDetectSqlAntiPatterns:
    def test_cross_join_detection(self):
        sql = "SELECT * FROM a CROSS JOIN b"
        patterns = _detect_sql_antipatterns(sql)
        assert any(p.name == "cross_join" for p in patterns)

    def test_select_star_no_limit(self):
        sql = "SELECT * FROM users"
        patterns = _detect_sql_antipatterns(sql)
        assert any(p.name == "select_star" for p in patterns)

    def test_select_star_with_limit(self):
        sql = "SELECT * FROM users LIMIT 10"
        patterns = _detect_sql_antipatterns(sql)
        assert not any(p.name == "select_star" for p in patterns)

    def test_order_by_no_limit(self):
        sql = "SELECT * FROM users ORDER BY created_at"
        patterns = _detect_sql_antipatterns(sql)
        assert any(p.name == "order_by_no_limit" for p in patterns)

    def test_order_by_with_limit(self):
        sql = "SELECT * FROM users ORDER BY created_at LIMIT 10"
        patterns = _detect_sql_antipatterns(sql)
        assert not any(p.name == "order_by_no_limit" for p in patterns)

    def test_drop_table_deprecated(self):
        sql = "DROP TABLE my_table"
        patterns = _detect_sql_antipatterns(sql)
        assert any(p.name == "drop_table_deprecated" for p in patterns)

    def test_sdp_pivot_prohibited(self):
        sql = """
        SELECT * FROM (
            SELECT year, product, amount
            FROM sales
        )
        PIVOT (
            SUM(amount) FOR year IN (2021, 2022, 2023)
        )
        """
        patterns = _detect_sql_antipatterns(sql)
        assert any(p.name == "sdp_pivot_prohibited" for p in patterns)


class TestDetectPySparkAntiPatterns:
    def test_collect_without_limit(self):
        code = "results = df.collect()"
        patterns = _detect_pyspark_antipatterns(code)
        assert any(p.name == "collect_without_limit" for p in patterns)

    def test_collect_with_limit(self):
        code = "results = df.limit(100).collect()"
        patterns = _detect_pyspark_antipatterns(code)
        assert not any(p.name == "collect_without_limit" for p in patterns)

    def test_collect_with_take(self):
        # .take() is itself a safe bounded collection — collect() not involved
        code = "results = df.take(10)"
        patterns = _detect_pyspark_antipatterns(code)
        assert not any(p.name == "collect_without_limit" for p in patterns)

    def test_collect_with_filter_and_limit(self):
        code = "results = df.filter(col('active')).limit(50).collect()"
        patterns = _detect_pyspark_antipatterns(code)
        assert not any(p.name == "collect_without_limit" for p in patterns)

    def test_python_udf(self):
        code = """
@udf
def my_func(x):
    return x * 2
"""
        patterns = _detect_pyspark_antipatterns(code)
        assert any(p.name == "python_udf" for p in patterns)

    def test_pandas_udf(self):
        code = """
@pandas_udf
def my_func(x):
    return x * 2
"""
        patterns = _detect_pyspark_antipatterns(code)
        assert not any(p.name == "python_udf" for p in patterns)
        # Generic pandas_udf warning fires when body has no specific builtin replacement
        assert any(p.name == "pandas_udf" for p in patterns)

    def test_pandas_udf_builtin_suppresses_generic(self):
        # When a specific builtin replacement is found, the generic pandas_udf
        # warning is suppressed in favour of the more actionable rule
        code = """
@pandas_udf
def uppercase_col(s):
    return s.upper()
"""
        patterns = _detect_pyspark_antipatterns(code)
        assert any(p.name == "pandas_udf_builtin_replacement" for p in patterns)
        assert not any(p.name == "pandas_udf" for p in patterns)

    def test_count_without_filter(self):
        code = "n = df.count()"
        patterns = _detect_pyspark_antipatterns(code)
        assert any(p.name == "count_without_filter" for p in patterns)

    def test_count_with_filter(self):
        code = "n = df.filter(col('active') == True).count()"
        patterns = _detect_pyspark_antipatterns(code)
        assert not any(p.name == "count_without_filter" for p in patterns)

    def test_count_with_where(self):
        code = "n = df.where(col('status') == 'active').count()"
        patterns = _detect_pyspark_antipatterns(code)
        assert not any(p.name == "count_without_filter" for p in patterns)

    def test_count_groupby(self):
        # groupBy().count() is an aggregation, not a full scan count
        code = "df.groupBy('department').count()"
        patterns = _detect_pyspark_antipatterns(code)
        assert not any(p.name == "count_without_filter" for p in patterns)

    def test_repartition_one(self):
        code = "df.repartition(1).write.parquet('output')"
        patterns = _detect_pyspark_antipatterns(code)
        assert any(p.name == "repartition_one" for p in patterns)

    def test_toPandas(self):
        code = "pandas_df = df.toPandas()"
        patterns = _detect_pyspark_antipatterns(code)
        assert any(p.name == "toPandas" for p in patterns)

    def test_withColumn_in_loop(self):
        code = """
for i in range(10):
    df = df.withColumn(f'col_{i}', lit(i))
"""
        patterns = _detect_pyspark_antipatterns(code)
        assert any(p.name == "withColumn_in_loop" for p in patterns)

    def test_withColumn_outside_loop(self):
        code = "df = df.withColumn('new_col', col('old_col') + 1)"
        patterns = _detect_pyspark_antipatterns(code)
        assert not any(p.name == "withColumn_in_loop" for p in patterns)

    def test_sdp_prohibited_ops_collect(self):
        code = """
@dp.table
def my_table():
    return spark.table('source').collect()
"""
        patterns = _detect_pyspark_antipatterns(code)
        assert any(p.name == "sdp_prohibited_ops" for p in patterns)

    def test_sdp_prohibited_ops_toPandas(self):
        code = """
@dp.materialized_view
def my_view():
    return spark.table('source').toPandas()
"""
        patterns = _detect_pyspark_antipatterns(code)
        assert any(p.name == "sdp_prohibited_ops" for p in patterns)

    def test_sdp_prohibited_ops_count(self):
        code = """
@dp.temporary_view
def my_temp_view():
    return spark.table('source').count()
"""
        patterns = _detect_pyspark_antipatterns(code)
        assert any(p.name == "sdp_prohibited_ops" for p in patterns)

    def test_window_without_partition_by(self):
        code = "window = Window.orderBy('timestamp')"
        patterns = _detect_pyspark_antipatterns(code)
        assert any(p.name == "window_without_partition_by" for p in patterns)

    def test_python_udf_builtin_replacement(self):
        code = """
@udf
def uppercase_string(s):
    return s.upper()
"""
        patterns = _detect_pyspark_antipatterns(code)
        assert any(p.name == "python_udf_builtin_replacement" for p in patterns)

    def test_pandas_udf_builtin_replacement(self):
        code = """
@pandas_udf
def lowercase_series(s):
    return s.lower()
"""
        patterns = _detect_pyspark_antipatterns(code)
        assert any(p.name == "pandas_udf_builtin_replacement" for p in patterns)


class TestAntiPattern:
    def test_antipattern_creation(self):
        pattern = AntiPattern(
            name="test_pattern",
            severity=Severity.WARNING,
            description="Test description",
            suggestion="Test suggestion",
        )
        assert pattern.name == "test_pattern"
        assert pattern.severity == Severity.WARNING


class TestSeverity:
    def test_severity_values(self):
        assert Severity.ERROR == "error"
        assert Severity.WARNING == "warning"
        assert Severity.INFO == "info"
