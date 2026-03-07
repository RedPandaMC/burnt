from dburnrate.parsers.antipatterns import (
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
        assert any(p.name == "select_star_no_limit" for p in patterns)

    def test_select_star_with_limit(self):
        sql = "SELECT * FROM users LIMIT 10"
        patterns = _detect_sql_antipatterns(sql)
        assert not any(p.name == "select_star_no_limit" for p in patterns)

    def test_order_by_no_limit(self):
        sql = "SELECT * FROM users ORDER BY created_at"
        patterns = _detect_sql_antipatterns(sql)
        assert any(p.name == "order_by_no_limit" for p in patterns)

    def test_order_by_with_limit(self):
        sql = "SELECT * FROM users ORDER BY created_at LIMIT 10"
        patterns = _detect_sql_antipatterns(sql)
        assert not any(p.name == "order_by_no_limit" for p in patterns)


class TestDetectPySparkAntiPatterns:
    def test_collect_without_limit(self):
        code = "results = df.collect()"
        patterns = _detect_pyspark_antipatterns(code)
        assert any(p.name == "collect_without_limit" for p in patterns)

    def test_collect_with_limit(self):
        code = "results = df.limit(100).collect()"
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

    def test_repartition_one(self):
        code = "df.repartition(1).write.parquet('output')"
        patterns = _detect_pyspark_antipatterns(code)
        assert any(p.name == "repartition_one" for p in patterns)

    def test_toPandas(self):
        code = "pandas_df = df.toPandas()"
        patterns = _detect_pyspark_antipatterns(code)
        assert any(p.name == "toPandas" for p in patterns)


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
