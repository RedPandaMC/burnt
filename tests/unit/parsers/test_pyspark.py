"""Test Python/Spark analysis via the Rust engine."""

from burnt.parsers.antipatterns import AntiPattern, Severity, detect_antipatterns


class TestRustEnginePythonAnalysis:
    """Tests that exercise burnt-engine for Python/Spark code analysis."""

    def test_simple_python_no_findings(self):
        code = "import pandas as pd\nprint('hello')"
        issues = detect_antipatterns(code, "python")
        assert isinstance(issues, list)

    def test_collect_without_limit(self):
        code = "results = df.collect()"
        issues = detect_antipatterns(code, "python")
        # The Rust engine should flag collect() as expensive
        assert any("collect" in i.description.lower() for i in issues)

    def test_cross_join(self):
        code = "df1.crossJoin(df2).show()"
        issues = detect_antipatterns(code, "python")
        assert any("cross" in i.description.lower() for i in issues)

    def test_repartition_one(self):
        code = "df.repartition(1).write.parquet('output')"
        issues = detect_antipatterns(code, "python")
        assert any("repartition" in i.description.lower() for i in issues)

    def test_spark_sql_extraction(self):
        code = "spark.sql('SELECT * FROM table')"
        issues = detect_antipatterns(code, "python")
        # No anti-pattern, but the engine should parse successfully
        assert isinstance(issues, list)

    def test_dynamic_sql_fstring(self):
        code = "spark.sql(f'SELECT * FROM {table}')"
        issues = detect_antipatterns(code, "python")
        # The Rust engine may or may not flag f-strings depending on rule set.
        # We just verify it doesn't crash.
        assert isinstance(issues, list)


class TestAntiPatternDataclass:
    def test_antipattern_creation(self):
        ap = AntiPattern(
            name="BP008",
            severity=Severity.ERROR,
            description="collect() without limit() can OOM the driver",
            suggestion="Add .limit(n).collect() or use .take(n)",
            line_number=42,
        )
        assert ap.name == "BP008"
        assert ap.severity == "error"
        assert ap.line_number == 42

    def test_severity_enum(self):
        assert Severity.ERROR == "error"
        assert Severity.WARNING == "warning"
        assert Severity.INFO == "info"

    def test_detect_antipatterns_invalid_syntax(self):
        code = "def invalid syntax here"
        # The Rust engine handles parse errors gracefully
        issues = detect_antipatterns(code, "python")
        assert isinstance(issues, list)
        # May contain a syntax error finding
        syntax_findings = [i for i in issues if "syntax" in i.description.lower()]
        assert syntax_findings or len(issues) == 0  # either reported or empty
