import pytest

from burnt.core.exceptions import ParseError
from burnt.parsers.pyspark import PYSPARK_WEIGHTS, PySparkVisitor, analyze_pyspark


class TestAnalyzePySpark:
    def test_analyze_pyspark_simple(self):
        code = "df.select('name').show()"
        ops, _ = analyze_pyspark(code)
        assert len(ops) == 0

    def test_analyze_pyspark_groupby(self):
        code = "df.groupBy('dept').count().show()"
        ops, _ = analyze_pyspark(code)
        assert any(op.name == "groupBy" for op in ops)

    def test_analyze_pyspark_join(self):
        code = "df1.join(df2, 'id').show()"
        ops, _ = analyze_pyspark(code)
        assert any(op.name == "join" for op in ops)

    def test_analyze_pyspark_crossjoin(self):
        code = "df1.crossJoin(df2).show()"
        ops, _ = analyze_pyspark(code)
        assert any(op.name == "crossJoin" for op in ops)

    def test_analyze_pyspark_collect(self):
        code = "results = df.collect()"
        ops, _ = analyze_pyspark(code)
        assert any(op.name == "collect" for op in ops)

    def test_analyze_pyspark_toPandas(self):
        code = "pandas_df = df.toPandas()"
        ops, _ = analyze_pyspark(code)
        assert any(op.name == "toPandas" for op in ops)

    def test_analyze_pyspark_repartition(self):
        code = "df.repartition(10).write.parquet('output')"
        ops, _ = analyze_pyspark(code)
        assert any(op.name == "repartition" for op in ops)

    def test_analyze_pyspark_repartition_one(self):
        code = "df.repartition(1).write.parquet('output')"
        ops, _ = analyze_pyspark(code)
        assert any(op.name == "repartition" and op.weight == 15 for op in ops)

    def test_analyze_pyspark_invalid_syntax(self):
        code = "def invalid syntax here"
        with pytest.raises(ParseError) as exc_info:
            analyze_pyspark(code)
        assert "Failed to parse PySpark" in str(exc_info.value)


class TestPySparkVisitor:
    def test_visitor_creation(self):
        visitor = PySparkVisitor()
        assert visitor.operations == []
        assert visitor._in_udf is False

    def test_pyspark_weights_defined(self):
        assert "groupBy" in PYSPARK_WEIGHTS
        assert "join" in PYSPARK_WEIGHTS
        assert "crossJoin" in PYSPARK_WEIGHTS
        assert "collect" in PYSPARK_WEIGHTS
        assert "toPandas" in PYSPARK_WEIGHTS
        assert "repartition" in PYSPARK_WEIGHTS
