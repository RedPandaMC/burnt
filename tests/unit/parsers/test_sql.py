import pytest

from burnt.core.exceptions import ParseError
from burnt.parsers.sql import (
    analyze_query,
    compute_complexity,
    detect_operations,
    extract_tables,
    parse_sql,
)


class TestParseSql:
    def test_parse_sql_valid(self):
        result = parse_sql("SELECT * FROM users")
        assert result is not None

    def test_parse_sql_empty(self):
        with pytest.raises(ParseError) as exc_info:
            parse_sql("")
        assert "Empty SQL string" in str(exc_info.value)

    def test_parse_sql_whitespace_only(self):
        with pytest.raises(ParseError):
            parse_sql("   ")

    def test_parse_sql_invalid(self):
        with pytest.raises(ParseError) as exc_info:
            parse_sql("SELECT * FROM")
        assert "Failed to parse SQL" in str(exc_info.value)


class TestExtractTables:
    def test_extract_tables_simple(self):
        tables = extract_tables("SELECT * FROM users")
        assert "users" in tables

    def test_extract_tables_with_schema(self):
        tables = extract_tables("SELECT * FROM schema.users")
        assert "schema.users" in tables

    def test_extract_tables_with_catalog(self):
        tables = extract_tables("SELECT * FROM catalog.schema.users")
        assert "catalog.schema.users" in tables

    def test_extract_tables_multiple(self):
        tables = extract_tables("SELECT * FROM a JOIN b ON a.id = b.id")
        assert "a" in tables
        assert "b" in tables

    def test_extract_tables_unique(self):
        tables = extract_tables("SELECT * FROM users u1 JOIN users u2 ON u1.id = u2.id")
        assert tables.count("users") == 1


class TestDetectOperations:
    def test_detect_operations_merge(self):
        ops = detect_operations(
            "MERGE INTO target t USING source s ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.name = s.name"
        )
        assert any(op.name == "Merge" for op in ops)

    def test_detect_operations_cross_join(self):
        ops = detect_operations("SELECT * FROM a CROSS JOIN b")
        assert any(op.name == "Join" and op.kind == "CROSS" for op in ops)

    def test_detect_operations_inner_join(self):
        ops = detect_operations("SELECT * FROM a JOIN b ON a.id = b.id")
        assert any(op.name == "Join" and op.kind == "INNER" for op in ops)

    def test_detect_operations_group_by(self):
        ops = detect_operations("SELECT dept, COUNT(*) FROM employees GROUP BY dept")
        assert any(op.name == "GroupBy" for op in ops)

    def test_detect_operations_window(self):
        ops = detect_operations(
            "SELECT *, ROW_NUMBER() OVER (PARTITION BY dept) FROM employees"
        )
        assert any(op.name == "Window" for op in ops)

    def test_detect_operations_order_by(self):
        ops = detect_operations("SELECT * FROM users ORDER BY created_at")
        assert any(op.name == "OrderBy" for op in ops)

    def test_detect_operations_distinct(self):
        ops = detect_operations("SELECT DISTINCT category FROM products")
        assert any(op.name == "Distinct" for op in ops)

    def test_detect_operations_cte(self):
        ops = detect_operations("WITH cte AS (SELECT * FROM users) SELECT * FROM cte")
        assert any(op.name == "CTE" for op in ops)

    def test_detect_operations_subquery(self):
        ops = detect_operations(
            "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)"
        )
        assert any(op.name == "Subquery" for op in ops)


class TestComputeComplexity:
    def test_compute_complexity_simple_select(self):
        complexity = compute_complexity("SELECT * FROM users")
        assert complexity == 0

    def test_compute_complexity_merge(self):
        complexity = compute_complexity(
            "MERGE INTO target t USING source s ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.name = s.name"
        )
        assert complexity == 20

    def test_compute_complexity_cross_join(self):
        complexity = compute_complexity("SELECT * FROM a CROSS JOIN b")
        assert complexity == 50

    def test_compute_complexity_multiple_operations(self):
        sql = """
        SELECT *, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary) as rn
        FROM employees
        WHERE dept IN (SELECT dept FROM depts)
        GROUP BY dept
        """
        complexity = compute_complexity(sql)
        assert complexity > 0


class TestAnalyzeQuery:
    def test_analyze_query_simple(self):
        profile = analyze_query("SELECT * FROM users WHERE id = 1")
        assert profile.sql == "SELECT * FROM users WHERE id = 1"
        assert profile.dialect == "databricks"
        assert "users" in profile.tables

    def test_analyze_query_with_operations(self):
        profile = analyze_query("SELECT * FROM a CROSS JOIN b")
        assert len(profile.operations) > 0
        assert profile.complexity_score > 0
