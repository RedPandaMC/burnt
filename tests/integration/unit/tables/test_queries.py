"""Unit tests for src/burnt/tables/queries.py."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from burnt.core.models import QueryRecord
from burnt.tables.queries import (
    _row_to_record,
    find_similar_queries,
    fingerprint_sql,
    get_query_history,
    normalize_sql,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_MINIMAL_ROW: dict = {
    "statement_id": "abc-123",
    "statement_text": "SELECT 1",
    "statement_type": "SELECT",
    "start_time": "2026-01-01T00:00:00Z",
    "end_time": "2026-01-01T00:00:01Z",
    "execution_duration_ms": "500",
    "compilation_duration_ms": "100",
    "read_bytes": "1024",
    "read_rows": "10",
    "produced_rows": "5",
    "written_bytes": "0",
    "total_task_duration_ms": "400",
    "warehouse_id": "wh-001",
    "cluster_id": None,
    "status": "FINISHED",
    "error_message": None,
}


def _make_row(**overrides: object) -> dict:
    """Return a copy of _MINIMAL_ROW with optional field overrides."""
    row = dict(_MINIMAL_ROW)
    row.update(overrides)
    return row


# ---------------------------------------------------------------------------
# normalize_sql
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestNormalizeSql:
    def test_strips_line_comments(self) -> None:
        sql = "SELECT id -- this is a comment\nFROM t"
        result = normalize_sql(sql)
        assert "--" not in result
        assert "COMMENT" not in result

    def test_strips_block_comments(self) -> None:
        sql = "SELECT /* block */ id FROM t"
        result = normalize_sql(sql)
        assert "/*" not in result
        assert "BLOCK" not in result

    def test_normalizes_whitespace(self) -> None:
        sql = "SELECT   id   FROM   t"
        result = normalize_sql(sql)
        assert "  " not in result
        assert result == "SELECT ID FROM T"

    def test_uppercases_sql(self) -> None:
        sql = "select id from t"
        result = normalize_sql(sql)
        assert result == result.upper()

    def test_replaces_string_literals(self) -> None:
        sql = "SELECT id FROM t WHERE name = 'alice'"
        result = normalize_sql(sql)
        assert "'alice'" not in result
        assert "?" in result

    def test_replaces_numeric_literals(self) -> None:
        sql = "SELECT id FROM t WHERE age = 42"
        result = normalize_sql(sql)
        assert "42" not in result
        assert "?" in result

    def test_replaces_decimal_literals(self) -> None:
        sql = "SELECT id FROM t WHERE score > 3.14"
        result = normalize_sql(sql)
        assert "3.14" not in result
        assert "?" in result

    def test_collapses_in_list(self) -> None:
        sql = "SELECT id FROM t WHERE id IN (1, 2, 3)"
        result = normalize_sql(sql)
        assert result.count("?") == 1
        assert "IN (?)" in result

    def test_single_in_item_preserved(self) -> None:
        sql = "SELECT id FROM t WHERE id IN (99)"
        result = normalize_sql(sql)
        assert "IN (?)" in result


# ---------------------------------------------------------------------------
# fingerprint_sql
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestFingerprintSql:
    def test_same_sql_same_fingerprint(self) -> None:
        sql = "SELECT id FROM users WHERE id = 1"
        assert fingerprint_sql(sql) == fingerprint_sql(sql)

    def test_equivalent_sql_same_fingerprint(self) -> None:
        sql1 = "SELECT id FROM users WHERE id = 1"
        sql2 = "SELECT id FROM users WHERE id = 99"
        # Both normalize to the same structure
        assert fingerprint_sql(sql1) == fingerprint_sql(sql2)

    def test_different_sql_different_fingerprint(self) -> None:
        sql1 = "SELECT id FROM users"
        sql2 = "SELECT name FROM orders"
        assert fingerprint_sql(sql1) != fingerprint_sql(sql2)

    def test_returns_hex_string(self) -> None:
        fp = fingerprint_sql("SELECT 1")
        assert len(fp) == 64
        assert all(c in "0123456789abcdef" for c in fp)

    def test_whitespace_insensitive(self) -> None:
        sql1 = "SELECT  id  FROM  t"
        sql2 = "SELECT id FROM t"
        assert fingerprint_sql(sql1) == fingerprint_sql(sql2)


# ---------------------------------------------------------------------------
# _row_to_record
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestRowToRecord:
    def test_int_fields_coerced(self) -> None:
        row = _make_row(execution_duration_ms="750", read_bytes="2048")
        record = _row_to_record(row)
        assert record.execution_duration_ms == 750
        assert isinstance(record.execution_duration_ms, int)
        assert record.read_bytes == 2048
        assert isinstance(record.read_bytes, int)

    def test_none_optional_fields_stay_none(self) -> None:
        row = _make_row(
            execution_duration_ms=None,
            compilation_duration_ms=None,
            read_bytes=None,
            read_rows=None,
            produced_rows=None,
            written_bytes=None,
            total_task_duration_ms=None,
            cluster_id=None,
            error_message=None,
        )
        record = _row_to_record(row)
        assert record.execution_duration_ms is None
        assert record.cluster_id is None
        assert record.error_message is None

    def test_returns_query_record_instance(self) -> None:
        record = _row_to_record(_make_row())
        assert isinstance(record, QueryRecord)

    def test_statement_id_preserved(self) -> None:
        row = _make_row(statement_id="unique-xyz")
        record = _row_to_record(row)
        assert record.statement_id == "unique-xyz"


# ---------------------------------------------------------------------------
# get_query_history
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestGetQueryHistory:
    def test_returns_list_of_query_records(self) -> None:
        client = MagicMock()
        client.execute_sql.return_value = [
            _make_row(),
            _make_row(statement_id="def-456"),
        ]
        results = get_query_history(client, "wh-001", days=7)
        assert len(results) == 2
        assert all(isinstance(r, QueryRecord) for r in results)

    def test_empty_result(self) -> None:
        client = MagicMock()
        client.execute_sql.return_value = []
        results = get_query_history(client, "wh-001")
        assert results == []

    def test_calls_execute_sql_once(self) -> None:
        client = MagicMock()
        client.execute_sql.return_value = []
        get_query_history(client, "wh-002", days=14)
        client.execute_sql.assert_called_once()

    def test_sql_contains_warehouse_id(self) -> None:
        client = MagicMock()
        client.execute_sql.return_value = []
        get_query_history(client, "wh-xyz", days=7)
        sql_arg: str = client.execute_sql.call_args[0][0]
        assert "wh-xyz" in sql_arg

    def test_sql_contains_days(self) -> None:
        client = MagicMock()
        client.execute_sql.return_value = []
        get_query_history(client, "wh-001", days=90)
        sql_arg: str = client.execute_sql.call_args[0][0]
        assert "90" in sql_arg

    def test_no_select_star(self) -> None:
        client = MagicMock()
        client.execute_sql.return_value = []
        get_query_history(client, "wh-001")
        sql_arg: str = client.execute_sql.call_args[0][0]
        assert "SELECT *" not in sql_arg.upper().replace("\n", " ")


# ---------------------------------------------------------------------------
# find_similar_queries
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestFindSimilarQueries:
    def _make_client_with_rows(self, rows: list[dict]) -> MagicMock:
        client = MagicMock()
        client.execute_sql.return_value = rows
        return client

    def test_matching_fingerprint_returned(self) -> None:
        target_sql = "SELECT id FROM users WHERE id = 1"
        fp = fingerprint_sql(target_sql)
        # Row with matching SQL (different literal, same structure)
        matching_row = _make_row(statement_text="SELECT id FROM users WHERE id = 999")
        other_row = _make_row(
            statement_id="other-999",
            statement_text="SELECT name FROM orders",
        )
        client = self._make_client_with_rows([matching_row, other_row])
        results = find_similar_queries(client, fp, "wh-001")
        assert len(results) == 1
        assert results[0].statement_text == "SELECT id FROM users WHERE id = 999"

    def test_no_match_returns_empty(self) -> None:
        fp = fingerprint_sql("SELECT id FROM users")
        row = _make_row(statement_text="SELECT name FROM orders")
        client = self._make_client_with_rows([row])
        results = find_similar_queries(client, fp, "wh-001")
        assert results == []

    def test_empty_history_returns_empty(self) -> None:
        fp = fingerprint_sql("SELECT 1")
        client = self._make_client_with_rows([])
        results = find_similar_queries(client, fp, "wh-001")
        assert results == []

    def test_respects_limit(self) -> None:
        target_sql = "SELECT id FROM t WHERE x = 1"
        fp = fingerprint_sql(target_sql)
        # 5 matching rows
        rows = [
            _make_row(
                statement_id=f"id-{i}", statement_text="SELECT id FROM t WHERE x = 99"
            )
            for i in range(5)
        ]
        client = self._make_client_with_rows(rows)
        results = find_similar_queries(client, fp, "wh-001", limit=3)
        assert len(results) == 3

    def test_sql_contains_finished_status(self) -> None:
        client = self._make_client_with_rows([])
        find_similar_queries(client, "fp", "wh-001")
        sql_arg: str = client.execute_sql.call_args[0][0]
        assert "FINISHED" in sql_arg

    def test_no_select_star(self) -> None:
        client = self._make_client_with_rows([])
        find_similar_queries(client, "fp", "wh-001")
        sql_arg: str = client.execute_sql.call_args[0][0]
        assert "SELECT *" not in sql_arg.upper().replace("\n", " ")
