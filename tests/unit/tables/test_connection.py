"""Unit tests for DatabricksClient."""

from unittest.mock import MagicMock, patch

import pytest

from dburnrate.core.config import Settings
from dburnrate.core.exceptions import DatabricksConnectionError, DatabricksQueryError
from dburnrate.tables.connection import DatabricksClient


def _settings(**kwargs) -> Settings:
    """Build Settings with test defaults."""
    defaults = {
        "workspace_url": "https://test.azuredatabricks.net",
        "token": "dapi-test",
    }
    defaults.update(kwargs)
    return Settings(**defaults)


class TestDatabricksClientInit:
    def test_init_success(self):
        client = DatabricksClient(_settings())
        assert client._base_url == "https://test.azuredatabricks.net"

    def test_init_strips_trailing_slash(self):
        client = DatabricksClient(
            _settings(workspace_url="https://test.azuredatabricks.net/")
        )
        assert client._base_url == "https://test.azuredatabricks.net"

    def test_init_missing_url(self):
        with pytest.raises(DatabricksConnectionError, match="WORKSPACE_URL"):
            DatabricksClient(_settings(workspace_url=None))

    def test_init_missing_token(self):
        with pytest.raises(DatabricksConnectionError, match="TOKEN"):
            DatabricksClient(_settings(token=None))

    def test_context_manager(self):
        with DatabricksClient(_settings()) as client:
            assert client._base_url == "https://test.azuredatabricks.net"


class TestExecuteSqlSuccess:
    def _mock_post(self, statement_id: str = "stmt-123") -> MagicMock:
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = {
            "statement_id": statement_id,
            "status": {"state": "SUCCEEDED"},
            "manifest": {"schema": {"columns": [{"name": "id"}, {"name": "val"}]}},
            "result": {"data_array": [["1", "foo"], ["2", "bar"]]},
        }
        return resp

    def test_execute_sql_returns_rows(self):
        client = DatabricksClient(_settings())
        mock_resp = self._mock_post()
        with patch.object(client._session, "post", return_value=mock_resp):
            rows = client.execute_sql("SELECT 1", "wh-001")
        assert rows == [{"id": "1", "val": "foo"}, {"id": "2", "val": "bar"}]

    def test_execute_sql_empty_result(self):
        client = DatabricksClient(_settings())
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = {
            "statement_id": "stmt-001",
            "status": {"state": "SUCCEEDED"},
            "manifest": {"schema": {"columns": [{"name": "n"}]}},
            "result": {"data_array": []},
        }
        with patch.object(client._session, "post", return_value=resp):
            rows = client.execute_sql("SELECT 1 WHERE false", "wh-001")
        assert rows == []


class TestExecuteSqlFailures:
    def test_query_failure_immediate(self):
        client = DatabricksClient(_settings())
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = {
            "statement_id": "stmt-bad",
            "status": {"state": "FAILED", "error": {"message": "Syntax error"}},
        }
        with (
            patch.object(client._session, "post", return_value=resp),
            pytest.raises(DatabricksQueryError, match="Syntax error"),
        ):
            client.execute_sql("INVALID SQL", "wh-001")

    def test_auth_error_raises_connection_error(self):
        client = DatabricksClient(_settings())
        resp = MagicMock()
        resp.status_code = 401
        resp.headers = {}
        with (
            patch.object(client._session, "post", return_value=resp),
            pytest.raises(DatabricksConnectionError, match="Authentication"),
        ):
            client.execute_sql("SELECT 1", "wh-001")


class TestRetryLogic:
    def test_retry_on_429(self):
        client = DatabricksClient(_settings())

        success_resp = MagicMock()
        success_resp.status_code = 200
        success_resp.json.return_value = {
            "statement_id": "stmt-ok",
            "status": {"state": "SUCCEEDED"},
            "manifest": {"schema": {"columns": []}},
            "result": {"data_array": []},
        }

        rate_limit_resp = MagicMock()
        rate_limit_resp.status_code = 429
        rate_limit_resp.headers = {"Retry-After": "0"}

        call_count = 0

        def side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return rate_limit_resp
            return success_resp

        with (
            patch.object(client._session, "post", side_effect=side_effect),
            patch("dburnrate.tables.connection.time.sleep"),
        ):
            rows = client.execute_sql("SELECT 1", "wh-001")
        assert rows == []
        assert call_count == 2

    def test_exhausted_retries_raises(self):
        client = DatabricksClient(_settings())
        rate_limit_resp = MagicMock()
        rate_limit_resp.status_code = 503
        rate_limit_resp.headers = {"Retry-After": "0"}

        with (
            patch.object(client._session, "post", return_value=rate_limit_resp),
            patch("dburnrate.tables.connection.time.sleep"),
            pytest.raises(DatabricksConnectionError),
        ):
            client.execute_sql("SELECT 1", "wh-001")


class TestPolling:
    def test_polls_until_succeeded(self):
        client = DatabricksClient(_settings())

        submit_resp = MagicMock()
        submit_resp.status_code = 200
        submit_resp.json.return_value = {
            "statement_id": "stmt-poll",
            "status": {"state": "PENDING"},
        }

        pending_resp = MagicMock()
        pending_resp.status_code = 200
        pending_resp.json.return_value = {
            "statement_id": "stmt-poll",
            "status": {"state": "RUNNING"},
        }

        done_resp = MagicMock()
        done_resp.status_code = 200
        done_resp.json.return_value = {
            "statement_id": "stmt-poll",
            "status": {"state": "SUCCEEDED"},
            "manifest": {"schema": {"columns": [{"name": "x"}]}},
            "result": {"data_array": [["42"]]},
        }

        with (
            patch.object(client._session, "post", return_value=submit_resp),
            patch.object(client._session, "get", side_effect=[pending_resp, done_resp]),
            patch("dburnrate.tables.connection.time.sleep"),
        ):
            rows = client.execute_sql("SELECT 42 AS x", "wh-001")
        assert rows == [{"x": "42"}]
