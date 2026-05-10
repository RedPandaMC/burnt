"""Unit tests for the Rust-backed Spark monitoring REST session client."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from burnt._engine import SessionState, session_start
from burnt._session import (
    _resolve_app_id,
    _resolve_rest_endpoint,
    collect,
    start,
)

# ---------------------------------------------------------------------------
# SessionState (Rust)
# ---------------------------------------------------------------------------


class TestSessionState:
    def test_defaults(self) -> None:
        s = SessionState()
        assert s.active is False
        assert s.rest_url is None
        assert s.app_id is None
        assert s.collected == []

    def test_repr(self) -> None:
        s = SessionState()
        s.active = True
        s.app_id = "app-123"
        assert "active=True" in repr(s)
        assert "app-123" in repr(s)


# ---------------------------------------------------------------------------
# session_start (Rust)
# ---------------------------------------------------------------------------


class TestSessionStartRust:
    def test_creates_active_state(self) -> None:
        state = session_start("http://localhost:4040/api/v1", "app-123")
        assert state.active is True
        assert state.rest_url == "http://localhost:4040/api/v1"
        assert state.app_id == "app-123"


# ---------------------------------------------------------------------------
# start() — Python endpoint discovery
# ---------------------------------------------------------------------------


class TestStartNoSpark:
    @patch("burnt._session._get_spark_session", return_value=None)
    def test_returns_inactive_when_no_spark(self, _mock) -> None:
        state = start()
        assert state.active is False

    @patch("burnt._session._get_spark_session", return_value=None)
    def test_no_exception_when_no_spark(self, _mock) -> None:
        state = start()  # must not raise
        assert isinstance(state, SessionState)


class TestStartWithSpark:
    @patch("burnt._session._get_spark_session")
    @patch("burnt._session._resolve_app_id", return_value="app-test")
    @patch(
        "burnt._session._resolve_rest_endpoint",
        return_value=("http://localhost:4040/api/v1", None),
    )
    def test_generic_spark_ui(self, _mock_endpoint, _mock_app, mock_spark) -> None:
        state = start()
        assert state.active is True
        assert state.rest_url == "http://localhost:4040/api/v1"
        assert state.app_id == "app-test"

    @patch("burnt._session._get_spark_session")
    @patch("burnt._session._resolve_app_id", return_value="app-db")
    @patch(
        "burnt._session._resolve_rest_endpoint",
        return_value=(
            "https://adb-1234.azuredatabricks.net/driver-proxy-api/o/0/c1/40001/api/v1",
            "Bearer tok",
        ),
    )
    def test_databricks_proxy(self, _mock_endpoint, _mock_app, mock_spark) -> None:
        state = start()
        assert state.active is True
        assert "driver-proxy-api" in (state.rest_url or "")
        assert state.auth_header == "Bearer tok"


# ---------------------------------------------------------------------------
# _resolve_app_id
# ---------------------------------------------------------------------------


class TestResolveAppId:
    def test_returns_app_id_from_conf(self) -> None:
        spark = MagicMock()
        spark.conf.get.return_value = "app-xyz-001"
        assert _resolve_app_id(spark) == "app-xyz-001"

    def test_warmup_on_empty_app_id(self) -> None:
        spark = MagicMock()
        spark.conf.get.side_effect = ["", "app-after-warmup"]
        spark.sql.return_value.collect.return_value = []

        result = _resolve_app_id(spark)

        spark.sql.assert_called_once_with("SELECT 1")
        assert result == "app-after-warmup"

    def test_returns_none_when_warmup_still_empty(self) -> None:
        spark = MagicMock()
        spark.conf.get.return_value = ""
        spark.sql.return_value.collect.return_value = []

        assert _resolve_app_id(spark) is None

    def test_returns_none_when_conf_raises(self) -> None:
        spark = MagicMock()
        spark.conf.get.side_effect = RuntimeError("no conf")
        assert _resolve_app_id(spark) is None


# ---------------------------------------------------------------------------
# _resolve_rest_endpoint
# ---------------------------------------------------------------------------


class TestResolveRestEndpoint:
    def test_databricks_url_shape(self) -> None:
        ctx = MagicMock()
        ctx.browserHostName = "adb-1234.azuredatabricks.net"
        ctx.workspaceId = "1234567890"
        ctx.clusterId = "0101-123456-abc12345"
        ctx.apiToken = "dapi-secret-token"

        spark = MagicMock()

        with (
            patch(
                "burnt._session.get_context",
                return_value=ctx,
                create=True,
            ),
            patch.dict(
                "sys.modules",
                {
                    "dbruntime.databricks_repl_context": MagicMock(
                        get_context=lambda: ctx
                    )
                },
            ),
        ):
            url, auth = _resolve_rest_endpoint(spark)

        assert "/driver-proxy-api/o/1234567890/0101-123456-abc12345/40001/api/v1" in (
            url or ""
        )
        assert auth == "Bearer dapi-secret-token"

    def test_generic_spark_ui_url(self) -> None:
        spark = MagicMock()
        spark.sparkContext.uiWebUrl = "http://localhost:4040"

        with patch.dict(
            "sys.modules",
            {"dbruntime": None, "dbruntime.databricks_repl_context": None},
        ):
            url, _auth = _resolve_rest_endpoint(spark)

        assert url == "http://localhost:4040/api/v1"


# ---------------------------------------------------------------------------
# collect() — Python wrapper around Rust session_collect
# ---------------------------------------------------------------------------


class TestCollect:
    def _make_state(self) -> SessionState:
        s = SessionState()
        s.active = True
        s.rest_url = "http://localhost:4040/api/v1"
        s.app_id = "app-test-1"
        return s

    @patch("burnt._session.session_collect")
    def test_calls_rust_collect(self, mock_rust_collect) -> None:
        state = self._make_state()
        collect(state)
        mock_rust_collect.assert_called_once_with(state)

    @patch("burnt._session.session_collect")
    def test_warns_and_deactivates_when_rust_sets_inactive(
        self, mock_rust_collect
    ) -> None:
        def side_effect(state: SessionState) -> None:
            state.active = False
            state.collected.clear()

        mock_rust_collect.side_effect = side_effect
        state = self._make_state()

        with pytest.warns(RuntimeWarning, match="Spark monitoring REST API"):
            collect(state)

        assert state.active is False
        assert state.collected == []

    def test_noop_when_inactive(self) -> None:
        state = SessionState()  # active=False
        collect(state)  # must not raise
        assert state.collected == []

    @patch("burnt._session.session_collect")
    def test_no_warning_when_still_active(self, mock_rust_collect) -> None:
        import warnings

        state = self._make_state()
        with warnings.catch_warnings(record=True) as record:
            warnings.simplefilter("always")
            collect(state)
        assert len(record) == 0
        assert state.active is True


# ---------------------------------------------------------------------------
# Integration: end-to-end with mocked Rust collect
# ---------------------------------------------------------------------------


class TestIntegration:
    @patch("burnt._session._get_spark_session")
    @patch("burnt._session._resolve_app_id", return_value="app-123")
    @patch(
        "burnt._session._resolve_rest_endpoint",
        return_value=("http://localhost:4040/api/v1", None),
    )
    @patch("burnt._session.session_collect")
    def test_start_then_collect(
        self, mock_collect, _mock_endpoint, _mock_app, _mock_spark
    ) -> None:
        state = start()
        assert state.active is True

        collect(state)
        mock_collect.assert_called_once_with(state)
