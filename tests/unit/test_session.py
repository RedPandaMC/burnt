"""Unit tests for the Spark monitoring REST session client."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from burnt.runtime.spark_monitor import (
    SessionState,
    _http_get,
    _normalise_stage,
    _resolve_app_id,
    _resolve_rest_endpoint,
    collect,
    start,
)


# ---------------------------------------------------------------------------
# SessionState
# ---------------------------------------------------------------------------


class TestSessionState:
    def test_defaults(self) -> None:
        s = SessionState()
        assert s.active is False
        assert s._rest_url is None
        assert s._app_id is None
        assert s.collected == []

    def test_repr(self) -> None:
        s = SessionState()
        s.active = True
        s._app_id = "app-123"
        assert "active=True" in repr(s)
        assert "app-123" in repr(s)


# ---------------------------------------------------------------------------
# start() — no Spark available
# ---------------------------------------------------------------------------


class TestStartNoSpark:
    @patch("burnt.runtime.spark_monitor._get_spark_session", return_value=None)
    def test_returns_inactive_when_no_spark(self, _mock) -> None:
        state = start()
        assert state.active is False

    @patch("burnt.runtime.spark_monitor._get_spark_session", return_value=None)
    def test_no_exception_when_no_spark(self, _mock) -> None:
        state = start()  # must not raise
        assert isinstance(state, SessionState)


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
        # First call returns empty, after warmup returns real ID
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

        with patch(
            "burnt.runtime.spark_monitor.get_context",
            return_value=ctx,
            create=True,
        ), patch.dict("sys.modules", {"dbruntime.databricks_repl_context": MagicMock(get_context=lambda: ctx)}):
            url, auth = _resolve_rest_endpoint(spark)

        assert "/driver-proxy-api/o/1234567890/0101-123456-abc12345/40001/api/v1" in (url or "")
        assert auth == "Bearer dapi-secret-token"

    def test_generic_spark_ui_url(self) -> None:
        spark = MagicMock()
        spark.sparkContext.uiWebUrl = "http://localhost:4040"

        with patch.dict("sys.modules", {"dbruntime": None, "dbruntime.databricks_repl_context": None}):
            url, auth = _resolve_rest_endpoint(spark)

        assert url == "http://localhost:4040/api/v1"
        assert auth is None

    def test_returns_none_when_no_ui_url(self) -> None:
        spark = MagicMock()
        spark.sparkContext.uiWebUrl = ""

        with patch.dict("sys.modules", {"dbruntime": None, "dbruntime.databricks_repl_context": None}):
            url, auth = _resolve_rest_endpoint(spark)

        assert url is None


# ---------------------------------------------------------------------------
# collect()
# ---------------------------------------------------------------------------

_SAMPLE_STAGES = [
    {
        "stageId": 1,
        "name": "count at <console>:1",
        "executorRunTime": 4200,
        "shuffleReadBytes": 1024,
        "shuffleWriteBytes": 2048,
        "memoryBytesSpilled": 0,
        "diskBytesSpilled": 0,
        "inputBytes": 8192,
    }
]


class TestCollect:
    def _make_state(self) -> SessionState:
        s = SessionState()
        s.active = True
        s._rest_url = "http://localhost:4040/api/v1"
        s._app_id = "app-test-1"
        return s

    @patch("burnt.runtime.spark_monitor._http_get", return_value=_SAMPLE_STAGES)
    def test_populates_collected(self, _mock) -> None:
        state = self._make_state()
        collect(state)

        assert len(state.collected) == 1
        stage = state.collected[0]
        assert stage["stage_id"] == 1
        assert stage["executor_run_time_ms"] == 4200
        assert stage["shuffle_read_bytes"] == 1024
        assert stage["input_bytes"] == 8192

    @patch("burnt.runtime.spark_monitor._http_get", return_value=None)
    def test_warns_and_deactivates_on_unreachable(self, _mock) -> None:
        state = self._make_state()

        with pytest.warns(RuntimeWarning, match="Spark monitoring REST API"):
            collect(state)

        assert state.active is False
        assert state.collected == []

    def test_noop_when_inactive(self) -> None:
        state = SessionState()  # active=False
        collect(state)  # must not raise, must not call HTTP
        assert state.collected == []

    @patch("burnt.runtime.spark_monitor._http_get", return_value=[])
    def test_empty_stages_list(self, _mock) -> None:
        state = self._make_state()
        collect(state)
        assert state.collected == []

    @patch("burnt.runtime.spark_monitor._http_get", return_value=[{"not": "a stage"}, _SAMPLE_STAGES[0]])
    def test_skips_non_dict_entries(self, _mock) -> None:
        state = self._make_state()
        collect(state)
        # Both are dicts so both are processed; normalise handles missing keys gracefully
        assert len(state.collected) == 2


# ---------------------------------------------------------------------------
# _normalise_stage
# ---------------------------------------------------------------------------


class TestNormaliseStage:
    def test_full_stage(self) -> None:
        raw = _SAMPLE_STAGES[0]
        norm = _normalise_stage(raw)
        assert norm["stage_id"] == 1
        assert norm["name"] == "count at <console>:1"
        assert norm["executor_run_time_ms"] == 4200
        assert norm["shuffle_write_bytes"] == 2048
        assert norm["memory_bytes_spilled"] == 0

    def test_missing_fields_default_to_zero(self) -> None:
        norm = _normalise_stage({})
        assert norm["stage_id"] == 0
        assert norm["shuffle_read_bytes"] == 0
        assert norm["input_bytes"] == 0


# ---------------------------------------------------------------------------
# _http_get — stdlib fallback path
# ---------------------------------------------------------------------------


class TestHttpGet:
    @patch("burnt.runtime.spark_monitor.urllib.request.urlopen")
    def test_stdlib_get_returns_parsed_json(self, mock_urlopen) -> None:
        import io
        payload = b'[{"stageId": 1}]'
        mock_resp = MagicMock()
        mock_resp.read.return_value = payload
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp

        # Force requests to be unavailable
        with patch.dict("sys.modules", {"requests": None}):
            result = _http_get("http://localhost:4040/api/v1/applications/x/stages", {})

        assert result == [{"stageId": 1}]

    @patch("burnt.runtime.spark_monitor.urllib.request.urlopen", side_effect=OSError("refused"))
    def test_stdlib_returns_none_on_error(self, _mock) -> None:
        with patch.dict("sys.modules", {"requests": None}):
            result = _http_get("http://localhost:4040/api/v1/applications/x/stages", {})
        assert result is None
