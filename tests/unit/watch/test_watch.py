"""Unit tests for burnt.watch modules."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from burnt.alerts.dispatch import (
    AlertResult,
    _post_slack,
    _post_teams,
    _post_webhook,
    dispatch,
)
from burnt.core.config import AlertSettings, Settings, WatchSettings
from burnt.watch.core import WatchResult, watch
from burnt.watch.drift import detect_cost_drift
from burnt.watch.idle import find_idle_clusters
from burnt.watch.tags import tag_attribution

# ---------------------------------------------------------------------------
# WatchSettings / AlertSettings defaults
# ---------------------------------------------------------------------------


class TestWatchSettings:
    def test_defaults(self) -> None:
        s = WatchSettings()
        assert s.drift_threshold == 0.25
        assert s.idle_threshold == 0.10
        assert s.days == 30
        assert s.tag_key is None
        assert s.warehouse_id is None


class TestAlertSettings:
    def test_defaults(self) -> None:
        s = AlertSettings()
        assert s.slack is None
        assert s.teams is None
        assert s.webhook is None
        assert s.delta_table is None


# ---------------------------------------------------------------------------
# tag_attribution
# ---------------------------------------------------------------------------


class TestTagAttribution:
    def test_requires_warehouse_id(self) -> None:
        with (
            patch("burnt.watch.tags.Settings.discover", return_value=(None, Settings())),
            pytest.raises(ValueError, match="warehouse_id"),
        ):
            tag_attribution("team")

    def test_returns_dict_of_costs(self) -> None:
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.execute_sql.return_value = [
            {"tag_value": "platform", "total_cost_usd": 42.0},
            {"tag_value": "__untagged__", "total_cost_usd": 7.5},
        ]
        with (
            patch("burnt.watch.tags.Settings.discover", return_value=(None, Settings())),
            patch("burnt.watch.tags.DatabricksClient", return_value=mock_client),
        ):
            result = tag_attribution("team", warehouse_id="wh-123")
        assert result == {"platform": 42.0, "__untagged__": 7.5}

    def test_skips_rows_without_tag_value(self) -> None:
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.execute_sql.return_value = [
            {"tag_value": None, "total_cost_usd": 1.0},
            {"tag_value": "eng", "total_cost_usd": 5.0},
        ]
        with (
            patch("burnt.watch.tags.Settings.discover", return_value=(None, Settings())),
            patch("burnt.watch.tags.DatabricksClient", return_value=mock_client),
        ):
            result = tag_attribution("team", warehouse_id="wh-123")
        assert "eng" in result
        assert None not in result

    def test_rejects_unsafe_tag_key(self) -> None:
        with (
            patch("burnt.watch.tags.Settings.discover", return_value=(None, Settings())),
            pytest.raises(ValueError),
        ):
            tag_attribution("team; DROP TABLE users --", warehouse_id="wh-123")


# ---------------------------------------------------------------------------
# find_idle_clusters
# ---------------------------------------------------------------------------


class TestFindIdleClusters:
    def test_requires_warehouse_id(self) -> None:
        with (
            patch("burnt.watch.idle.Settings.discover", return_value=(None, Settings())),
            pytest.raises(ValueError, match="warehouse_id"),
        ):
            find_idle_clusters()

    def test_returns_list_with_recommendation(self) -> None:
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.execute_sql.return_value = [
            {
                "cluster_id": "abc-123",
                "avg_cpu_pct": 3.0,
                "running_hours": 48.0,
                "total_cost_usd": 200.0,
                "wasted_cost_usd": 194.0,
            }
        ]
        with (
            patch("burnt.watch.idle.Settings.discover", return_value=(None, Settings())),
            patch("burnt.watch.idle.DatabricksClient", return_value=mock_client),
        ):
            clusters = find_idle_clusters(warehouse_id="wh-123")
        assert len(clusters) == 1
        c = clusters[0]
        assert c["cluster_id"] == "abc-123"
        assert c["wasted_cost_usd"] == 194.0
        assert "recommendation" in c

    def test_skips_rows_without_cluster_id(self) -> None:
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.execute_sql.return_value = [
            {"cluster_id": None, "avg_cpu_pct": 2.0, "running_hours": 10.0,
             "total_cost_usd": 50.0, "wasted_cost_usd": 49.0},
        ]
        with (
            patch("burnt.watch.idle.Settings.discover", return_value=(None, Settings())),
            patch("burnt.watch.idle.DatabricksClient", return_value=mock_client),
        ):
            clusters = find_idle_clusters(warehouse_id="wh-123")
        assert clusters == []


# ---------------------------------------------------------------------------
# detect_cost_drift
# ---------------------------------------------------------------------------


class TestDetectCostDrift:
    def test_requires_warehouse_id(self) -> None:
        with (
            patch("burnt.watch.drift.Settings.discover", return_value=(None, Settings())),
            pytest.raises(ValueError, match="warehouse_id"),
        ):
            detect_cost_drift()

    def test_returns_drift_dict(self) -> None:
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.execute_sql.return_value = [
            {"resource_id": "job:42", "median_cost": 10.0, "recent_avg": 14.0, "drift_ratio": 0.4},
            {"resource_id": "pipeline:abc", "median_cost": 5.0, "recent_avg": 3.0, "drift_ratio": -0.4},
        ]
        with (
            patch("burnt.watch.drift.Settings.discover", return_value=(None, Settings())),
            patch("burnt.watch.drift.DatabricksClient", return_value=mock_client),
        ):
            result = detect_cost_drift(warehouse_id="wh-123")
        assert result == {"job:42": 0.4, "pipeline:abc": -0.4}

    def test_skips_rows_without_resource_id(self) -> None:
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.execute_sql.return_value = [
            {"resource_id": None, "drift_ratio": 0.5},
            {"resource_id": "job:1", "drift_ratio": 0.3},
        ]
        with (
            patch("burnt.watch.drift.Settings.discover", return_value=(None, Settings())),
            patch("burnt.watch.drift.DatabricksClient", return_value=mock_client),
        ):
            result = detect_cost_drift(warehouse_id="wh-123")
        assert "job:1" in result
        assert None not in result


# ---------------------------------------------------------------------------
# WatchResult
# ---------------------------------------------------------------------------


class TestWatchResult:
    def test_default_fields(self) -> None:
        r = WatchResult()
        assert r.tag_costs == {}
        assert r.idle_clusters == []
        assert r.cost_drift == {}
        assert r.total_cost_usd == 0

    def test_build_message_with_drift(self) -> None:
        r = WatchResult(cost_drift={"job:1": 0.3}, total_cost_usd=50.0)
        msg = r._build_message()
        assert "job:1" in msg
        assert "+30.0%" in msg
        assert "$50.00" in msg

    def test_build_message_with_idle_cluster(self) -> None:
        r = WatchResult(
            idle_clusters=[{"cluster_id": "abc", "wasted_cost_usd": 99.5}]
        )
        msg = r._build_message()
        assert "abc" in msg
        assert "$99.50" in msg

    def test_alert_dispatches(self) -> None:
        r = WatchResult(cost_drift={"job:1": 0.4})
        with patch("burnt.alerts.dispatch.dispatch") as mock_dispatch:
            mock_dispatch.return_value = AlertResult(webhook_sent=True)
            r.alert(webhook="https://example.com/hook")
        mock_dispatch.assert_called_once()
        call_kwargs = mock_dispatch.call_args
        assert call_kwargs.kwargs["webhook"] == "https://example.com/hook"


# ---------------------------------------------------------------------------
# watch() orchestration
# ---------------------------------------------------------------------------


class TestWatch:
    def test_watch_returns_result_on_empty(self) -> None:
        with (
            patch("burnt.core.config.Settings.discover", return_value=(None, Settings())),
            patch("burnt.watch.tags.tag_attribution", return_value={}),
            patch("burnt.watch.idle.find_idle_clusters", return_value=[]),
            patch("burnt.watch.drift.detect_cost_drift", return_value={}),
        ):
            result = watch()
        assert isinstance(result, WatchResult)
        assert result.total_cost_usd == 0

    def test_watch_aggregates_total_cost(self) -> None:
        with (
            patch("burnt.core.config.Settings.discover", return_value=(None, Settings())),
            patch(
                "burnt.watch.tags.tag_attribution",
                return_value={"platform": 100.0, "__untagged__": 20.0},
            ),
            patch("burnt.watch.idle.find_idle_clusters", return_value=[]),
            patch("burnt.watch.drift.detect_cost_drift", return_value={}),
        ):
            result = watch(tag_key="team")
        assert result.total_cost_usd == pytest.approx(120.0)

    def test_watch_tolerates_monitor_failures(self) -> None:
        with (
            patch("burnt.core.config.Settings.discover", return_value=(None, Settings())),
            patch("burnt.watch.tags.tag_attribution", side_effect=RuntimeError("no conn")),
            patch("burnt.watch.idle.find_idle_clusters", side_effect=RuntimeError("no conn")),
            patch("burnt.watch.drift.detect_cost_drift", side_effect=RuntimeError("no conn")),
        ):
            # Should not raise
            result = watch(tag_key="team")
        assert isinstance(result, WatchResult)


# ---------------------------------------------------------------------------
# dispatch
# ---------------------------------------------------------------------------


class TestDispatch:
    def test_dispatch_no_destinations_reads_config(self) -> None:
        settings = Settings()
        with patch("burnt.core.config.Settings.discover", return_value=(None, settings)):
            result = dispatch("test message")
        # No destinations configured → nothing sent, no errors
        assert not result.slack_sent
        assert not result.teams_sent
        assert not result.webhook_sent
        assert result.errors == []

    def test_dispatch_slack_success(self) -> None:
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        with patch("burnt.alerts.dispatch.requests.post", return_value=mock_resp):
            result = dispatch("hello", slack="https://hooks.slack.com/test")
        assert result.slack_sent
        assert result.errors == []

    def test_dispatch_slack_failure_captured(self) -> None:
        with patch(
            "burnt.alerts.dispatch.requests.post",
            side_effect=Exception("timeout"),
        ):
            result = dispatch("hello", slack="https://hooks.slack.com/test")
        assert not result.slack_sent
        assert any("Slack" in e for e in result.errors)

    def test_dispatch_webhook_success(self) -> None:
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        with patch("burnt.alerts.dispatch.requests.post", return_value=mock_resp):
            result = dispatch("hello", webhook="https://example.com/hook")
        assert result.webhook_sent

    def test_post_slack_sends_block_kit(self) -> None:

        captured = {}
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()

        def capture_post(url: str, **kwargs: object) -> MagicMock:
            captured.update(kwargs)
            return mock_resp

        with patch("burnt.alerts.dispatch.requests.post", side_effect=capture_post):
            _post_slack("https://hooks.slack.com/x", "test message", "warning")

        payload = captured["json"]
        assert "blocks" in payload
        text = payload["blocks"][0]["text"]["text"]
        assert "warning" in text
        assert "test message" in text

    def test_post_teams_sends_message_card(self) -> None:
        captured = {}
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()

        def capture_post(url: str, **kwargs: object) -> MagicMock:
            captured.update(kwargs)
            return mock_resp

        with patch("burnt.alerts.dispatch.requests.post", side_effect=capture_post):
            _post_teams("https://outlook.office.com/webhook/x", "alert text", "error")

        payload = captured["json"]
        assert payload["@type"] == "MessageCard"
        assert "alert text" in payload["sections"][0]["text"]

    def test_post_webhook_sends_json(self) -> None:
        captured = {}
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()

        def capture_post(url: str, **kwargs: object) -> MagicMock:
            captured.update(kwargs)
            return mock_resp

        with patch("burnt.alerts.dispatch.requests.post", side_effect=capture_post):
            _post_webhook("https://example.com/hook", "msg", "info")

        payload = captured["json"]
        assert payload["source"] == "burnt"
        assert payload["severity"] == "info"
        assert payload["message"] == "msg"
        assert "timestamp" in payload
