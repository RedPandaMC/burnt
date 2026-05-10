"""Unit tests for src/burnt/tables/compute.py."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from burnt.core.exceptions import DatabricksQueryError
from burnt.core.models import ClusterConfig
from burnt.tables.compute import (
    _parse_int,
    get_cluster_config,
    get_node_timeline,
    get_node_types,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def mock_client() -> MagicMock:
    """Return a MagicMock standing in for DatabricksClient."""
    return MagicMock()


# ---------------------------------------------------------------------------
# get_node_types
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestGetNodeTypes:
    """Tests for get_node_types."""

    def test_returns_mapping_of_two_node_types(self, mock_client: MagicMock) -> None:
        """Normal response with two node types returns correct mapping."""
        mock_client.execute_sql.return_value = [
            {"node_type_id": "Standard_DS3_v2", "dbu_per_hour": "0.75"},
            {"node_type_id": "Standard_DS5_v2", "dbu_per_hour": "1.5"},
        ]
        result = get_node_types(mock_client, "wh-123")
        assert result == {"Standard_DS3_v2": 0.75, "Standard_DS5_v2": 1.5}

    def test_empty_result_returns_empty_dict(self, mock_client: MagicMock) -> None:
        """Empty result from client returns empty dict."""
        mock_client.execute_sql.return_value = []
        result = get_node_types(mock_client, "wh-123")
        assert result == {}

    def test_rows_with_none_dbu_per_hour_are_skipped(
        self, mock_client: MagicMock
    ) -> None:
        """Rows where dbu_per_hour is None are excluded from the mapping."""
        mock_client.execute_sql.return_value = [
            {"node_type_id": "Standard_DS3_v2", "dbu_per_hour": None},
            {"node_type_id": "Standard_DS5_v2", "dbu_per_hour": "1.5"},
        ]
        result = get_node_types(mock_client, "wh-123")
        assert result == {"Standard_DS5_v2": 1.5}
        assert "Standard_DS3_v2" not in result


# ---------------------------------------------------------------------------
# get_cluster_config
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestGetClusterConfig:
    """Tests for get_cluster_config."""

    def test_found_cluster_maps_to_cluster_config(self, mock_client: MagicMock) -> None:
        """A found cluster is correctly mapped to ClusterConfig fields."""
        cluster_row = {
            "cluster_id": "abc-123",
            "cluster_name": "my-cluster",
            "node_type_id": "Standard_DS3_v2",
            "driver_node_type_id": "Standard_DS3_v2",
            "num_workers": "4",
            "autoscale_min_workers": None,
            "autoscale_max_workers": None,
            "spark_version": "13.3.x-scala2.12",
            "cluster_source": "UI",
        }
        node_type_rows = [
            {"node_type_id": "Standard_DS3_v2", "dbu_per_hour": "0.75"},
        ]

        def execute_sql_side_effect(sql: str, warehouse_id: str):  # type: ignore[return]
            if "system.compute.clusters" in sql:
                return [cluster_row]
            if "system.compute.node_types" in sql:
                return node_type_rows

        mock_client.execute_sql.side_effect = execute_sql_side_effect

        config = get_cluster_config(mock_client, "abc-123", "wh-123")

        assert isinstance(config, ClusterConfig)
        assert config.instance_type == "Standard_DS3_v2"
        assert config.num_workers == 4
        assert config.dbu_per_hour == 0.75

    def test_cluster_not_found_raises_error(self, mock_client: MagicMock) -> None:
        """DatabricksQueryError is raised when cluster is not found."""
        mock_client.execute_sql.return_value = []
        with pytest.raises(DatabricksQueryError, match="Cluster not found: missing-id"):
            get_cluster_config(mock_client, "missing-id", "wh-123")

    def test_unknown_node_type_defaults_dbu_to_zero(
        self, mock_client: MagicMock
    ) -> None:
        """When node_type_id is not in node_types, dbu_per_hour defaults to 0.0."""
        cluster_row = {
            "cluster_id": "xyz-999",
            "cluster_name": "exotic-cluster",
            "node_type_id": "UnknownType",
            "driver_node_type_id": "UnknownType",
            "num_workers": "2",
            "autoscale_min_workers": None,
            "autoscale_max_workers": None,
            "spark_version": "14.0.x-scala2.12",
            "cluster_source": "API",
        }

        def execute_sql_side_effect(sql: str, warehouse_id: str):  # type: ignore[return]
            if "system.compute.clusters" in sql:
                return [cluster_row]
            if "system.compute.node_types" in sql:
                return []

        mock_client.execute_sql.side_effect = execute_sql_side_effect

        config = get_cluster_config(mock_client, "xyz-999", "wh-123")
        assert config.dbu_per_hour == 0.0

    def test_num_workers_none_defaults_to_one(self, mock_client: MagicMock) -> None:
        """When num_workers is None (e.g. autoscale cluster), defaults to 1."""
        cluster_row = {
            "cluster_id": "auto-1",
            "cluster_name": "autoscale",
            "node_type_id": "Standard_DS3_v2",
            "driver_node_type_id": "Standard_DS3_v2",
            "num_workers": None,
            "autoscale_min_workers": "2",
            "autoscale_max_workers": "8",
            "spark_version": "13.3.x-scala2.12",
            "cluster_source": "UI",
        }

        def execute_sql_side_effect(sql: str, warehouse_id: str):  # type: ignore[return]
            if "system.compute.clusters" in sql:
                return [cluster_row]
            if "system.compute.node_types" in sql:
                return [{"node_type_id": "Standard_DS3_v2", "dbu_per_hour": "0.75"}]

        mock_client.execute_sql.side_effect = execute_sql_side_effect

        config = get_cluster_config(mock_client, "auto-1", "wh-123")
        assert config.num_workers == 1


# ---------------------------------------------------------------------------
# get_node_timeline
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestGetNodeTimeline:
    """Tests for get_node_timeline."""

    def test_returns_raw_rows_from_client(self, mock_client: MagicMock) -> None:
        """get_node_timeline returns raw rows as returned by the client."""
        expected = [
            {
                "cluster_id": "abc-123",
                "node_type": "Standard_DS3_v2",
                "start_time": "2024-01-01T00:00:00Z",
                "end_time": "2024-01-01T01:00:00Z",
                "driver": False,
                "num_nodes": 4,
            }
        ]
        mock_client.execute_sql.return_value = expected

        result = get_node_timeline(
            mock_client,
            "abc-123",
            "2024-01-01T00:00:00Z",
            "2024-01-01T01:00:00Z",
            "wh-123",
        )

        assert result == expected

    def test_empty_timeline_returns_empty_list(self, mock_client: MagicMock) -> None:
        """Returns empty list when no timeline records exist."""
        mock_client.execute_sql.return_value = []

        result = get_node_timeline(
            mock_client,
            "no-cluster",
            "2024-01-01T00:00:00Z",
            "2024-01-02T00:00:00Z",
            "wh-123",
        )

        assert result == []


# ---------------------------------------------------------------------------
# _parse_int
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestParseInt:
    """Tests for the _parse_int helper."""

    def test_none_returns_none(self) -> None:
        """None input returns None."""
        assert _parse_int(None) is None

    def test_string_int_returns_int(self) -> None:
        """String '5' parses to integer 5."""
        assert _parse_int("5") == 5

    def test_int_returns_int(self) -> None:
        """Integer 7 returns 7."""
        assert _parse_int(7) == 7

    def test_invalid_string_returns_none(self) -> None:
        """Non-numeric string 'abc' returns None."""
        assert _parse_int("abc") is None

    def test_float_string_returns_none(self) -> None:
        """Float string '3.14' returns None (int() raises ValueError)."""
        assert _parse_int("3.14") is None

    def test_list_returns_none(self) -> None:
        """A list input returns None."""
        assert _parse_int([1, 2]) is None
