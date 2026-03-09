"""Tests for EstimationPipeline."""

from unittest.mock import MagicMock, patch

import pytest

from burnt.core.models import ClusterConfig
from burnt.estimators.pipeline import EstimationPipeline, create_pipeline


@pytest.fixture
def cluster():
    return ClusterConfig(
        instance_type="Standard_DS3_v2",
        num_workers=2,
        dbu_per_hour=0.75,
    )


class TestEstimationPipelineOffline:
    def test_offline_mode_returns_static_estimate(self, cluster):
        """Offline mode (no backend) returns static estimation."""
        pipeline = EstimationPipeline(backend=None, warehouse_id=None)
        result = pipeline.estimate(
            "SELECT * FROM orders JOIN customers ON orders.id = customers.id", cluster
        )

        assert result.estimated_dbu > 0
        assert result.confidence in ("low", "medium", "high")

    def test_offline_mode_no_warehouse_id(self, cluster):
        """No warehouse_id falls back to static estimation."""
        pipeline = EstimationPipeline(backend=None, warehouse_id=None)
        result = pipeline.estimate(
            "SELECT * FROM orders JOIN customers ON orders.id = customers.id", cluster
        )

        assert result.estimated_dbu > 0


class TestEstimationPipelineWithBackend:
    @patch("burnt.estimators.pipeline.DatabricksClient")
    def test_tier_2_delta_metadata(self, mock_client_class, cluster):
        """Pipeline attempts Delta metadata retrieval."""
        mock_client = MagicMock()
        mock_client.execute_sql.return_value = [
            {
                "location": "s3://bucket/orders",
                "sizeInBytes": "1000000000",
                "numFiles": "10",
                "partitionColumns": "[]",
            }
        ]
        mock_client_class.return_value = mock_client

        pipeline = EstimationPipeline(backend=mock_client, warehouse_id="abc123")
        result = pipeline.estimate(
            "SELECT * FROM orders JOIN customers ON orders.id = customers.id", cluster
        )

        assert result.estimated_dbu > 0
        mock_client.execute_sql.assert_called()

    @patch("burnt.estimators.pipeline.DatabricksClient")
    def test_tier_3_explain_cost(self, mock_client_class, cluster):
        """Pipeline attempts EXPLAIN COST retrieval."""
        mock_client = MagicMock()
        mock_client.execute_sql.side_effect = [
            [
                {
                    "location": "s3://bucket/orders",
                    "sizeInBytes": "1000000",
                    "numFiles": "1",
                    "partitionColumns": "[]",
                }
            ],
            [{"plan": "== Physical Plan ==\nScan"}],
        ]
        mock_client_class.return_value = mock_client

        pipeline = EstimationPipeline(backend=mock_client, warehouse_id="abc123")
        result = pipeline.estimate(
            "SELECT * FROM orders JOIN customers ON orders.id = customers.id", cluster
        )

        assert result.estimated_dbu > 0

    @patch("burnt.estimators.pipeline.DatabricksClient")
    def test_graceful_tier_failure(self, mock_client_class, cluster):
        """Pipeline continues when a tier fails."""
        mock_client = MagicMock()
        mock_client.execute_sql.side_effect = Exception("Connection failed")
        mock_client_class.return_value = mock_client

        pipeline = EstimationPipeline(backend=mock_client, warehouse_id="abc123")
        result = pipeline.estimate(
            "SELECT * FROM orders JOIN customers ON orders.id = customers.id", cluster
        )

        assert result.estimated_dbu > 0


class TestCreatePipeline:
    def test_create_pipeline_offline(self):
        """Factory creates offline pipeline when no credentials."""
        pipeline = create_pipeline(settings=None, warehouse_id=None)
        assert pipeline._backend is None
        assert pipeline._warehouse_id is None

    @patch("burnt.estimators.pipeline.DatabricksClient")
    def test_create_pipeline_with_credentials(self, mock_client_class):
        """Factory creates backend-connected pipeline when credentials provided."""
        from burnt.core.config import Settings

        settings = Settings()
        settings.workspace_url = "https://test.cloud.databricks.com"
        settings.token = "test_token"

        pipeline = create_pipeline(settings=settings, warehouse_id="abc123")

        assert pipeline._backend is not None
        assert pipeline._warehouse_id == "abc123"
        mock_client_class.assert_called_once()
