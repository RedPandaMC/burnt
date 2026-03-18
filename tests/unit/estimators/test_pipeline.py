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
    def test_offline_mode_suppresses_dollar_amounts(self, cluster):
        """Offline mode (no backend) suppresses all cost fields."""
        pipeline = EstimationPipeline(backend=None, warehouse_id=None)
        result = pipeline.estimate(
            "SELECT * FROM orders JOIN customers ON orders.id = customers.id", cluster
        )

        assert result.estimated_cost_usd is None
        assert result.estimated_cost_eur is None
        assert result.estimated_dbu is None
        assert result.confidence == "none"

    def test_offline_mode_preserves_complexity(self, cluster):
        """Offline mode keeps complexity score in breakdown."""
        pipeline = EstimationPipeline(backend=None, warehouse_id=None)
        result = pipeline.estimate(
            "SELECT * FROM a CROSS JOIN b", cluster
        )

        assert "complexity" in result.breakdown
        assert result.breakdown["complexity"] > 0

    def test_offline_mode_no_warehouse_id(self, cluster):
        """No warehouse_id falls back to static estimation."""
        pipeline = EstimationPipeline(backend=None, warehouse_id=None)
        result = pipeline.estimate(
            "SELECT * FROM orders JOIN customers ON orders.id = customers.id", cluster
        )

        assert result.estimated_dbu is None
        assert result.estimated_cost_usd is None


class TestEstimationPipelineWithBackend:
    def test_tier_2_delta_metadata(self, cluster):
        """Pipeline attempts Delta metadata retrieval."""
        mock_backend = MagicMock()
        mock_backend.execute_sql.return_value = [
            {
                "location": "s3://bucket/orders",
                "sizeInBytes": "1000000000",
                "numFiles": "10",
                "partitionColumns": "[]",
            }
        ]

        pipeline = EstimationPipeline(backend=mock_backend, warehouse_id="abc123")
        result = pipeline.estimate(
            "SELECT * FROM orders JOIN customers ON orders.id = customers.id", cluster
        )

        assert result.estimated_dbu > 0
        mock_backend.execute_sql.assert_called()

    def test_tier_3_explain_cost(self, cluster):
        """Pipeline attempts EXPLAIN COST retrieval."""
        mock_backend = MagicMock()
        mock_backend.execute_sql.side_effect = [
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

        pipeline = EstimationPipeline(backend=mock_backend, warehouse_id="abc123")
        result = pipeline.estimate(
            "SELECT * FROM orders JOIN customers ON orders.id = customers.id", cluster
        )

        assert result.estimated_dbu > 0

    def test_graceful_tier_failure(self, cluster):
        """Pipeline continues when a tier fails."""
        mock_backend = MagicMock()
        mock_backend.execute_sql.side_effect = Exception("Connection failed")

        pipeline = EstimationPipeline(backend=mock_backend, warehouse_id="abc123")
        result = pipeline.estimate(
            "SELECT * FROM orders JOIN customers ON orders.id = customers.id", cluster
        )

        assert result.estimated_dbu > 0

    def test_connected_mode_produces_cost_usd(self, cluster):
        """Connected backend path populates estimated_cost_usd from DBU."""
        mock_backend = MagicMock()
        mock_backend.execute_sql.side_effect = Exception("no tiers")

        pipeline = EstimationPipeline(backend=mock_backend, warehouse_id="abc123")
        result = pipeline.estimate(
            "SELECT * FROM orders", cluster
        )

        assert result.estimated_dbu is not None
        assert result.estimated_cost_usd is not None
        assert result.estimated_cost_usd > 0
        assert result.confidence != "none"


class TestCreatePipeline:
    def test_create_pipeline_offline(self):
        """Factory creates offline pipeline when no credentials."""
        pipeline = create_pipeline(settings=None, warehouse_id=None)
        assert pipeline._backend is None
        assert pipeline._warehouse_id is None

    @patch("burnt.runtime.auto._create_rest_backend")
    def test_create_pipeline_with_credentials(self, mock_create_backend):
        """Factory creates backend-connected pipeline when DATABRICKS_HOST is set."""
        mock_backend = MagicMock()
        mock_create_backend.return_value = mock_backend

        with patch.dict(
            "os.environ", {"DATABRICKS_HOST": "https://test.cloud.databricks.com"}
        ):
            pipeline = create_pipeline(warehouse_id="abc123")

        assert pipeline._backend is not None
        assert pipeline._warehouse_id == "abc123"
        mock_create_backend.assert_called_once()
