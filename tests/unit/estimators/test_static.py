from decimal import Decimal
from unittest.mock import Mock

from dburnrate.core.models import ClusterConfig
from dburnrate.estimators.static import CostEstimator, estimate_cost


class TestCostEstimator:
    def test_estimator_creation(self):
        estimator = CostEstimator()
        assert estimator.cluster.instance_type == "Standard_DS3_v2"
        assert estimator.cluster.num_workers == 2
        assert estimator.target_currency == "USD"

    def test_estimator_with_custom_cluster(self):
        cluster = ClusterConfig(
            instance_type="Standard_DS4_v2",
            num_workers=4,
            dbu_per_hour=1.50,
        )
        estimator = CostEstimator(cluster=cluster)
        assert estimator.cluster.instance_type == "Standard_DS4_v2"
        assert estimator.cluster.num_workers == 4

    def test_estimate_sql_simple(self):
        estimator = CostEstimator()
        result = estimator.estimate("SELECT * FROM users")
        assert result.estimated_dbu >= 0
        assert result.estimated_cost_usd is not None

    def test_estimate_sql_complex(self):
        estimator = CostEstimator()
        result = estimator.estimate("SELECT * FROM a CROSS JOIN b")
        assert result.estimated_dbu > 0

    def test_estimate_pyspark(self):
        estimator = CostEstimator()
        result = estimator.estimate("df.groupBy('dept').count()", language="pyspark")
        assert result.estimated_dbu > 0

    def test_estimate_with_photon(self):
        cluster = ClusterConfig(photon_enabled=True)
        estimator = CostEstimator(cluster=cluster)
        result = estimator.estimate("SELECT * FROM a CROSS JOIN b")
        assert result.estimated_dbu > 0

    def test_estimate_eur_currency(self):
        mock_exchange = Mock()
        mock_exchange.get_rate_for_amount.return_value = Decimal("85")
        estimator = CostEstimator(
            target_currency="EUR", exchange_rate_provider=mock_exchange
        )
        result = estimator.estimate("SELECT * FROM users")
        assert result.estimated_cost_eur is not None

    def test_infer_sku_all_purpose(self):
        estimator = CostEstimator()
        cluster = ClusterConfig(instance_type="Standard_DS3_v2")
        sku = estimator._infer_sku(cluster)
        assert sku == "ALL_PURPOSE"

    def test_compute_confidence_no_tables(self):
        from dburnrate.core.models import QueryProfile

        estimator = CostEstimator()
        profile = QueryProfile(sql="SELECT 1")
        confidence = estimator._compute_confidence(profile)
        assert confidence == "low"

    def test_compute_confidence_high_complexity(self):
        from dburnrate.core.models import OperationInfo, QueryProfile

        estimator = CostEstimator()
        profile = QueryProfile(
            sql="SELECT * FROM users",
            tables=["users"],
            operations=[OperationInfo(name="Join", kind="CROSS", weight=50)],
            complexity_score=60,
        )
        confidence = estimator._compute_confidence(profile)
        assert confidence == "medium"


class TestEstimateCost:
    def test_estimate_cost_convenience_function(self):
        result = estimate_cost("SELECT * FROM users")
        assert result.estimated_dbu >= 0
        assert result.estimated_cost_usd is not None

    def test_estimate_cost_with_cluster(self):
        cluster = ClusterConfig(num_workers=4)
        result = estimate_cost("SELECT * FROM users", cluster=cluster)
        assert result.estimated_dbu >= 0
