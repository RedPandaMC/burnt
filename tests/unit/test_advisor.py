"""Unit tests for burnt advisor module."""

from unittest.mock import Mock, patch

import pytest

from burnt.advisor.report import AdvisoryReport, ComputeScenario
from burnt.advisor.session import advise, advise_current_session
from burnt.core.models import ClusterConfig, ClusterRecommendation


class TestComputeScenario:
    def test_compute_scenario_creation(self):
        scenario = ComputeScenario(
            compute_type="Jobs Compute",
            sku="JOBS_COMPUTE",
            estimated_cost_usd=18.25,
            savings_pct=-60.0,
            tradeoff="Recommended",
        )
        assert scenario.compute_type == "Jobs Compute"
        assert scenario.sku == "JOBS_COMPUTE"
        assert scenario.estimated_cost_usd == 18.25
        assert scenario.savings_pct == -60.0
        assert scenario.tradeoff == "Recommended"


class TestAdvisoryReport:
    def test_advisory_report_creation(self):
        baseline = ComputeScenario(
            compute_type="All-Purpose",
            sku="ALL_PURPOSE",
            estimated_cost_usd=45.12,
            savings_pct=0.0,
            tradeoff="(Your test run)",
        )

        scenario1 = ComputeScenario(
            compute_type="Jobs Compute",
            sku="JOBS_COMPUTE",
            estimated_cost_usd=18.25,
            savings_pct=-60.0,
            tradeoff="Recommended",
        )

        scenario2 = ComputeScenario(
            compute_type="SQL Serverless",
            sku="SERVERLESS",
            estimated_cost_usd=28.50,
            savings_pct=-37.0,
            tradeoff="Fastest cold start",
        )

        cluster_config = ClusterConfig(
            instance_type="Standard_DS3_v2", num_workers=3, dbu_per_hour=0.75
        )

        recommendation = ClusterRecommendation(
            economy=ClusterConfig(
                instance_type="Standard_DS3_v2", num_workers=2, dbu_per_hour=0.75
            ),
            balanced=cluster_config,
            performance=ClusterConfig(
                instance_type="Standard_DS5_v2", num_workers=4, dbu_per_hour=3.00
            ),
            current_cost_usd=45.12,
            rationale="Test rationale",
        )

        report = AdvisoryReport(
            baseline=baseline,
            scenarios=[scenario1, scenario2],
            recommended=cluster_config,
            recommendation=recommendation,
            insights=["Peak memory 14%, downsize DS4→DS3"],
            run_metrics={"duration_ms": 60000},
        )

        assert report.baseline == baseline
        assert len(report.scenarios) == 2
        assert report.recommended == cluster_config
        assert report.recommendation == recommendation
        assert len(report.insights) == 1
        assert report.run_metrics["duration_ms"] == 60000

    def test_comparison_table(self):
        baseline = ComputeScenario(
            compute_type="All-Purpose",
            sku="ALL_PURPOSE",
            estimated_cost_usd=45.12,
            savings_pct=0.0,
            tradeoff="(Your test run)",
        )

        scenario = ComputeScenario(
            compute_type="Jobs Compute",
            sku="JOBS_COMPUTE",
            estimated_cost_usd=18.25,
            savings_pct=-60.0,
            tradeoff="Recommended",
        )

        cluster_config = ClusterConfig(
            instance_type="Standard_DS3_v2", num_workers=3, dbu_per_hour=0.75
        )

        recommendation = ClusterRecommendation(
            economy=ClusterConfig(
                instance_type="Standard_DS3_v2", num_workers=2, dbu_per_hour=0.75
            ),
            balanced=cluster_config,
            performance=ClusterConfig(
                instance_type="Standard_DS5_v2", num_workers=4, dbu_per_hour=3.00
            ),
            current_cost_usd=45.12,
            rationale="Test",
        )

        report = AdvisoryReport(
            baseline=baseline,
            scenarios=[scenario],
            recommended=cluster_config,
            recommendation=recommendation,
            insights=[],
            run_metrics={},
        )

        table = report.comparison_table()
        assert "Compute Migration Analysis" in table
        assert "All-Purpose" in table
        assert "Jobs Compute" in table
        assert "$45.12" in table
        assert "$18.25" in table
        assert "-60.0%" in table

    def test_what_if_not_implemented(self):
        baseline = ComputeScenario(
            compute_type="All-Purpose",
            sku="ALL_PURPOSE",
            estimated_cost_usd=45.12,
            savings_pct=0.0,
            tradeoff="Test",
        )

        report = AdvisoryReport(
            baseline=baseline,
            scenarios=[],
            recommended=ClusterConfig(),
            recommendation=ClusterRecommendation(
                economy=ClusterConfig(),
                balanced=ClusterConfig(),
                performance=ClusterConfig(),
                current_cost_usd=0.0,
                rationale="",
            ),
            insights=[],
            run_metrics={},
        )

        with pytest.raises(NotImplementedError):
            report.what_if()


class TestSessionAdvisor:
    @patch("burnt.advisor.session._auto_backend_or_error")
    def test_advise_current_session_no_backend(self, mock_auto_backend):
        mock_auto_backend.side_effect = RuntimeError("No Databricks context")

        with pytest.raises(RuntimeError, match="No Databricks context"):
            advise_current_session()

    @patch("burnt.advisor.session._auto_backend_or_error")
    def test_advise_current_session_with_mock_backend(self, mock_auto_backend):
        mock_backend = Mock()
        mock_backend.get_session_metrics.return_value = {
            "duration_ms": 60000,
            "peak_memory_pct": 25.0,
            "peak_cpu_pct": 35.0,
            "spill_to_disk_bytes": 0,
            "cluster_id": "test-cluster",
        }

        mock_backend.get_cluster_config.return_value = ClusterConfig(
            instance_type="Standard_DS4_v2",
            num_workers=2,
            dbu_per_hour=1.5,
            sku="ALL_PURPOSE",
        )

        mock_auto_backend.return_value = mock_backend

        # Mock get_cluster_config to avoid instance catalog dependency
        with patch("burnt.advisor.session.get_cluster_config") as mock_get_cluster:
            mock_get_cluster.return_value = ClusterConfig(
                instance_type="Standard_DS3_v2",
                num_workers=3,
                dbu_per_hour=0.75,
                sku="JOBS_COMPUTE",
            )

            report = advise_current_session(backend=mock_backend)

            assert isinstance(report, AdvisoryReport)
            assert report.baseline.compute_type == "All-Purpose"
            assert len(report.scenarios) > 0
            assert isinstance(report.recommended, ClusterConfig)
            assert isinstance(report.recommendation, ClusterRecommendation)

    @patch("burnt.advisor.session._auto_backend_or_error")
    def test_advise_with_statement_id(self, mock_auto_backend):
        mock_backend = Mock()
        mock_backend.execute_sql.return_value = [
            {
                "execution_duration_ms": 45000,
                "read_bytes": 1024 * 1024 * 100,  # 100 MB
                "cluster_id": "test-cluster",
                "statement_id": "test-statement",
            }
        ]

        mock_auto_backend.return_value = mock_backend

        # Mock get_cluster_config
        with patch("burnt.advisor.session.get_cluster_config") as mock_get_cluster:
            mock_get_cluster.return_value = ClusterConfig(
                instance_type="Standard_DS3_v2",
                num_workers=3,
                dbu_per_hour=0.75,
                sku="JOBS_COMPUTE",
            )

            report = advise(statement_id="test-statement", backend=mock_backend)

            assert isinstance(report, AdvisoryReport)
            assert report.baseline.compute_type == "All-Purpose"
            assert report.baseline.tradeoff == "(Historical run)"

    def test_advise_with_job_id_not_implemented(self):
        with pytest.raises(NotImplementedError, match="advise\\(job_id=\\.\\.\\.\\)"):
            advise(job_id="test-job")

    def test_advise_no_parameters(self):
        with pytest.raises(ValueError, match="Either run_id or statement_id"):
            advise()


class TestErrorMessages:
    def test_advise_current_session_outside_databricks(self):
        with patch("burnt.advisor.session._auto_backend_or_error") as mock_auto:
            mock_auto.side_effect = RuntimeError(
                "No Databricks execution context detected. "
                "advise_current_session() requires a Databricks runtime. "
                "Set DATABRICKS_HOST and authentication credentials, "
                "or run inside a Databricks notebook."
            )

            with pytest.raises(
                RuntimeError, match="No Databricks execution context detected"
            ):
                advise_current_session()

    @patch("burnt.advisor.session._auto_backend_or_error")
    def test_advise_current_session_no_metrics(self, mock_auto_backend):
        mock_backend = Mock()
        mock_backend.get_session_metrics.side_effect = Exception("No metrics")
        mock_auto_backend.return_value = mock_backend

        with pytest.raises(RuntimeError, match="Could not retrieve session metrics"):
            advise_current_session(backend=mock_backend)

    def test_advise_no_history_found(self):
        mock_backend = Mock()
        mock_backend.execute_sql.return_value = []

        with pytest.raises(RuntimeError, match="Failed to fetch metrics"):
            advise(run_id="test-run", backend=mock_backend)

    @patch("burnt.advisor.session._auto_backend_or_error")
    def test_advise_history_error(self, mock_auto_backend):
        mock_backend = Mock()
        mock_backend.execute_sql.side_effect = Exception("Permission denied")
        mock_auto_backend.return_value = mock_backend

        with pytest.raises(RuntimeError, match="Failed to fetch metrics"):
            advise(run_id="test-run", backend=mock_backend)


if __name__ == "__main__":
    pytest.main([__file__])
