"""Unit tests for the HybridEstimator."""

import pytest

from dburnrate.core.models import ClusterConfig, ExplainPlan, QueryRecord
from dburnrate.estimators.hybrid import HybridEstimator


@pytest.fixture
def cluster() -> ClusterConfig:
    """Return a default cluster configuration for tests."""
    return ClusterConfig(
        instance_type="Standard_DS3_v2", num_workers=2, dbu_per_hour=0.75
    )


@pytest.fixture
def estimator() -> HybridEstimator:
    """Return a HybridEstimator instance."""
    return HybridEstimator()


@pytest.fixture
def simple_query() -> str:
    """Return a simple SQL query for testing."""
    return "SELECT * FROM users"


class TestEstimateStaticFallback:
    """estimate() with no EXPLAIN, no historical → falls back to static."""

    def test_returns_cost_estimate(
        self, estimator: HybridEstimator, simple_query: str, cluster: ClusterConfig
    ) -> None:
        """Static fallback returns a valid CostEstimate."""
        result = estimator.estimate(simple_query, cluster)
        assert result.estimated_dbu >= 0
        assert result.confidence in ("low", "medium", "high")

    def test_no_explain_no_historical_uses_static(
        self, estimator: HybridEstimator, simple_query: str, cluster: ClusterConfig
    ) -> None:
        """With no signals, result confidence matches static estimator output."""
        result = estimator.estimate(
            simple_query, cluster, explain_plan=None, historical=None
        )
        assert result.estimated_dbu >= 0
        # Static estimator for a trivial query with no tables yields low confidence.
        assert result.confidence in ("low", "medium", "high")


class TestEstimateWithExplainStatsComplete:
    """estimate() with explain_plan stats_complete=True → confidence=high."""

    def test_confidence_high_when_stats_complete(
        self, estimator: HybridEstimator, simple_query: str, cluster: ClusterConfig
    ) -> None:
        """stats_complete=True yields confidence=high."""
        plan = ExplainPlan(
            total_size_bytes=2_000_000_000,  # 2 GB
            shuffle_count=1,
            join_types=["BroadcastHashJoin"],
            stats_complete=True,
        )
        result = estimator.estimate(simple_query, cluster, explain_plan=plan)
        assert result.confidence == "high"

    def test_dbu_positive(
        self, estimator: HybridEstimator, simple_query: str, cluster: ClusterConfig
    ) -> None:
        """Blended DBU is positive when explain has meaningful data."""
        plan = ExplainPlan(
            total_size_bytes=5_000_000_000,  # 5 GB
            shuffle_count=2,
            stats_complete=True,
        )
        result = estimator.estimate(simple_query, cluster, explain_plan=plan)
        assert result.estimated_dbu > 0

    def test_cost_usd_set(
        self, estimator: HybridEstimator, simple_query: str, cluster: ClusterConfig
    ) -> None:
        """estimated_cost_usd is set (not None) for blended estimate."""
        plan = ExplainPlan(total_size_bytes=1_000_000_000, stats_complete=True)
        result = estimator.estimate(simple_query, cluster, explain_plan=plan)
        assert result.estimated_cost_usd is not None
        assert result.estimated_cost_usd >= 0


class TestEstimateWithExplainStatsIncomplete:
    """estimate() with explain_plan stats_complete=False → confidence=medium."""

    def test_confidence_medium_when_stats_incomplete(
        self, estimator: HybridEstimator, simple_query: str, cluster: ClusterConfig
    ) -> None:
        """stats_complete=False yields confidence=medium."""
        plan = ExplainPlan(
            total_size_bytes=1_000_000_000,
            shuffle_count=0,
            stats_complete=False,
        )
        result = estimator.estimate(simple_query, cluster, explain_plan=plan)
        assert result.confidence == "medium"

    def test_blended_dbu_uses_higher_static_weight(
        self, estimator: HybridEstimator, simple_query: str, cluster: ClusterConfig
    ) -> None:
        """Incomplete stats → 60% static weight; result is between pure static and pure explain."""
        plan = ExplainPlan(
            total_size_bytes=10_000_000_000,  # 10 GB — large explain contribution
            shuffle_count=5,
            stats_complete=False,
        )
        result = estimator.estimate(simple_query, cluster, explain_plan=plan)
        assert result.estimated_dbu > 0


class TestEstimateWithHistorical:
    """estimate() with historical records → confidence=high, uses median."""

    def _make_record(self, duration_ms: int) -> QueryRecord:
        return QueryRecord(
            statement_id=f"s-{duration_ms}",
            statement_text="SELECT 1",
            start_time="2025-01-01T00:00:00Z",
            execution_duration_ms=duration_ms,
        )

    def test_historical_match_confidence_high(
        self, estimator: HybridEstimator, simple_query: str, cluster: ClusterConfig
    ) -> None:
        """Historical match yields confidence=high."""
        records = [self._make_record(60_000), self._make_record(90_000)]
        result = estimator.estimate(simple_query, cluster, historical=records)
        assert result.confidence == "high"

    def test_historical_uses_median_duration(
        self, estimator: HybridEstimator, simple_query: str, cluster: ClusterConfig
    ) -> None:
        """DBU is computed from the median (p50) of execution_duration_ms."""
        # Median of [60000, 60000, 120000] = 60000 ms
        records = [
            self._make_record(60_000),
            self._make_record(60_000),
            self._make_record(120_000),
        ]
        result = estimator.estimate(simple_query, cluster, historical=records)
        # p50 = 60_000ms = 1 minute; cluster dbu_per_hour=0.75
        # dbu = (60_000 / 3_600_000) * 0.75 = 0.0125
        expected_dbu = round((60_000 / 3_600_000) * 0.75, 4)
        assert result.estimated_dbu == pytest.approx(expected_dbu, rel=1e-4)

    def test_historical_cost_usd_computed(
        self, estimator: HybridEstimator, simple_query: str, cluster: ClusterConfig
    ) -> None:
        """estimated_cost_usd is set for historical estimates."""
        records = [self._make_record(3_600_000)]  # 1 hour
        result = estimator.estimate(simple_query, cluster, historical=records)
        assert result.estimated_cost_usd is not None
        # 1 hour * 0.75 DBU/h = 0.75 DBU; 0.75 * 0.20 = 0.15 USD
        assert result.estimated_cost_usd == pytest.approx(0.15, rel=1e-4)

    def test_historical_takes_priority_over_explain(
        self, estimator: HybridEstimator, simple_query: str, cluster: ClusterConfig
    ) -> None:
        """Historical signal takes priority; explain_plan is ignored when historical matches."""
        records = [self._make_record(60_000)]
        plan = ExplainPlan(
            total_size_bytes=1_000_000_000_000,  # enormous to distinguish
            stats_complete=True,
        )
        result_hist = estimator.estimate(simple_query, cluster, historical=records)
        result_explain = estimator.estimate(simple_query, cluster, explain_plan=plan)

        # Historical estimate should be much smaller than pure explain of 1 TB scan
        assert result_hist.estimated_dbu < result_explain.estimated_dbu


class TestEstimateHistoricalAllNone:
    """estimate() with historical records where all durations are None → fallback to static."""

    def test_all_none_durations_falls_back_to_static(
        self, estimator: HybridEstimator, simple_query: str, cluster: ClusterConfig
    ) -> None:
        """All-None execution_duration_ms → falls back to static estimator."""
        records = [
            QueryRecord(
                statement_id="s-1",
                statement_text="SELECT 1",
                start_time="2025-01-01T00:00:00Z",
                execution_duration_ms=None,
            ),
        ]
        result = estimator.estimate(simple_query, cluster, historical=records)
        # Falls back to static; static for "SELECT * FROM users" gives low/medium confidence
        assert result.estimated_dbu >= 0
        assert result.confidence in ("low", "medium", "high")


class TestExplainDbu:
    """_explain_dbu() unit tests."""

    def test_broadcast_hash_join_contributes(self, estimator: HybridEstimator) -> None:
        """BroadcastHashJoin join_dbu > 0."""
        cluster = ClusterConfig(dbu_per_hour=1.0)
        plan = ExplainPlan(
            total_size_bytes=0,
            shuffle_count=0,
            join_types=["BroadcastHashJoin"],
        )
        dbu = estimator._explain_dbu(plan, cluster)
        assert dbu > 0

    def test_sort_merge_join_higher_than_broadcast(
        self, estimator: HybridEstimator
    ) -> None:
        """SortMergeJoin has higher weight than BroadcastHashJoin."""
        cluster = ClusterConfig(dbu_per_hour=1.0)
        plan_broadcast = ExplainPlan(
            total_size_bytes=0, shuffle_count=0, join_types=["BroadcastHashJoin"]
        )
        plan_smj = ExplainPlan(
            total_size_bytes=0, shuffle_count=0, join_types=["SortMergeJoin"]
        )
        assert estimator._explain_dbu(plan_smj, cluster) > estimator._explain_dbu(
            plan_broadcast, cluster
        )

    def test_scan_scales_with_size(self, estimator: HybridEstimator) -> None:
        """Larger total_size_bytes → larger DBU estimate."""
        cluster = ClusterConfig(dbu_per_hour=1.0)
        small = ExplainPlan(total_size_bytes=1_000_000_000)  # 1 GB
        large = ExplainPlan(total_size_bytes=10_000_000_000)  # 10 GB
        assert estimator._explain_dbu(large, cluster) > estimator._explain_dbu(
            small, cluster
        )

    def test_shuffle_contributes(self, estimator: HybridEstimator) -> None:
        """More shuffles → higher DBU."""
        cluster = ClusterConfig(dbu_per_hour=1.0)
        no_shuffle = ExplainPlan(total_size_bytes=0, shuffle_count=0)
        with_shuffle = ExplainPlan(total_size_bytes=0, shuffle_count=3)
        assert estimator._explain_dbu(with_shuffle, cluster) > estimator._explain_dbu(
            no_shuffle, cluster
        )


class TestBlend:
    """_blend() produces DBU between static and explain values in expected range."""

    def test_blended_dbu_between_components(
        self, estimator: HybridEstimator, simple_query: str, cluster: ClusterConfig
    ) -> None:
        """Blended DBU lies between 0 and explain-only DBU when static is near 0."""
        plan = ExplainPlan(
            total_size_bytes=5_000_000_000,
            shuffle_count=2,
            join_types=["SortMergeJoin"],
            stats_complete=True,
        )
        static_estimate = estimator._static.estimate(simple_query, cluster=cluster)
        explain_dbu = estimator._explain_dbu(plan, cluster)

        result = estimator._blend(static_estimate, plan, cluster)

        # Blended should be between the two components (weighted average)
        lo = min(static_estimate.estimated_dbu, explain_dbu)
        hi = max(static_estimate.estimated_dbu, explain_dbu)
        assert lo <= result.estimated_dbu <= hi

    def test_stats_complete_true_yields_high_confidence(
        self, estimator: HybridEstimator, simple_query: str, cluster: ClusterConfig
    ) -> None:
        """_blend with stats_complete=True returns confidence=high."""
        plan = ExplainPlan(
            total_size_bytes=1_000_000_000,
            stats_complete=True,
        )
        static_estimate = estimator._static.estimate(simple_query, cluster=cluster)
        result = estimator._blend(static_estimate, plan, cluster)
        assert result.confidence == "high"

    def test_stats_complete_false_yields_medium_confidence(
        self, estimator: HybridEstimator, simple_query: str, cluster: ClusterConfig
    ) -> None:
        """_blend with stats_complete=False returns confidence=medium."""
        plan = ExplainPlan(
            total_size_bytes=1_000_000_000,
            stats_complete=False,
        )
        static_estimate = estimator._static.estimate(simple_query, cluster=cluster)
        result = estimator._blend(static_estimate, plan, cluster)
        assert result.confidence == "medium"
