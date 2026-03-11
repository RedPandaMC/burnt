"""Fluent WhatIfBuilder for what-if cost scenario modeling."""

import logging
from collections.abc import Callable
from decimal import Decimal

from rich.console import Console
from rich.table import Table

from ..core.models import (
    ClusterConfig,
    CostEstimate,
    MultiScenarioResult,
    WhatIfModification,
    WhatIfResult,
)

logger = logging.getLogger(__name__)

console = Console()

SPEEDUP_FACTORS = {
    "complex_join": 2.7,
    "aggregation": 4.0,
    "window": 2.5,
    "simple_insert": 1.0,
}

PHOTON_COST_MULTIPLIER = Decimal("2.5")

SPOT_VM_DISCOUNT = 0.70
POOL_VM_DISCOUNT = 0.70

COST_MULTIPLIERS = {
    "delta_format": 0.70,
    "liquid_clustering": 0.40,
    "disk_cache": 0.15,
    "column_pruning": 0.60,
    "file_skipping": 0.70,
    "aqe": 0.85,
    "shuffle_partitions": 0.92,
    "broadcast_join": 0.70,
}

VERIFIED_MULTIPLIERS = {"aqe"}


def apply_photon_scenario(
    estimate: CostEstimate,
    query_type: str = "complex_join",
) -> CostEstimate:
    """Apply Photon scenario to an estimate. Internal helper."""
    speedup = SPEEDUP_FACTORS.get(query_type, 2.0)
    original_cost = estimate.estimated_cost_usd or 0.0

    new_dbu = estimate.estimated_dbu * float(PHOTON_COST_MULTIPLIER) / speedup
    new_cost = original_cost * float(PHOTON_COST_MULTIPLIER) / speedup

    savings_pct = (original_cost - new_cost) / max(original_cost, 0.01) * 100

    warnings = list(estimate.warnings)
    if savings_pct < 0:
        warnings.append(
            f"Photon increases cost by {-savings_pct:.1f}% for {query_type}"
        )

    return CostEstimate(
        estimated_dbu=round(new_dbu, 2),
        estimated_cost_usd=round(new_cost, 4),
        confidence="medium",
        breakdown={**estimate.breakdown, "photon": True, "speedup": speedup},
        warnings=warnings,
    )


def apply_cluster_resize(
    estimate: CostEstimate,
    current_cluster: ClusterConfig,
    new_cluster: ClusterConfig,
) -> CostEstimate:
    """Apply cluster resize scenario. Internal helper."""
    current_factor = current_cluster.num_workers * current_cluster.dbu_per_hour
    new_factor = new_cluster.num_workers * new_cluster.dbu_per_hour

    ratio = new_factor / current_factor
    new_cost = (estimate.estimated_cost_usd or 0) * ratio

    savings_pct = (
        ((estimate.estimated_cost_usd or 0) - new_cost)
        / (estimate.estimated_cost_usd or 1)
        * 100
    )

    return CostEstimate(
        estimated_dbu=estimate.estimated_dbu,
        estimated_cost_usd=round(new_cost, 4),
        confidence="medium",
        breakdown={**estimate.breakdown, "cluster_resize_ratio": ratio},
        warnings=[f"Estimated savings: {savings_pct:.1f}%"],
    )


def apply_serverless_migration(
    estimate: CostEstimate,
    current_sku: str = "ALL_PURPOSE",
    utilization_pct: float = 50.0,
) -> CostEstimate:
    """Apply serverless migration scenario. Internal helper."""
    serverless_rates = {
        "ALL_PURPOSE": 0.95,
        "JOBS_COMPUTE": 0.45,
        "SQL_PRO": 0.70,
    }

    classic_rates = {
        "ALL_PURPOSE": 0.55,
        "JOBS_COMPUTE": 0.30,
        "SQL_PRO": 0.55,
    }

    serverless_rate = serverless_rates.get(current_sku, 0.70)
    classic_rate = classic_rates.get(current_sku, 0.55)

    if utilization_pct < 30:
        ratio = serverless_rate / classic_rate
    else:
        effective_classic = classic_rate * (utilization_pct / 100)
        ratio = serverless_rate / effective_classic

    new_cost = (estimate.estimated_cost_usd or 0) * ratio

    return CostEstimate(
        estimated_dbu=estimate.estimated_dbu,
        estimated_cost_usd=round(new_cost, 4),
        confidence="low",
        breakdown={
            **estimate.breakdown,
            "serverless": True,
            "utilization": utilization_pct,
        },
        warnings=[
            f"Serverless is {'cheaper' if ratio < 1 else 'more expensive'} at {utilization_pct}% utilization"
        ],
    )


class WhatIfBuilder:
    """Fluent builder for what-if cost scenario modeling."""

    def __init__(
        self,
        estimate: CostEstimate,
        cluster: ClusterConfig | None = None,
    ):
        self._original_estimate = estimate
        self._cluster = cluster or ClusterConfig()
        self._modifications: list[WhatIfModification] = []
        self._photon_query_type: str | None = None
        self._target_instance: str | None = None
        self._target_workers: int | None = None
        self._use_spot: bool | None = None
        self._spot_fallback: bool = True
        self._use_pool: bool = False
        self._pool_instance_pool_id: str | None = None
        self._pool_use_spot: bool = False
        self._pool_min_idle: int = 0
        self._to_serverless: bool = False
        self._serverless_utilization: float = 50.0
        self._scenarios: list[tuple[str | None, Callable[[WhatIfBuilder], None]]] = []
        self._top_level_mods_applied: bool = False

    def cluster(self) -> "ClusterBuilder":
        """Enter cluster configuration context."""
        return ClusterBuilder(self)

    def data_source(self) -> "DataSourceBuilder":
        """Enter data source configuration context."""
        return DataSourceBuilder(self)

    def spark_config(self) -> "SparkConfigBuilder":
        """Enter Spark configuration context."""
        return SparkConfigBuilder(self)

    def scenarios(
        self,
        scenarios: list[tuple[str | None, Callable[["WhatIfBuilder"], None]]],
    ) -> "WhatIfBuilder":
        """Define multiple scenarios to compare."""
        self._scenarios = scenarios
        return self

    def options(self) -> None:
        """Print available options to console."""
        table = Table(title="What-If Options")
        table.add_column("Category", style="cyan")
        table.add_column("Options", style="green")

        table.add_row(
            "Cluster",
            "enable_photon, to_instance, use_spot, use_pool, set_workers, to_serverless",
        )
        table.add_row(
            "Data Source",
            "to_delta_format, enable_liquid_clustering, enable_disk_cache, compact_files",
        )
        table.add_row(
            "Spark Config",
            "with_shuffle_partitions, with_aqe_enabled, with_broadcast_threshold_mb",
        )

        console.print(table)

    def compare(self) -> WhatIfResult | MultiScenarioResult:
        """Compare scenarios and return result."""
        if self._scenarios:
            return self._compare_multiple()
        return self._compare_single()

    def _compare_single(self) -> WhatIfResult:
        """Compare single scenario."""
        projected = self._apply_modifications()

        original_cost = self._original_estimate.estimated_cost_usd or 0
        projected_cost = projected.estimated_cost_usd or 0

        if original_cost > 0:
            savings_pct = ((original_cost - projected_cost) / original_cost) * 100
        else:
            savings_pct = 0.0

        return WhatIfResult(
            original=self._original_estimate,
            projected=projected,
            modifications=self._modifications,
            total_savings_pct=round(savings_pct, 1),
            recommended_cluster=self._cluster,
        )

    def _compare_multiple(self) -> MultiScenarioResult:
        """Compare multiple scenarios."""
        results: list[tuple[str, WhatIfResult]] = []

        top_level_mods = list(self._modifications)
        top_level_applied = self._top_level_mods_applied

        baseline_name = "Baseline"
        baseline_builder = WhatIfBuilder(self._original_estimate, self._cluster)
        if top_level_applied:
            baseline_builder._modifications = list(top_level_mods)
            baseline_builder._photon_query_type = self._photon_query_type
            baseline_builder._target_instance = self._target_instance
            baseline_builder._target_workers = self._target_workers
            baseline_builder._use_spot = self._use_spot
            baseline_builder._spot_fallback = self._spot_fallback
            baseline_builder._use_pool = self._use_pool
            baseline_builder._pool_use_spot = self._pool_use_spot
            baseline_builder._to_serverless = self._to_serverless

        baseline_result = baseline_builder._compare_single()
        results.append((baseline_name, baseline_result))

        for idx, (name, builder_fn) in enumerate(self._scenarios):
            scenario_name = name or f"Scenario {idx + 1}"
            builder = WhatIfBuilder(self._original_estimate, self._cluster)

            if top_level_applied:
                builder._modifications = list(top_level_mods)
                builder._photon_query_type = self._photon_query_type
                builder._target_instance = self._target_instance
                builder._target_workers = self._target_workers
                builder._use_spot = self._use_spot
                builder._spot_fallback = self._spot_fallback
                builder._use_pool = self._use_pool
                builder._pool_use_spot = self._pool_use_spot
                builder._to_serverless = self._to_serverless

            builder_fn(builder)
            result = builder._compare_single()
            results.append((scenario_name, result))

        return MultiScenarioResult(scenarios=results)

    def _apply_modifications(self) -> CostEstimate:
        """Apply all modifications to get projected estimate."""
        estimate = self._original_estimate
        cluster = ClusterConfig(
            instance_type=self._cluster.instance_type,
            num_workers=self._cluster.num_workers,
            dbu_per_hour=self._cluster.dbu_per_hour,
            photon_enabled=self._cluster.photon_enabled,
            sku=self._cluster.sku,
            spot_policy=self._cluster.spot_policy,
        )

        if self._photon_query_type:
            estimate = apply_photon_scenario(estimate, self._photon_query_type)
            self._modifications.append(
                WhatIfModification(
                    name="Enable Photon",
                    cost_multiplier=float(PHOTON_COST_MULTIPLIER)
                    / SPEEDUP_FACTORS.get(self._photon_query_type, 2.0),
                    is_verified=True,
                    rationale=f"Photon {self._photon_query_type} optimization",
                    trade_offs=[
                        f"Requires {SPEEDUP_FACTORS.get(self._photon_query_type, 2.0)}x speedup to break even"
                    ],
                )
            )
            cluster = ClusterConfig(
                instance_type=cluster.instance_type,
                num_workers=cluster.num_workers,
                dbu_per_hour=cluster.dbu_per_hour,
                photon_enabled=True,
                sku=cluster.sku,
                spot_policy=cluster.spot_policy,
            )

        if self._target_instance:
            from ..core.instances import AZURE_INSTANCE_CATALOG

            try:
                target_spec = AZURE_INSTANCE_CATALOG[self._target_instance]
                current_spec = AZURE_INSTANCE_CATALOG.get(
                    cluster.instance_type,
                    type("Spec", (), {"dbu_rate": cluster.dbu_per_hour})(),
                )
                ratio = target_spec.dbu_rate / current_spec.dbu_rate

                new_breakdown = dict(estimate.breakdown)
                new_breakdown["instance_change_ratio"] = ratio

                estimate = CostEstimate(
                    estimated_dbu=estimate.estimated_dbu,
                    estimated_cost_usd=round(
                        (estimate.estimated_cost_usd or 0) * ratio, 4
                    ),
                    confidence="medium",
                    breakdown=new_breakdown,
                )
                self._modifications.append(
                    WhatIfModification(
                        name=f"Migrate to {self._target_instance}",
                        cost_multiplier=ratio,
                        is_verified=False,
                        rationale=f"Instance type change from {cluster.instance_type}",
                        trade_offs=[],
                    )
                )
                cluster = ClusterConfig(
                    instance_type=self._target_instance,
                    num_workers=cluster.num_workers,
                    dbu_per_hour=target_spec.dbu_rate,
                    photon_enabled=cluster.photon_enabled,
                    sku=cluster.sku,
                    spot_policy=cluster.spot_policy,
                )
            except KeyError:
                logger.warning(f"Unknown instance type: {self._target_instance}")

        if self._target_workers:
            old_workers = cluster.num_workers
            cluster = ClusterConfig(
                instance_type=cluster.instance_type,
                num_workers=self._target_workers,
                dbu_per_hour=cluster.dbu_per_hour,
                photon_enabled=cluster.photon_enabled,
                sku=cluster.sku,
                spot_policy=cluster.spot_policy,
            )
            ratio = self._target_workers / old_workers
            estimate = CostEstimate(
                estimated_dbu=estimate.estimated_dbu * ratio,
                estimated_cost_usd=round((estimate.estimated_cost_usd or 0) * ratio, 4),
                confidence="medium",
                breakdown={**estimate.breakdown, "worker_change": self._target_workers},
            )
            self._modifications.append(
                WhatIfModification(
                    name=f"Resize to {self._target_workers} workers",
                    cost_multiplier=ratio,
                    is_verified=False,
                    rationale=f"Worker count change from {old_workers}",
                    trade_offs=[],
                )
            )

        if self._use_spot is not None:
            if self._use_spot:
                vm_discount = SPOT_VM_DISCOUNT
                policy = (
                    "SPOT_WITH_ON_DEMAND_FALLBACK" if self._spot_fallback else "SPOT"
                )
                self._modifications.append(
                    WhatIfModification(
                        name=f"Use Spot Instances ({'with fallback' if self._spot_fallback else 'no fallback'})",
                        cost_multiplier=vm_discount,
                        is_verified=False,
                        rationale="VM cost discount using spot instances",
                        trade_offs=["Risk of eviction without fallback"],
                    )
                )
            else:
                vm_discount = 1.0
                policy = "ON_DEMAND"

            estimate = CostEstimate(
                estimated_dbu=estimate.estimated_dbu,
                estimated_cost_usd=round(
                    (estimate.estimated_cost_usd or 0) * vm_discount, 4
                ),
                confidence="medium",
                breakdown={**estimate.breakdown, "spot": self._use_spot},
            )
            cluster = ClusterConfig(
                instance_type=cluster.instance_type,
                num_workers=cluster.num_workers,
                dbu_per_hour=cluster.dbu_per_hour,
                photon_enabled=cluster.photon_enabled,
                sku=cluster.sku,
                spot_policy=policy,  # type: ignore
            )

        if self._use_pool:
            if self._pool_use_spot:
                vm_discount = POOL_VM_DISCOUNT
                self._modifications.append(
                    WhatIfModification(
                        name="Use Instance Pool (with Spot)",
                        cost_multiplier=vm_discount,
                        is_verified=False,
                        rationale="Instance pool with spot VMs",
                        trade_offs=["Faster startup", "Risk of spot eviction"],
                    )
                )
            else:
                vm_discount = 1.0
                self._modifications.append(
                    WhatIfModification(
                        name="Use Instance Pool (on-demand)",
                        cost_multiplier=1.0,
                        is_verified=False,
                        rationale="Instance pool with on-demand VMs + idle costs",
                        trade_offs=["Faster startup", "Pay for idle VMs"],
                    )
                )

            estimate = CostEstimate(
                estimated_dbu=estimate.estimated_dbu,
                estimated_cost_usd=round(
                    (estimate.estimated_cost_usd or 0) * vm_discount, 4
                ),
                confidence="medium",
                breakdown={
                    **estimate.breakdown,
                    "pool": True,
                    "pool_use_spot": self._pool_use_spot,
                },
            )

        if self._to_serverless:
            estimate = apply_serverless_migration(
                estimate, cluster.sku, self._serverless_utilization
            )
            self._modifications.append(
                WhatIfModification(
                    name="Migrate to Serverless",
                    cost_multiplier=0.85,
                    is_verified=False,
                    rationale="Serverless compute migration",
                    trade_offs=["Pay-per-query model"],
                )
            )

        return estimate


class ClusterBuilder:
    """Builder for cluster configuration changes."""

    def __init__(self, parent: WhatIfBuilder):
        self._parent = parent

    def enable_photon(self, query_type: str = "complex_join") -> "ClusterBuilder":
        """Enable Photon optimization."""
        if self._parent._use_pool:
            logger.warning(
                "Photon already configured via use_pool(). "
                "Overriding with enable_photon()."
            )
        self._parent._photon_query_type = query_type
        self._parent._top_level_mods_applied = True
        return self

    def disable_photon(self) -> "ClusterBuilder":
        """Disable Photon optimization."""
        self._parent._photon_query_type = None
        return self

    def to_instance(self, instance_type: str) -> "ClusterBuilder":
        """Change to a different instance type."""
        self._parent._target_instance = instance_type
        self._parent._top_level_mods_applied = True
        return self

    def use_spot(self, fallback: bool = True) -> "ClusterBuilder":
        """Use spot instances."""
        if self._parent._use_pool:
            logger.warning("use_pool() already called. Overriding with use_spot().")
        self._parent._use_spot = True
        self._parent._spot_fallback = fallback
        self._parent._use_pool = False
        self._parent._top_level_mods_applied = True
        return self

    def use_pool(
        self,
        instance_pool_id: str | None = None,
        use_spot: bool = False,
        min_idle: int = 0,
    ) -> "ClusterBuilder":
        """Use an instance pool."""
        self._parent._use_pool = True
        self._parent._pool_instance_pool_id = instance_pool_id
        self._parent._pool_use_spot = use_spot
        self._parent._pool_min_idle = min_idle
        self._parent._use_spot = None
        self._parent._top_level_mods_applied = True
        return self

    def set_workers(self, count: int) -> "ClusterBuilder":
        """Set number of workers."""
        self._parent._target_workers = count
        self._parent._top_level_mods_applied = True
        return self

    def to_serverless(self, utilization_pct: float = 50.0) -> "ClusterBuilder":
        """Migrate to serverless compute."""
        self._parent._to_serverless = True
        self._parent._serverless_utilization = utilization_pct
        self._parent._top_level_mods_applied = True
        return self

    def data_source(self) -> "DataSourceBuilder":
        """Enter data source configuration context."""
        return DataSourceBuilder(self._parent)

    def spark_config(self) -> "SparkConfigBuilder":
        """Enter Spark configuration context."""
        return SparkConfigBuilder(self._parent)

    def compare(self) -> WhatIfResult:
        """Compare scenarios and return result."""
        return self._parent.compare()


class DataSourceBuilder:
    """Builder for data source optimization changes."""

    def __init__(self, parent: WhatIfBuilder):
        self._parent = parent

    def to_delta_format(self) -> "DataSourceBuilder":
        """Migrate to Delta format."""
        self._parent._modifications.append(
            WhatIfModification(
                name="Delta Format",
                cost_multiplier=COST_MULTIPLIERS["delta_format"],
                is_verified=False,
                rationale="Delta Lake format optimization",
                trade_offs=["Requires Delta Lake"],
            )
        )
        self._apply_datasource_multiplier(COST_MULTIPLIERS["delta_format"])
        return self

    def enable_liquid_clustering(self, keys: list[str]) -> "DataSourceBuilder":
        """Enable Liquid Clustering."""
        self._parent._modifications.append(
            WhatIfModification(
                name="Liquid Clustering",
                cost_multiplier=COST_MULTIPLIERS["liquid_clustering"],
                is_verified=False,
                rationale=f"Liquid clustering on {keys}",
                trade_offs=["Requires Delta Lake", "Best for high-cardinality columns"],
            )
        )
        self._apply_datasource_multiplier(COST_MULTIPLIERS["liquid_clustering"])
        return self

    def set_partitioning(self, column: str) -> "DataSourceBuilder":
        """Set partitioning on a column."""
        self._parent._modifications.append(
            WhatIfModification(
                name=f"Partition by {column}",
                cost_multiplier=0.80,
                is_verified=False,
                rationale=f"Partition optimization on {column}",
                trade_offs=["Requires understanding of query patterns"],
            )
        )
        self._apply_datasource_multiplier(0.80)
        return self

    def enable_disk_cache(self) -> "DataSourceBuilder":
        """Enable disk cache."""
        self._parent._modifications.append(
            WhatIfModification(
                name="Disk Cache",
                cost_multiplier=COST_MULTIPLIERS["disk_cache"],
                is_verified=False,
                rationale="Disk cache for repeated reads",
                trade_offs=["Only effective for repeated queries"],
            )
        )
        self._apply_datasource_multiplier(COST_MULTIPLIERS["disk_cache"])
        return self

    def compact_files(self, target_mb: int = 128) -> "DataSourceBuilder":
        """Compact small files."""
        self._parent._modifications.append(
            WhatIfModification(
                name=f"File Compaction ({target_mb}MB target)",
                cost_multiplier=0.90,
                is_verified=False,
                rationale="Small file compaction",
                trade_offs=["One-time optimization cost"],
            )
        )
        self._apply_datasource_multiplier(0.90)
        return self

    def enable_column_pruning(self) -> "DataSourceBuilder":
        """Enable column pruning optimization."""
        self._parent._modifications.append(
            WhatIfModification(
                name="Column Pruning",
                cost_multiplier=COST_MULTIPLIERS["column_pruning"],
                is_verified=False,
                rationale="Read only required columns",
                trade_offs=["Requires SELECT with specific columns"],
            )
        )
        self._apply_datasource_multiplier(COST_MULTIPLIERS["column_pruning"])
        return self

    def enable_file_skipping(self) -> "DataSourceBuilder":
        """Enable file skipping / predicate pushdown."""
        self._parent._modifications.append(
            WhatIfModification(
                name="File Skipping",
                cost_multiplier=COST_MULTIPLIERS["file_skipping"],
                is_verified=False,
                rationale="Skip irrelevant data files",
                trade_offs=["Requires partitioned/clustered data"],
            )
        )
        self._apply_datasource_multiplier(COST_MULTIPLIERS["file_skipping"])
        return self

    def set_compression(self, codec: str = "zstd") -> "DataSourceBuilder":
        """Set compression codec."""
        self._parent._modifications.append(
            WhatIfModification(
                name=f"Compression ({codec})",
                cost_multiplier=0.95,
                is_verified=False,
                rationale=f"Compression with {codec}",
                trade_offs=["CPU overhead for compression"],
            )
        )
        self._apply_datasource_multiplier(0.95)
        return self

    def _apply_datasource_multiplier(self, multiplier: float) -> None:
        """Apply a multiplier to the current estimate."""
        current = self._parent._original_estimate
        new_cost = (current.estimated_cost_usd or 0) * multiplier
        self._parent._original_estimate = CostEstimate(
            estimated_dbu=current.estimated_dbu,
            estimated_cost_usd=round(new_cost, 4),
            confidence=current.confidence,
            breakdown={**current.breakdown, "datasource_optimization": True},
            warnings=list(current.warnings),
        )

    def cluster(self) -> ClusterBuilder:
        """Enter cluster configuration context."""
        return ClusterBuilder(self._parent)

    def spark_config(self) -> "SparkConfigBuilder":
        """Enter Spark configuration context."""
        return SparkConfigBuilder(self._parent)

    def compare(self) -> WhatIfResult:
        """Compare scenarios and return result."""
        return self._parent.compare()


class SparkConfigBuilder:
    """Builder for Spark configuration changes."""

    def __init__(self, parent: WhatIfBuilder):
        self._parent = parent

    def with_shuffle_partitions(self, count: int) -> "SparkConfigBuilder":
        """Set shuffle partitions count."""
        self._parent._modifications.append(
            WhatIfModification(
                name=f"Shuffle Partitions ({count})",
                cost_multiplier=COST_MULTIPLIERS["shuffle_partitions"],
                is_verified=False,
                rationale=f"Optimized shuffle with {count} partitions",
                trade_offs=["May need tuning for specific workloads"],
            )
        )
        self._apply_spark_multiplier(COST_MULTIPLIERS["shuffle_partitions"])
        return self

    def with_auto_shuffle_partitions(self) -> "SparkConfigBuilder":
        """Enable automatic shuffle partitions."""
        self._parent._modifications.append(
            WhatIfModification(
                name="Auto Shuffle Partitions",
                cost_multiplier=0.88,
                is_verified=False,
                rationale="Adaptive shuffle partition sizing",
                trade_offs=[],
            )
        )
        self._apply_spark_multiplier(0.88)
        return self

    def with_broadcast_threshold_mb(self, mb: int) -> "SparkConfigBuilder":
        """Set broadcast join threshold."""
        self._parent._modifications.append(
            WhatIfModification(
                name=f"Broadcast Threshold ({mb}MB)",
                cost_multiplier=COST_MULTIPLIERS["broadcast_join"],
                is_verified=False,
                rationale=f"Broadcast join threshold set to {mb}MB",
                trade_offs=["Memory pressure on drivers"],
            )
        )
        self._apply_spark_multiplier(COST_MULTIPLIERS["broadcast_join"])
        return self

    def with_aqe_enabled(self, coalesce: bool = True) -> "SparkConfigBuilder":
        """Enable Adaptive Query Execution."""
        self._parent._modifications.append(
            WhatIfModification(
                name="AQE Enabled",
                cost_multiplier=COST_MULTIPLIERS["aqe"],
                is_verified=True,
                rationale="Adaptive Query Execution (verified by Databricks)",
                trade_offs=[],
            )
        )
        self._apply_spark_multiplier(COST_MULTIPLIERS["aqe"])
        return self

    def set(self, key: str, value: str | int | bool) -> "SparkConfigBuilder":
        """Set arbitrary Spark configuration."""
        self._parent._modifications.append(
            WhatIfModification(
                name=f"Spark Config: {key}={value}",
                cost_multiplier=1.0,
                is_verified=False,
                rationale="Custom Spark configuration",
                trade_offs=["Effect varies by configuration"],
            )
        )
        return self

    def _apply_spark_multiplier(self, multiplier: float) -> None:
        """Apply a multiplier to the current estimate."""
        current = self._parent._original_estimate
        new_cost = (current.estimated_cost_usd or 0) * multiplier
        self._parent._original_estimate = CostEstimate(
            estimated_dbu=current.estimated_dbu,
            estimated_cost_usd=round(new_cost, 4),
            confidence=current.confidence,
            breakdown={**current.breakdown, "spark_optimization": True},
            warnings=list(current.warnings),
        )

    def cluster(self) -> ClusterBuilder:
        """Enter cluster configuration context."""
        return ClusterBuilder(self._parent)

    def data_source(self) -> DataSourceBuilder:
        """Enter data source configuration context."""
        return DataSourceBuilder(self._parent)

    def compare(self) -> WhatIfResult:
        """Compare scenarios and return result."""
        return self._parent.compare()
