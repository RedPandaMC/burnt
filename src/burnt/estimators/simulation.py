"""Fluent Simulation builder for cost scenario modeling."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from decimal import Decimal

from ..core.models import (
    ClusterConfig,
    CostEstimate,
    MultiSimulationResult,
    SimulationModification,
    SimulationResult,
)

logger = logging.getLogger(__name__)

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


@dataclass
class _ExtraMod:
    """Internal: a single accumulated cost multiplier (data source or Spark config)."""

    name: str
    multiplier: float
    is_verified: bool = False
    rationale: str = ""
    trade_offs: list[str] = field(default_factory=list)


@dataclass
class _ScenarioState:
    """All mutable state for one named (or unnamed) scenario."""

    photon_query_type: str | None = None
    target_instance: str | None = None
    target_workers: int | None = None
    use_spot: bool | None = None
    spot_fallback: bool = True
    disable_spot_flag: bool = False
    use_pool: bool = False
    pool_instance_pool_id: str | None = None
    pool_use_spot: bool = False
    pool_min_idle: int = 0
    to_serverless: bool = False
    serverless_utilization: float = 50.0
    extra_multipliers: list[_ExtraMod] = field(default_factory=list)


class Simulation:
    """Fluent builder for cost scenario modeling."""

    def __init__(
        self,
        estimate: CostEstimate,
        cluster: ClusterConfig | None = None,
    ):
        self._original_estimate = estimate
        self._cluster = cluster or ClusterConfig()
        self._unnamed_state: _ScenarioState = _ScenarioState()
        self._named_scenarios: dict[str, _ScenarioState] = {}
        self._current_scenario: str | None = None

    def _current_state(self) -> _ScenarioState:
        """Return the state for the currently active scenario."""
        if self._current_scenario is not None:
            return self._named_scenarios[self._current_scenario]
        return self._unnamed_state

    def scenario(self, name: str) -> Simulation:
        """Switch to building a named scenario.

        Raises ValueError if unnamed modifications have already been applied.
        """
        if self._unnamed_state != _ScenarioState():
            raise ValueError(
                "Cannot mix pre-scenario modifications with named scenarios. "
                "Call scenario() first."
            )
        self._current_scenario = name
        if name not in self._named_scenarios:
            self._named_scenarios[name] = _ScenarioState()
        return self

    def cluster(self) -> ClusterContext:
        """Enter cluster configuration context."""
        return ClusterContext(self)

    def data_source(self) -> DataSourceContext:
        """Enter data source configuration context."""
        return DataSourceContext(self)

    def spark_config(self) -> SparkConfigContext:
        """Enter Spark configuration context."""
        return SparkConfigContext(self)

    def compare(self) -> SimulationResult | MultiSimulationResult:
        """Run all scenarios and return comparison results."""
        if self._named_scenarios:
            return self._compare_named()
        return self._compare_single()

    def _compare_single(self) -> SimulationResult:
        """Apply unnamed state and return a single SimulationResult."""
        projected, modifications = self._apply_state(self._unnamed_state)
        original_cost = self._original_estimate.estimated_cost_usd or 0
        projected_cost = projected.estimated_cost_usd or 0
        savings_pct = (
            ((original_cost - projected_cost) / original_cost) * 100
            if original_cost > 0
            else 0.0
        )
        return SimulationResult(
            original=self._original_estimate,
            projected=projected,
            modifications=modifications,
            total_savings_pct=round(savings_pct, 1),
            recommended_cluster=self._cluster,
        )

    def _compare_named(self) -> MultiSimulationResult:
        """Apply each named scenario and return a MultiSimulationResult."""
        results: list[tuple[str, SimulationResult]] = []
        for name, state in self._named_scenarios.items():
            projected, modifications = self._apply_state(state)
            original_cost = self._original_estimate.estimated_cost_usd or 0
            projected_cost = projected.estimated_cost_usd or 0
            savings_pct = (
                ((original_cost - projected_cost) / original_cost) * 100
                if original_cost > 0
                else 0.0
            )
            results.append(
                (
                    name,
                    SimulationResult(
                        original=self._original_estimate,
                        projected=projected,
                        modifications=modifications,
                        total_savings_pct=round(savings_pct, 1),
                    ),
                )
            )
        return MultiSimulationResult(scenarios=results)

    def _apply_state(
        self, state: _ScenarioState
    ) -> tuple[CostEstimate, list[SimulationModification]]:
        """Apply all modifications in a ScenarioState to the original estimate."""
        estimate = self._original_estimate
        modifications: list[SimulationModification] = []

        cluster = ClusterConfig(
            instance_type=self._cluster.instance_type,
            num_workers=self._cluster.num_workers,
            dbu_per_hour=self._cluster.dbu_per_hour,
            photon_enabled=self._cluster.photon_enabled,
            sku=self._cluster.sku,
            spot_policy=self._cluster.spot_policy,
        )

        # --- Photon ---
        if state.photon_query_type:
            estimate = apply_photon_scenario(estimate, state.photon_query_type)
            modifications.append(
                SimulationModification(
                    name="Enable Photon",
                    cost_multiplier=float(PHOTON_COST_MULTIPLIER)
                    / SPEEDUP_FACTORS.get(state.photon_query_type, 2.0),
                    is_verified=True,
                    rationale=f"Photon {state.photon_query_type} optimization",
                    trade_offs=[
                        f"Requires {SPEEDUP_FACTORS.get(state.photon_query_type, 2.0)}x speedup to break even"
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

        # --- Instance type change ---
        if state.target_instance:
            from ..core.instances import AZURE_INSTANCE_CATALOG

            try:
                target_spec = AZURE_INSTANCE_CATALOG[state.target_instance]
                current_spec = AZURE_INSTANCE_CATALOG.get(
                    cluster.instance_type,
                    type("Spec", (), {"dbu_rate": cluster.dbu_per_hour})(),
                )
                ratio = target_spec.dbu_rate / current_spec.dbu_rate
                estimate = CostEstimate(
                    estimated_dbu=estimate.estimated_dbu,
                    estimated_cost_usd=round(
                        (estimate.estimated_cost_usd or 0) * ratio, 4
                    ),
                    confidence="medium",
                    breakdown={**estimate.breakdown, "instance_change_ratio": ratio},
                )
                modifications.append(
                    SimulationModification(
                        name=f"Migrate to {state.target_instance}",
                        cost_multiplier=ratio,
                        is_verified=False,
                        rationale=f"Instance type change from {cluster.instance_type}",
                        trade_offs=[],
                    )
                )
                cluster = ClusterConfig(
                    instance_type=state.target_instance,
                    num_workers=cluster.num_workers,
                    dbu_per_hour=target_spec.dbu_rate,
                    photon_enabled=cluster.photon_enabled,
                    sku=cluster.sku,
                    spot_policy=cluster.spot_policy,
                )
            except KeyError:
                logger.warning(f"Unknown instance type: {state.target_instance}")

        # --- Worker resize ---
        if state.target_workers:
            old_workers = cluster.num_workers
            ratio = state.target_workers / old_workers
            estimate = CostEstimate(
                estimated_dbu=estimate.estimated_dbu * ratio,
                estimated_cost_usd=round(
                    (estimate.estimated_cost_usd or 0) * ratio, 4
                ),
                confidence="medium",
                breakdown={**estimate.breakdown, "worker_change": state.target_workers},
            )
            modifications.append(
                SimulationModification(
                    name=f"Resize to {state.target_workers} workers",
                    cost_multiplier=ratio,
                    is_verified=False,
                    rationale=f"Worker count change from {old_workers}",
                    trade_offs=[],
                )
            )
            cluster = ClusterConfig(
                instance_type=cluster.instance_type,
                num_workers=state.target_workers,
                dbu_per_hour=cluster.dbu_per_hour,
                photon_enabled=cluster.photon_enabled,
                sku=cluster.sku,
                spot_policy=cluster.spot_policy,
            )

        # --- Spot / on-demand / pool ---
        if state.disable_spot_flag:
            # Explicit on-demand override — no cost change, just records intent
            modifications.append(
                SimulationModification(
                    name="Disable Spot (On-Demand)",
                    cost_multiplier=1.0,
                    is_verified=False,
                    rationale="Explicit on-demand compute (no spot eviction risk)",
                    trade_offs=["Higher VM cost than spot"],
                )
            )
            cluster = ClusterConfig(
                instance_type=cluster.instance_type,
                num_workers=cluster.num_workers,
                dbu_per_hour=cluster.dbu_per_hour,
                photon_enabled=cluster.photon_enabled,
                sku=cluster.sku,
                spot_policy="ON_DEMAND",
            )
        elif state.use_spot is not None:
            if state.use_spot:
                vm_discount = SPOT_VM_DISCOUNT
                policy = (
                    "SPOT_WITH_ON_DEMAND_FALLBACK" if state.spot_fallback else "SPOT"
                )
                modifications.append(
                    SimulationModification(
                        name=f"Use Spot Instances ({'with fallback' if state.spot_fallback else 'no fallback'})",
                        cost_multiplier=vm_discount,
                        is_verified=False,
                        rationale="VM cost discount using spot instances",
                        trade_offs=["Risk of eviction without fallback"],
                    )
                )
                estimate = CostEstimate(
                    estimated_dbu=estimate.estimated_dbu,
                    estimated_cost_usd=round(
                        (estimate.estimated_cost_usd or 0) * vm_discount, 4
                    ),
                    confidence="medium",
                    breakdown={**estimate.breakdown, "spot": True},
                )
                cluster = ClusterConfig(
                    instance_type=cluster.instance_type,
                    num_workers=cluster.num_workers,
                    dbu_per_hour=cluster.dbu_per_hour,
                    photon_enabled=cluster.photon_enabled,
                    sku=cluster.sku,
                    spot_policy=policy,  # type: ignore[arg-type]
                )

        if state.use_pool:
            if state.pool_use_spot:
                vm_discount = POOL_VM_DISCOUNT
                modifications.append(
                    SimulationModification(
                        name="Use Instance Pool (with Spot)",
                        cost_multiplier=vm_discount,
                        is_verified=False,
                        rationale="Instance pool with spot VMs",
                        trade_offs=["Faster startup", "Risk of spot eviction"],
                    )
                )
            else:
                vm_discount = 1.0
                modifications.append(
                    SimulationModification(
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
                    "pool_use_spot": state.pool_use_spot,
                },
            )

        # --- Serverless ---
        if state.to_serverless:
            estimate = apply_serverless_migration(
                estimate, cluster.sku, state.serverless_utilization
            )
            modifications.append(
                SimulationModification(
                    name="Migrate to Serverless",
                    cost_multiplier=0.85,
                    is_verified=False,
                    rationale="Serverless compute migration",
                    trade_offs=["Pay-per-query model"],
                )
            )

        # --- Data source / Spark config extra multipliers ---
        for extra in state.extra_multipliers:
            estimate = CostEstimate(
                estimated_dbu=estimate.estimated_dbu,
                estimated_cost_usd=round(
                    (estimate.estimated_cost_usd or 0) * extra.multiplier, 4
                ),
                confidence=estimate.confidence,
                breakdown={
                    **estimate.breakdown,
                    "extra_optimization": extra.name,
                },
                warnings=list(estimate.warnings),
            )
            modifications.append(
                SimulationModification(
                    name=extra.name,
                    cost_multiplier=extra.multiplier,
                    is_verified=extra.is_verified,
                    rationale=extra.rationale,
                    trade_offs=extra.trade_offs,
                )
            )

        return estimate, modifications


class ClusterContext:
    """Context for cluster configuration changes."""

    def __init__(self, parent: Simulation):
        self._parent = parent

    def enable_photon(self, query_type: str = "complex_join") -> ClusterContext:
        """Enable Photon optimization."""
        state = self._parent._current_state()
        state.photon_query_type = query_type
        return self

    def disable_photon(self) -> ClusterContext:
        """Disable Photon optimization."""
        state = self._parent._current_state()
        state.photon_query_type = None
        return self

    def to_instance(self, instance_type: str) -> ClusterContext:
        """Change to a different instance type."""
        state = self._parent._current_state()
        state.target_instance = instance_type
        return self

    def use_spot(self, fallback: bool = True) -> ClusterContext:
        """Use spot instances."""
        state = self._parent._current_state()
        state.use_spot = True
        state.spot_fallback = fallback
        state.use_pool = False
        state.disable_spot_flag = False
        return self

    def disable_spot(self) -> ClusterContext:
        """Explicitly disable spot — force on-demand compute."""
        state = self._parent._current_state()
        state.disable_spot_flag = True
        state.use_spot = None
        state.use_pool = False
        return self

    def use_pool(
        self,
        instance_pool_id: str | None = None,
        use_spot: bool = False,
        min_idle: int = 0,
    ) -> ClusterContext:
        """Use an instance pool."""
        state = self._parent._current_state()
        state.use_pool = True
        state.pool_instance_pool_id = instance_pool_id
        state.pool_use_spot = use_spot
        state.pool_min_idle = min_idle
        state.use_spot = None
        return self

    def set_workers(self, count: int) -> ClusterContext:
        """Set number of workers."""
        state = self._parent._current_state()
        state.target_workers = count
        return self

    def to_serverless(self, utilization_pct: float = 50.0) -> ClusterContext:
        """Migrate to serverless compute."""
        state = self._parent._current_state()
        state.to_serverless = True
        state.serverless_utilization = utilization_pct
        return self

    def data_source(self) -> DataSourceContext:
        """Enter data source configuration context."""
        return DataSourceContext(self._parent)

    def spark_config(self) -> SparkConfigContext:
        """Enter Spark configuration context."""
        return SparkConfigContext(self._parent)

    def scenario(self, name: str) -> Simulation:
        """Switch to a new named scenario."""
        return self._parent.scenario(name)

    def compare(self) -> SimulationResult | MultiSimulationResult:
        """Compare scenarios and return result."""
        return self._parent.compare()


class DataSourceContext:
    """Context for data source optimization changes."""

    def __init__(self, parent: Simulation):
        self._parent = parent

    def _add_mod(
        self,
        name: str,
        multiplier: float,
        is_verified: bool,
        rationale: str,
        trade_offs: list[str],
    ) -> None:
        state = self._parent._current_state()
        state.extra_multipliers.append(
            _ExtraMod(
                name=name,
                multiplier=multiplier,
                is_verified=is_verified,
                rationale=rationale,
                trade_offs=trade_offs,
            )
        )

    def to_delta_format(self) -> DataSourceContext:
        """Migrate to Delta format."""
        self._add_mod(
            name="Delta Format",
            multiplier=COST_MULTIPLIERS["delta_format"],
            is_verified=False,
            rationale="Delta Lake format optimization",
            trade_offs=["Requires Delta Lake"],
        )
        return self

    def enable_liquid_clustering(self, keys: list[str]) -> DataSourceContext:
        """Enable Liquid Clustering."""
        self._add_mod(
            name="Liquid Clustering",
            multiplier=COST_MULTIPLIERS["liquid_clustering"],
            is_verified=False,
            rationale=f"Liquid clustering on {keys}",
            trade_offs=["Requires Delta Lake", "Best for high-cardinality columns"],
        )
        return self

    def set_partitioning(self, column: str) -> DataSourceContext:
        """Set partitioning on a column."""
        self._add_mod(
            name=f"Partition by {column}",
            multiplier=0.80,
            is_verified=False,
            rationale=f"Partition optimization on {column}",
            trade_offs=["Requires understanding of query patterns"],
        )
        return self

    def enable_disk_cache(self) -> DataSourceContext:
        """Enable disk cache."""
        self._add_mod(
            name="Disk Cache",
            multiplier=COST_MULTIPLIERS["disk_cache"],
            is_verified=False,
            rationale="Disk cache for repeated reads",
            trade_offs=["Only effective for repeated queries"],
        )
        return self

    def compact_files(self, target_mb: int = 128) -> DataSourceContext:
        """Compact small files."""
        self._add_mod(
            name=f"File Compaction ({target_mb}MB target)",
            multiplier=0.90,
            is_verified=False,
            rationale="Small file compaction",
            trade_offs=["One-time optimization cost"],
        )
        return self

    def enable_column_pruning(self) -> DataSourceContext:
        """Enable column pruning optimization."""
        self._add_mod(
            name="Column Pruning",
            multiplier=COST_MULTIPLIERS["column_pruning"],
            is_verified=False,
            rationale="Read only required columns",
            trade_offs=["Requires SELECT with specific columns"],
        )
        return self

    def enable_file_skipping(self) -> DataSourceContext:
        """Enable file skipping / predicate pushdown."""
        self._add_mod(
            name="File Skipping",
            multiplier=COST_MULTIPLIERS["file_skipping"],
            is_verified=False,
            rationale="Skip irrelevant data files",
            trade_offs=["Requires partitioned/clustered data"],
        )
        return self

    def set_compression(self, codec: str = "zstd") -> DataSourceContext:
        """Set compression codec."""
        self._add_mod(
            name=f"Compression ({codec})",
            multiplier=0.95,
            is_verified=False,
            rationale=f"Compression with {codec}",
            trade_offs=["CPU overhead for compression"],
        )
        return self

    def cluster(self) -> ClusterContext:
        """Enter cluster configuration context."""
        return ClusterContext(self._parent)

    def spark_config(self) -> SparkConfigContext:
        """Enter Spark configuration context."""
        return SparkConfigContext(self._parent)

    def scenario(self, name: str) -> Simulation:
        """Switch to a new named scenario."""
        return self._parent.scenario(name)

    def compare(self) -> SimulationResult | MultiSimulationResult:
        """Compare scenarios and return result."""
        return self._parent.compare()


class SparkConfigContext:
    """Context for Spark configuration changes."""

    def __init__(self, parent: Simulation):
        self._parent = parent

    def _add_mod(
        self,
        name: str,
        multiplier: float,
        is_verified: bool,
        rationale: str,
        trade_offs: list[str],
    ) -> None:
        state = self._parent._current_state()
        state.extra_multipliers.append(
            _ExtraMod(
                name=name,
                multiplier=multiplier,
                is_verified=is_verified,
                rationale=rationale,
                trade_offs=trade_offs,
            )
        )

    def with_shuffle_partitions(self, count: int) -> SparkConfigContext:
        """Set shuffle partitions count."""
        self._add_mod(
            name=f"Shuffle Partitions ({count})",
            multiplier=COST_MULTIPLIERS["shuffle_partitions"],
            is_verified=False,
            rationale=f"Optimized shuffle with {count} partitions",
            trade_offs=["May need tuning for specific workloads"],
        )
        return self

    def with_auto_shuffle_partitions(self) -> SparkConfigContext:
        """Enable automatic shuffle partitions."""
        self._add_mod(
            name="Auto Shuffle Partitions",
            multiplier=0.88,
            is_verified=False,
            rationale="Adaptive shuffle partition sizing",
            trade_offs=[],
        )
        return self

    def with_broadcast_threshold_mb(self, mb: int) -> SparkConfigContext:
        """Set broadcast join threshold."""
        self._add_mod(
            name=f"Broadcast Threshold ({mb}MB)",
            multiplier=COST_MULTIPLIERS["broadcast_join"],
            is_verified=False,
            rationale=f"Broadcast join threshold set to {mb}MB",
            trade_offs=["Memory pressure on drivers"],
        )
        return self

    def with_aqe_enabled(self, coalesce: bool = True) -> SparkConfigContext:
        """Enable Adaptive Query Execution."""
        self._add_mod(
            name="AQE Enabled",
            multiplier=COST_MULTIPLIERS["aqe"],
            is_verified=True,
            rationale="Adaptive Query Execution (verified by Databricks)",
            trade_offs=[],
        )
        return self

    def with_dynamic_allocation(
        self,
        enabled: bool = True,
        min_executors: int = 0,
        max_executors: int | None = None,
    ) -> SparkConfigContext:
        """Enable dynamic allocation. 0.80x cost for bursty workloads. Trade-off: scale-up latency."""
        if enabled:
            self._add_mod(
                name="Dynamic Allocation",
                multiplier=0.80,
                is_verified=False,
                rationale="Dynamic executor scaling for bursty workloads",
                trade_offs=["Scale-up latency when demand spikes"],
            )
        return self

    def with_max_partition_bytes_mb(self, mb: int = 128) -> SparkConfigContext:
        """Controls scan parallelism. No cost multiplier (structural setting)."""
        # Structural setting only — no cost multiplier applied
        return self

    def with_io_cache(self, enabled: bool = True) -> SparkConfigContext:
        """Databricks disk I/O cache. 0.15x for repeated scan workloads.
        Requires cache-optimized nodes (L-series Azure). No effect on write-heavy jobs."""
        if enabled:
            self._add_mod(
                name="I/O Cache",
                multiplier=COST_MULTIPLIERS["disk_cache"],
                is_verified=False,
                rationale="Databricks disk I/O cache for repeated scan workloads",
                trade_offs=[
                    "Requires cache-optimized nodes (L-series Azure)",
                    "No effect on write-heavy jobs",
                ],
            )
        return self

    def with_delta_optimize_write(self, enabled: bool = True) -> SparkConfigContext:
        """Delta auto-optimize write. 0.75x for read-heavy workflows. Trade-off: write latency."""
        if enabled:
            self._add_mod(
                name="Delta Optimize Write",
                multiplier=0.75,
                is_verified=False,
                rationale="Delta auto-optimize write for read-heavy workflows",
                trade_offs=["Increased write latency"],
            )
        return self

    def with_delta_auto_compact(self, enabled: bool = True) -> SparkConfigContext:
        """Delta auto-compaction. Saves on subsequent reads. Extra compute on write."""
        # Benefit is on subsequent reads; no direct multiplier on current workload
        return self

    def prefer_sort_merge_join(self, prefer: bool = False) -> SparkConfigContext:
        """prefer=False favors broadcast joins (0.70x for join-heavy on small dims)."""
        if not prefer:
            self._add_mod(
                name="Prefer Broadcast Join",
                multiplier=COST_MULTIPLIERS["broadcast_join"],
                is_verified=False,
                rationale="Broadcast joins favored over sort-merge for small dimension tables",
                trade_offs=["Memory pressure on drivers for large broadcast tables"],
            )
        return self

    def set(self, key: str, value: str | int | bool) -> SparkConfigContext:
        """Set arbitrary Spark configuration."""
        self._add_mod(
            name=f"Spark Config: {key}={value}",
            multiplier=1.0,
            is_verified=False,
            rationale="Custom Spark configuration",
            trade_offs=["Effect varies by configuration"],
        )
        return self

    def cluster(self) -> ClusterContext:
        """Enter cluster configuration context."""
        return ClusterContext(self._parent)

    def data_source(self) -> DataSourceContext:
        """Enter data source configuration context."""
        return DataSourceContext(self._parent)

    def scenario(self, name: str) -> Simulation:
        """Switch to a new named scenario."""
        return self._parent.scenario(name)

    def compare(self) -> SimulationResult | MultiSimulationResult:
        """Compare scenarios and return result."""
        return self._parent.compare()
