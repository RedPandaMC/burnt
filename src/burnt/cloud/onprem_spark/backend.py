"""OnPrem Spark PricingBackend — pure arithmetic from user-supplied rates."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from burnt.core.config import OnPremSparkSettings
    from burnt.graph.model import CostGraph


class OnPremSparkBackend:
    """PricingBackend for self-hosted Spark — pure config arithmetic, no SDKs.

    Required config: onprem_spark.cost_per_vcpu_hour
    Optional config: onprem_spark.cost_per_gb_hour, onprem_spark.cost_per_gb_shuffle
    """

    name = "onprem-spark"

    def __init__(self, config: OnPremSparkSettings) -> None:
        from burnt.core.exceptions import ConfigError

        if config.cost_per_vcpu_hour is None:
            raise ConfigError("onprem_spark.cost_per_vcpu_hour is required")
        self._config = config

    def map(self, graph: CostGraph) -> object:
        """Estimate cost from CostGraph node metadata and configured rates."""
        from burnt.core.models import CostEstimate

        cfg = self._config
        total_usd = 0.0
        breakdown: dict[str, float] = {}

        # Bytes of data scanned by non-driver nodes
        input_bytes = sum(
            n.estimated_input_bytes
            for n in graph.nodes
            if n.estimated_input_bytes is not None and not n.driver_bound
        )
        data_gb = input_bytes / 1_073_741_824

        # Heuristic: assume 1 GB/s scan throughput per vCPU to derive run time
        scan_throughput_gbs = 1.0
        run_time_hours = (data_gb / scan_throughput_gbs) / 3600 if data_gb else 0.0

        active_nodes = [n for n in graph.nodes if not n.driver_bound]
        num_vcpus = len(active_nodes) if active_nodes else 1

        # vCPU cost
        cpu_cost = run_time_hours * num_vcpus * cfg.cost_per_vcpu_hour  # type: ignore[operator]
        if cpu_cost:
            breakdown["cpu"] = cpu_cost
        total_usd += cpu_cost

        # Memory cost (optional)
        if cfg.cost_per_gb_hour is not None and data_gb:
            mem_cost = data_gb * run_time_hours * cfg.cost_per_gb_hour
            if mem_cost:
                breakdown["memory"] = mem_cost
            total_usd += mem_cost

        # Shuffle cost (optional)
        if cfg.cost_per_gb_shuffle is not None:
            shuffle_bytes = sum(
                n.estimated_input_bytes
                for n in graph.nodes
                if n.shuffle_required and n.estimated_input_bytes is not None
            )
            shuffle_gb = shuffle_bytes / 1_073_741_824
            shuffle_cost = shuffle_gb * cfg.cost_per_gb_shuffle
            if shuffle_cost:
                breakdown["shuffle"] = shuffle_cost
            total_usd += shuffle_cost

        confidence = "low" if not input_bytes else "medium"
        costs = {"USD": round(total_usd, 6)} if total_usd else {}

        return CostEstimate(
            costs=costs,
            confidence=confidence,
            breakdown=breakdown,
        )
