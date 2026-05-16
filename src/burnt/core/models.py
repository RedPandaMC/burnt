"""Pydantic models for burnt."""

from __future__ import annotations

import logging
from typing import Any

from pydantic import BaseModel, ConfigDict

from .enums import CloudProvider, Confidence, Sku, SpotPolicy

logger = logging.getLogger(__name__)


class ClusterConfig(BaseModel):
    """Databricks cluster configuration."""

    model_config = ConfigDict(frozen=True)

    instance_type: str = "Standard_DS3_v2"
    num_workers: int = 2
    dbu_per_hour: float = 0.75
    photon_enabled: bool = False
    sku: Sku = Sku.ALL_PURPOSE
    spot_policy: SpotPolicy = SpotPolicy.ON_DEMAND
    autoscale_min_workers: int | None = None
    autoscale_max_workers: int | None = None

    def to_dab(
        self, name: str = "cluster", spark_version: str = "15.4.x-scala2.12"
    ) -> str:
        """Render as a Databricks Asset Bundle YAML cluster resource."""
        import yaml

        cluster: dict[str, Any] = {
            "node_type_id": self.instance_type,
            "num_workers": self.num_workers,
            "spark_version": spark_version,
            "runtime_engine": "PHOTON" if self.photon_enabled else "STANDARD",
        }
        if (
            self.autoscale_min_workers is not None
            and self.autoscale_max_workers is not None
        ):
            cluster["autoscale"] = {
                "min_workers": self.autoscale_min_workers,
                "max_workers": self.autoscale_max_workers,
            }
        dab_dict = {"resources": {"clusters": {name: cluster}}}
        return yaml.dump(dab_dict, default_flow_style=False, sort_keys=False)


class ClusterProfile(BaseModel):
    """Full cluster configuration including runtime context used by the estimation pipeline."""

    config: ClusterConfig
    driver_node_type: str | None = None
    spark_version: str | None = None
    custom_spark_conf: dict[str, str] = {}
    cluster_tags: dict[str, str] = {}
    instance_pool_id: str | None = None
    instance_pool_max_capacity: int | None = None
    cloud_provider: CloudProvider = CloudProvider.AZURE

    @classmethod
    def from_databricks_json(cls, payload: dict) -> ClusterProfile:
        """Populate both config (via ClusterConfig.from_databricks_json) and extended fields."""
        cluster = payload.get("new_cluster", payload)
        config = ClusterConfig.from_databricks_json(payload)
        return cls(
            config=config,
            driver_node_type=cluster.get("driver_node_type_id"),
            spark_version=cluster.get("spark_version"),
            custom_spark_conf=cluster.get("spark_conf", {}),
            cluster_tags=cluster.get("custom_tags", {}),
            instance_pool_id=cluster.get("instance_pool_id"),
            instance_pool_max_capacity=cluster.get("instance_pool_max_capacity"),
            cloud_provider="AZURE",
        )


class PricingInfo(BaseModel):
    """Pricing information for a SKU."""

    sku_name: str
    dbu_rate: float
    cloud: str = "AZURE"
    region: str = "EAST_US"


class CostEstimate(BaseModel):
    """Cost estimate for a query or workload.

    All currency amounts live in ``costs`` — a plain dict keyed by ISO 4217 code.
    Use ``cost_in`` for direct lookup (no I/O, no conversion).
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    estimated_dbu: float | None = None
    costs: dict[str, float] = {}
    confidence: Confidence = Confidence.LOW
    breakdown: dict[str, Any] = {}
    warnings: list[str] = []

    def cost_in(self, currency: str) -> float | None:
        """Return the pre-computed cost in *currency*, or None if unavailable."""
        return self.costs.get(currency.upper())
