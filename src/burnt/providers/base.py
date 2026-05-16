"""Provider backend protocol and registry for burnt pricing backends."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:

    from burnt.core.models import CostEstimate


@runtime_checkable
class ProviderBackend(Protocol):
    """Protocol for pricing provider backends.

    All backends live under burnt.providers.* and implement this protocol.
    Install the matching extra to enable dollar-cost mapping:

        pip install burnt[azure-databricks]
        pip install burnt[aws-databricks]
        pip install burnt[gcp-databricks]
        pip install burnt[onprem-spark]

    Without any extra, compute_seconds are reported without dollar conversion.
    """

    name: str

    def estimate(
        self,
        compute_seconds: float,
        *,
        instance_type: str | None = None,
        num_workers: int = 1,
        region: str | None = None,
        sku: str | None = None,
        photon_enabled: bool = False,
        spot_policy: str = "ON_DEMAND",
        shuffle_bytes: int = 0,
        currency: str = "USD",
    ) -> CostEstimate:
        """Map compute seconds to a cost estimate.

        Args:
            compute_seconds: Total executor CPU+wall time in seconds.
            instance_type: Cloud instance type (e.g. Standard_DS3_v2, r5.xlarge).
            num_workers: Number of worker nodes in the cluster.
            region: Cloud region (e.g. eastus, us-east-1, us-central1).
            sku: Databricks compute SKU (e.g. ALL_PURPOSE, JOBS_COMPUTE).
            photon_enabled: Whether Photon runtime is enabled.
            spot_policy: Spot instance policy (ON_DEMAND, SPOT_WITH_ON_DEMAND_FALLBACK, SPOT).
            shuffle_bytes: Total shuffle bytes read+written.
            currency: Target currency code (USD, EUR, GBP, ...).

        Returns:
            CostEstimate with estimated_cost_usd and optionally estimated_cost_eur.
        """
        ...

    def resolve_instance(
        self, instance_type: str, region: str | None = None
    ) -> InstanceSpec | None:
        """Look up instance spec by type name.

        Args:
            instance_type: Cloud instance type identifier.
            region: Optional region for cloud-specific lookups.

        Returns:
            InstanceSpec or None if not found.
        """
        ...

    def refresh_cache(self) -> None:
        """Force-refresh the pricing cache from the API."""
        ...

    def is_available(self) -> bool:
        """Return True if the backend is functional (API reachable, credentials set, etc.)."""
        ...


class InstanceSpec:
    """Specification for a cloud VM instance type.

    Used by ProviderBackend.resolve_instance() to get vCPU, memory, and
    VM-cost-per-hour for cost calculations.
    """

    __slots__ = (
        "category",
        "dbu_rate",
        "instance_type",
        "local_storage_gb",
        "memory_gb",
        "photon_dbu_rate",
        "vcpus",
        "vm_cost_per_hour",
    )

    def __init__(
        self,
        instance_type: str,
        vcpus: int,
        memory_gb: float,
        local_storage_gb: float = 0.0,
        vm_cost_per_hour: float = 0.0,
        category: str = "general",
        dbu_rate: float = 0.0,
        photon_dbu_rate: float | None = None,
    ):
        self.instance_type = instance_type
        self.vcpus = vcpus
        self.memory_gb = memory_gb
        self.local_storage_gb = local_storage_gb
        self.vm_cost_per_hour = vm_cost_per_hour
        self.category = category
        self.dbu_rate = dbu_rate
        self.photon_dbu_rate = (
            photon_dbu_rate if photon_dbu_rate is not None else dbu_rate * 2.5
        )

    def total_vcpus(self, num_workers: int) -> int:
        return self.vcpus * num_workers

    def total_memory_gb(self, num_workers: int) -> float:
        return self.memory_gb * num_workers

    def __repr__(self) -> str:
        return (
            f"InstanceSpec(type={self.instance_type}, vcpus={self.vcpus}, "
            f"memory_gb={self.memory_gb}, dbu_rate={self.dbu_rate})"
        )


_BACKENDS: dict[str, type[ProviderBackend]] = {}


def register_backend(name: str, cls: type[ProviderBackend]) -> None:
    """Register a ProviderBackend class by name."""
    _BACKENDS[name] = cls


def get_backend(name: str) -> ProviderBackend | None:
    """Instantiate and return a registered backend by name."""
    cls = _BACKENDS.get(name)
    if cls is None:
        return None
    return cls()


def list_backends() -> list[str]:
    """Return sorted list of registered backend names."""
    return sorted(_BACKENDS.keys())
