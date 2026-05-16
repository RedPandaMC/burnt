"""Compute-seconds normalizer and cost-component decomposition.

burnt reports findings with compute_seconds (executor RunTime).  Each
ProviderBackend turns those seconds into dollar amounts using its own model,
but all backends share this normalization layer so the pipeline is consistent.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ComputeComponents:
    """Decomposition of a workload into measurable compute units."""

    executor_seconds: float
    num_workers: int
    total_vcpus: int
    total_memory_gb: float
    shuffle_gb: float

    @classmethod
    def from_raw(
        cls,
        compute_seconds: float,
        *,
        instance_spec,  # InstanceSpec
        num_workers: int = 1,
        shuffle_bytes: int = 0,
    ) -> ComputeComponents:
        """Build ComputeComponents from raw runtime values.

        Args:
            compute_seconds: Total executor runtime in seconds.
            instance_spec: InstanceSpec for the cluster nodes.
            num_workers: Number of worker nodes.
            shuffle_bytes: Total shuffle bytes (read + write).
        """
        total_vcpus = instance_spec.total_vcpus(num_workers)
        total_memory_gb = instance_spec.total_memory_gb(num_workers)
        shuffle_gb = shuffle_bytes / 1e9
        return cls(
            executor_seconds=compute_seconds,
            num_workers=num_workers,
            total_vcpus=total_vcpus,
            total_memory_gb=total_memory_gb,
            shuffle_gb=shuffle_gb,
        )

    def vcpu_hours(self) -> float:
        """Total vCPU-hours consumed across the cluster."""
        return (self.executor_seconds / 3600.0) * self.total_vcpus

    def memory_gb_hours(self) -> float:
        """Total GB-hours consumed across the cluster."""
        return (self.executor_seconds / 3600.0) * self.total_memory_gb

    def executor_hours(self) -> float:
        """Node-hours (sum of all node executor times)."""
        return (self.executor_seconds / 3600.0) * self.num_workers
