"""Instance catalog for Azure."""

from __future__ import annotations

from pydantic import BaseModel


class InstanceInfo(BaseModel):
    """Information about a VM instance."""

    name: str
    vcpus: int
    memory_gb: int
    dbu_rate: float
    category: str = "general"
    storage_gb: int | None = None


AZURE_INSTANCE_CATALOG: dict[str, InstanceInfo] = {
    "Standard_DS3_v2": InstanceInfo(
        name="Standard_DS3_v2",
        vcpus=4,
        memory_gb=14,
        dbu_rate=0.75,
        category="general",
    ),
    "Standard_DS4_v2": InstanceInfo(
        name="Standard_DS4_v2",
        vcpus=8,
        memory_gb=28,
        dbu_rate=1.5,
        category="general",
    ),
    "Standard_DS5_v2": InstanceInfo(
        name="Standard_DS5_v2",
        vcpus=16,
        memory_gb=56,
        dbu_rate=3.0,
        category="general",
    ),
    "Standard_E8_v3": InstanceInfo(
        name="Standard_E8_v3",
        vcpus=8,
        memory_gb=64,
        dbu_rate=2.0,
        category="memory",
    ),
    "Standard_E16_v3": InstanceInfo(
        name="Standard_E16_v3",
        vcpus=16,
        memory_gb=128,
        dbu_rate=4.0,
        category="memory",
    ),
}


def lookup_instance(node_type: str) -> InstanceInfo | None:
    """Look up an instance by node type.

    Args:
        node_type: The Azure VM size (e.g., "Standard_DS3_v2").

    Returns:
        Instance information or None if not found.
    """
    return AZURE_INSTANCE_CATALOG.get(node_type)
