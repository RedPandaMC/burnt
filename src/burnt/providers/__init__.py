"""Provider pricing backends for burnt.

Each sub-package implements ``ProviderBackend`` for a specific cloud x runtime
combination. Install the matching extra to enable dollar-cost mapping:

    pip install burnt[azure-databricks]
    pip install burnt[aws-databricks]
    pip install burnt[gcp-databricks]
    pip install burnt[onprem-spark]

The registry is auto-populated when each extra is imported.
"""

from __future__ import annotations

from . import (
    aws_databricks,
    azure_databricks,
    gcp_databricks,
    onprem_spark,
)
from .base import (
    InstanceSpec,
    ProviderBackend,
    get_backend,
    list_backends,
    register_backend,
)

__all__ = [
    "InstanceSpec",
    "ProviderBackend",
    "get_backend",
    "list_backends",
    "register_backend",
]
