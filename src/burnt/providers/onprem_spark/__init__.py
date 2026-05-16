"""On-premises Spark pricing backend — no external dependencies, pure config."""

from __future__ import annotations

from .backend import OnPremSparkBackend, compute_onprem_cost
from .config import OnPremConfig

__all__ = [
    "OnPremConfig",
    "OnPremSparkBackend",
    "compute_onprem_cost",
]
