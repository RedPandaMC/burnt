"""On-premises Spark cost configuration."""

from pydantic import BaseModel


class OnPremConfig(BaseModel):
    """On-premises Spark cost configuration.

    Loaded from burnt.toml::

        [burnt.pricing.onprem]
        cost_per_vcpu_hour = 0.048
        cost_per_gb_hour = 0.006
        cost_per_gb_shuffle = 0.001
        datacenter_overhead_pct = 15.0

    These rates are FinOps-style blended chargeback rates that include
    hardware CapEx, datacenter overhead (power, cooling, networking),
    and admin overhead.  Adjust to match your organisation's TCO.
    """

    cost_per_vcpu_hour: float = 0.048
    cost_per_gb_hour: float = 0.006
    cost_per_gb_shuffle: float = 0.001
    datacenter_overhead_pct: float = 15.0
