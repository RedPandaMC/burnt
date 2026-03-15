"""Core exceptions for burnt."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from burnt.core.models import CostEstimate


class BurntError(Exception):
    """Base exception for all burnt errors."""

    pass


class ParseError(BurntError):
    """Raised when parsing fails."""

    pass


class ConfigError(BurntError):
    """Raised when configuration is invalid."""

    pass


class PricingError(BurntError):
    """Raised when pricing lookup fails."""

    pass


class EstimationError(BurntError):
    """Raised when cost estimation fails."""

    pass


class DatabricksConnectionError(BurntError):
    """Raised when connection to Databricks workspace fails."""

    pass


class DatabricksQueryError(BurntError):
    """Raised when a SQL statement execution fails on Databricks."""


class NotAvailableError(BurntError):
    """Raised when a feature is not available in the current execution context."""

    pass


class CostBudgetExceeded(BurntError):
    """Raised when estimated cost exceeds the user-specified budget."""

    def __init__(
        self,
        estimate: CostEstimate,
        budget: float,
        label: str = "",
        *,
        currency: str = "USD",
    ):
        self.estimate = estimate
        self.budget = budget
        self.currency = currency
        self.label = label
        estimate_cost = estimate.estimated_cost_usd
        msg = (
            f"Estimated cost ${estimate_cost:.2f} exceeds "
            f"budget {currency} ${budget:.2f}"
        )
        if label:
            msg += f" ({label})"
        super().__init__(msg)
