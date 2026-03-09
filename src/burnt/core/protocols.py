"""Protocol classes for burnt."""

from __future__ import annotations

from datetime import date  # noqa: TC003
from decimal import Decimal  # noqa: TC003
from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from .models import CostEstimate


@runtime_checkable
class Estimator(Protocol):
    """Protocol for cost estimators."""

    def estimate(self, query: str, **kwargs: object) -> CostEstimate:
        """Estimate cost for a query."""
        ...


@runtime_checkable
class ExchangeRateProvider(Protocol):
    """Protocol for exchange rate providers."""

    def get_rate(self, date: date, from_currency: str, to_currency: str) -> Decimal:
        """Get exchange rate between currencies."""
        ...
