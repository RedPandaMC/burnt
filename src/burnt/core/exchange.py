"""Exchange rate providers for currency conversion."""

from datetime import date, timedelta
from decimal import Decimal
from functools import lru_cache

from .exceptions import PricingError


class FrankfurterProvider:
    """Exchange rate provider using Frankfurter API."""

    BASE_URL = "https://api.frankfurter.dev/v1"

    def __init__(self) -> None:
        """Initialize provider with a per-instance LRU cache."""
        self.get_rate = lru_cache(maxsize=30)(self._get_rate)

    def _get_rate(self, target_date: date, from_curr: str, to_curr: str) -> Decimal:
        """Fetch exchange rate from Frankfurter API."""
        if from_curr == to_curr:
            return Decimal("1")

        if target_date.weekday() >= 5:
            target_date = target_date - timedelta(days=target_date.weekday() - 4)

        try:
            import requests

            resp = requests.get(
                f"{self.BASE_URL}/{target_date.isoformat()}",
                params={"base": from_curr, "symbols": to_curr},
                timeout=10,
            )
            resp.raise_for_status()
            rates = resp.json()["rates"]
            return Decimal(str(rates[to_curr]))
        except Exception as e:
            raise PricingError(f"Failed to get exchange rate: {e}") from e

    def get_rate_for_amount(
        self,
        amount: Decimal,
        target_date: date,
        from_curr: str = "USD",
        to_curr: str = "EUR",
    ) -> Decimal:
        """Convert amount between currencies."""
        rate = self.get_rate(target_date, from_curr, to_curr)
        return amount * rate


class FixedRateProvider:
    """Fixed exchange rate provider for testing."""

    def __init__(self, rate: Decimal):
        """Initialize with a fixed rate."""
        self._rate = rate

    def get_rate(self, target_date: date, from_curr: str, to_curr: str) -> Decimal:
        """Get fixed exchange rate."""
        if from_curr == to_curr:
            return Decimal("1")
        return self._rate
