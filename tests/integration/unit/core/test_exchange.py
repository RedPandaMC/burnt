from datetime import date, timedelta
from decimal import Decimal
from unittest.mock import Mock, patch

import pytest
import requests

from burnt.core.exceptions import PricingError
from burnt.core.exchange import FixedRateProvider, FrankfurterProvider


class TestFrankfurterProvider:
    def test_get_rate_same_currency(self):
        provider = FrankfurterProvider()
        rate = provider.get_rate(date.today(), "USD", "USD")
        assert rate == Decimal("1")

    @patch.object(requests, "get")
    def test_get_rate_success(self, mock_get):
        mock_response = Mock()
        mock_response.json.return_value = {"rates": {"EUR": "0.92"}}
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        provider = FrankfurterProvider()
        rate = provider.get_rate(date.today() + timedelta(days=1), "USD", "EUR")
        assert rate == Decimal("0.92")

    @patch.object(requests, "get")
    def test_get_rate_api_error(self, mock_get):
        mock_get.side_effect = Exception("Network error")

        provider = FrankfurterProvider()
        with pytest.raises(PricingError) as exc_info:
            provider.get_rate(date.today() + timedelta(days=1), "USD", "EUR")
        assert "Failed to get exchange rate" in str(exc_info.value)

    @patch.object(requests, "get")
    def test_get_rate_for_amount(self, mock_get):
        mock_response = Mock()
        mock_response.json.return_value = {"rates": {"EUR": "0.92"}}
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        provider = FrankfurterProvider()
        result = provider.get_rate_for_amount(
            Decimal("100"), date.today() + timedelta(days=1), "USD", "EUR"
        )
        assert result == Decimal("92")

    def test_get_rate_weekday_adjustment(self):
        saturday = date(2024, 1, 6)
        assert saturday.weekday() == 5

        adjusted = saturday - timedelta(days=saturday.weekday() - 4)
        assert adjusted.weekday() == 4


class TestFixedRateProvider:
    def test_fixed_rate_creation(self):
        provider = FixedRateProvider(Decimal("0.85"))
        assert provider._rate == Decimal("0.85")

    def test_get_rate_same_currency(self):
        provider = FixedRateProvider(Decimal("0.85"))
        rate = provider.get_rate(date.today(), "USD", "USD")
        assert rate == Decimal("1")

    def test_get_rate_different_currency(self):
        provider = FixedRateProvider(Decimal("0.85"))
        rate = provider.get_rate(date.today(), "USD", "EUR")
        assert rate == Decimal("0.85")
