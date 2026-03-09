"""Core exceptions for burnt."""


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

    pass
