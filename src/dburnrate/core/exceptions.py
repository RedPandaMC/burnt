"""Core exceptions for dburnrate."""


class DburnrateError(Exception):
    """Base exception for all dburnrate errors."""

    pass


class ParseError(DburnrateError):
    """Raised when parsing fails."""

    pass


class ConfigError(DburnrateError):
    """Raised when configuration is invalid."""

    pass


class PricingError(DburnrateError):
    """Raised when pricing lookup fails."""

    pass


class EstimationError(DburnrateError):
    """Raised when cost estimation fails."""

    pass


class DatabricksConnectionError(DburnrateError):
    """Raised when connection to Databricks workspace fails."""

    pass


class DatabricksQueryError(DburnrateError):
    """Raised when a SQL statement execution fails on Databricks."""

    pass
