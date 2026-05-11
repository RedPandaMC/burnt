"""AWS Databricks PricingBackend."""

try:
    from .backend import AwsDatabricksBackend

    __all__ = ["AwsDatabricksBackend"]
except ImportError:

    def AwsDatabricksBackend(*args, **kwargs):  # type: ignore[misc]
        from burnt.core.exceptions import NotAvailableError

        raise NotAvailableError(
            "AwsDatabricksBackend requires: pip install burnt[aws-databricks]"
        )

    __all__ = ["AwsDatabricksBackend"]
