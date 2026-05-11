"""Azure Databricks PricingBackend."""

try:
    from .backend import AzureDatabricksBackend

    __all__ = ["AzureDatabricksBackend"]
except ImportError:

    def AzureDatabricksBackend(*args, **kwargs):  # type: ignore[misc]
        from burnt.core.exceptions import NotAvailableError

        raise NotAvailableError(
            "AzureDatabricksBackend requires: pip install burnt[azure-databricks]"
        )

    __all__ = ["AzureDatabricksBackend"]
