"""GCP Databricks PricingBackend."""

try:
    from .backend import GcpDatabricksBackend

    __all__ = ["GcpDatabricksBackend"]
except ImportError:

    def GcpDatabricksBackend(*args, **kwargs):  # type: ignore[misc]
        from burnt.core.exceptions import NotAvailableError

        raise NotAvailableError(
            "GcpDatabricksBackend requires: pip install burnt[gcp-databricks]"
        )

    __all__ = ["GcpDatabricksBackend"]
