def require(extra: str) -> None:
    """Raise ImportError if an optional dependency is not installed."""
    try:
        import importlib

        importlib.import_module(extra)
    except ImportError as e:
        raise ImportError(
            f"The '{extra}' extra is required. Install with: pip install burnt[{extra}]"
        ) from e
