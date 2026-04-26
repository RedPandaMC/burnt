"""Auto-detection of execution context and backend selection."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .backend import Backend


def auto_backend() -> Backend | None:
    """Auto-detect execution context and return appropriate backend.

    Detection order:
    1. In-cluster (any Spark environment) - checks for active SparkSession
    2. External Databricks - checks DATABRICKS_HOST + auth credentials
    3. Offline mode - returns None (static estimation only)

    Returns:
        Backend instance if execution context detected, None for offline mode
    """
    # Try generic Spark first
    spark_backend = _create_spark_backend()
    if spark_backend is not None:
        return spark_backend

    # Try Databricks REST if credentials are present
    if os.environ.get("DATABRICKS_HOST"):
        return _create_rest_backend()

    return None


def _create_spark_backend() -> Backend | None:
    """Create SparkBackend from active SparkSession."""
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        return None

    spark = SparkSession.getActiveSession()
    if spark is None:
        return None

    from .spark_backend import SparkBackend

    return SparkBackend(spark)


def _create_rest_backend() -> Backend:
    """Create RestBackend using Databricks SDK with unified auth."""
    try:
        from databricks.sdk import WorkspaceClient
        from .rest_backend import RestBackend
    except ImportError as err:
        raise ImportError(
            "Databricks REST backend requires databricks-sdk. "
            "Install with: pip install burnt[databricks]"
        ) from err

    client = WorkspaceClient()
    return RestBackend(workspace_client=client)


def current_notebook_path() -> str | None:
    """Get the current notebook or script path.

    Detection order:
    1. SparkConf: spark.databricks.notebook.path (Databricks)
    2. dbutils: Notebook context (Databricks interactive)
    3. ipynbname: Local Jupyter notebooks
    4. inspect.stack(): Python scripts

    Returns:
        Path to current notebook/script, or None if undetectable
    """
    path = _get_spark_notebook_path()
    if path:
        return path

    path = _get_dbutils_notebook_path()
    if path:
        return path

    path = _get_ipynbname_path()
    if path:
        return path

    return _get_script_path()


def _get_spark_notebook_path() -> str | None:
    """Get path from SparkConf."""
    try:
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        if spark is not None:
            # Databricks-specific key
            path = spark.conf.get("spark.databricks.notebook.path", None)
            if path:
                return path
    except ImportError:
        pass

    return None


def _get_dbutils_notebook_path() -> str | None:
    """Get path from dbutils context."""
    try:
        from pyspark.dbutils import DBUtils
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        if spark is not None:
            dbutils = DBUtils(spark)
            return dbutils.notebook.getContext().notebookPath().get()
    except ImportError:
        pass
    except Exception:
        pass

    return None


def _get_ipynbname_path() -> str | None:
    """Get path from ipynbname (local Jupyter)."""
    try:
        import ipynbname

        return ipynbname.path()
    except ImportError:
        pass
    except Exception:
        pass

    return None


def _get_script_path() -> str | None:
    """Get script path using inspect."""
    import inspect

    for frame_info in inspect.stack():
        filename = frame_info.filename
        if filename and filename != "<stdin>" and filename.endswith(".py"):
            return os.path.abspath(filename)

    return None
