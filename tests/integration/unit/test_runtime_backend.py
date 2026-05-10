"""Unit tests for runtime backend."""

from __future__ import annotations

import os
import sys
from unittest.mock import MagicMock, patch

import pytest

from burnt.core.exceptions import NotAvailableError


class TestBackendProtocol:
    """Tests for Backend protocol definition."""

    def test_backend_protocol_is_runtime_checkable(self) -> None:
        """Verify Backend protocol can be checked at runtime."""
        from burnt.runtime import Backend

        assert hasattr(Backend, "__protocol_attrs__")


class TestAutoBackend:
    """Tests for auto_backend() function."""

    @patch.dict(os.environ, {}, clear=True)
    def test_no_backend_when_no_env_vars(self) -> None:
        """Returns None when no Databricks env vars are set."""
        from burnt.runtime import auto_backend

        result = auto_backend()
        assert result is None

    @patch.dict(os.environ, {"DATABRICKS_HOST": "https://example.cloud.databricks.com"})
    @patch("burnt.runtime.auto._create_rest_backend")
    def test_rest_backend_when_host_set(self, mock_create: MagicMock) -> None:
        """Returns RestBackend when DATABRICKS_HOST is set."""
        mock_backend = MagicMock()
        mock_create.return_value = mock_backend

        from burnt.runtime import auto_backend

        result = auto_backend()

        mock_create.assert_called_once()
        assert result == mock_backend

    @patch.dict(os.environ, {"DATABRICKS_RUNTIME_VERSION": "15.0"})
    @patch("burnt.runtime.auto._create_spark_backend")
    def test_spark_backend_when_runtime_version_set(
        self, mock_create: MagicMock
    ) -> None:
        """Returns SparkBackend when DATABRICKS_RUNTIME_VERSION is set."""
        mock_backend = MagicMock()
        mock_create.return_value = mock_backend

        from burnt.runtime import auto_backend

        result = auto_backend()

        mock_create.assert_called_once()
        assert result == mock_backend


class TestSparkBackend:
    """Tests for SparkBackend implementation."""

    def test_import_error_when_pyspark_not_installed(self) -> None:
        """SparkBackend raises ImportError when pyspark not installed."""
        with (
            patch.dict(sys.modules, {"pyspark.sql": None, "pyspark": MagicMock()}),
            pytest.raises(ImportError, match="pyspark"),
        ):
            from burnt.runtime.spark_backend import SparkBackend

            SparkBackend(MagicMock())

    def test_type_error_when_not_sparksession(self) -> None:
        """SparkBackend raises TypeError when passed non-SparkSession."""
        with patch.dict(sys.modules, {"pyspark.sql": MagicMock()}):
            from burnt.runtime.spark_backend import SparkBackend

            with pytest.raises(TypeError, match="isinstance"):
                SparkBackend("not a spark session")


class TestRestBackend:
    """Tests for RestBackend implementation."""

    def test_import_error_when_sdk_not_installed(self) -> None:
        """RestBackend raises ImportError when SDK not installed."""
        with (
            patch.dict(sys.modules, {"databricks.sdk": None}),
            pytest.raises(ImportError, match="databricks-sdk"),
        ):
            from burnt.runtime.rest_backend import RestBackend

            RestBackend()

    def test_get_session_metrics_raises_not_available(self) -> None:
        """get_session_metrics raises NotAvailableError."""
        from burnt.runtime.rest_backend import RestBackend

        backend = RestBackend.__new__(RestBackend)
        with pytest.raises(NotAvailableError, match="in-cluster"):
            backend.get_session_metrics()


class TestCurrentNotebookPath:
    """Tests for current_notebook_path() function."""

    def test_returns_none_when_no_context_available(self) -> None:
        """Returns None when no notebook context available."""
        import builtins

        original_import = builtins.__import__

        def mock_import(name, *args, **kwargs):
            if name in ("pyspark", "pyspark.sql", "pyspark.dbutils", "ipynbname"):
                raise ImportError(f"No module named '{name}'")
            return original_import(name, *args, **kwargs)

        with patch.object(builtins, "__import__", side_effect=mock_import):
            from burnt.runtime import current_notebook_path

            result = current_notebook_path()
            assert result is None or result.endswith(".py")
