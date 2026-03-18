"""Pytest configuration for integration tests.

Provides fixtures for Databricks connectivity with SQLite fallback.
"""

from __future__ import annotations

import pytest


@pytest.fixture(scope="session")
def settings():
    """Load settings from environment."""
    from burnt.core.config import Settings

    return Settings()


@pytest.fixture
def databricks_client(settings):
    """Provide a Databricks client for integration tests.

    If BURNT_WORKSPACE_URL is set, returns a real DatabricksClient.
    Otherwise, returns a SQLiteBackend mock for local testing.

    Usage:
        def test_something(databricks_client):
            queries = databricks_client.get_recent_queries(limit=10)
            assert len(queries) > 0
    """
    from burnt.core.config import Settings

    settings = Settings()

    if settings.workspace_url:
        # Real Databricks connection
        try:
            from burnt.tables.connection import DatabricksClient

            return DatabricksClient(settings)
        except Exception as e:
            pytest.skip(f"Failed to create Databricks client: {e}")
    else:
        # SQLite mock for local testing
        from tests.fixtures.mock_backend import create_mock_backend

        backend = create_mock_backend(scale_factor=1)
        yield backend
        backend.close()


@pytest.fixture
def require_databricks(settings):
    """Skip test if no Databricks connection available.

    Usage:
        def test_live_query(require_databricks, databricks_client):
            # This test only runs with real Databricks
            queries = databricks_client.get_recent_queries()
            ...
    """
    if not settings.workspace_url:
        pytest.skip(
            "No Databricks connection (set BURNT_WORKSPACE_URL and BURNT_TOKEN)"
        )


@pytest.fixture
def mock_backend_only():
    """Always provide a SQLite mock backend (no real Databricks)."""
    from tests.fixtures.mock_backend import create_mock_backend

    backend = create_mock_backend(scale_factor=1)
    yield backend
    backend.close()
