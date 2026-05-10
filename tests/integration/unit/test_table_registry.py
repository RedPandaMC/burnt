"""Unit tests for TableRegistry."""


import pytest

from burnt.core.table_registry import TableRegistry


class TestTableRegistry:
    """Test the TableRegistry class."""

    def test_default_values(self):
        """Test that default values are set correctly."""
        registry = TableRegistry()

        assert registry.billing_usage == "system.billing.usage"
        assert registry.billing_list_prices == "system.billing.list_prices"
        assert registry.query_history == "system.query.history"
        assert registry.compute_node_types == "system.compute.node_types"
        assert registry.compute_clusters == "system.compute.clusters"
        assert registry.compute_node_timeline == "system.compute.node_timeline"
        assert registry.lakeflow_jobs == "system.lakeflow.jobs"
        assert registry.lakeflow_job_run_timeline == "system.lakeflow.job_run_timeline"
        assert (
            registry.predictive_optimization
            == "system.storage.predictive_optimization_operations_history"
        )

    def test_custom_values(self):
        """Test that custom values can be set."""
        registry = TableRegistry(
            billing_usage="governance.cost.v_billing",
            query_history="governance.query.v_history",
        )

        assert registry.billing_usage == "governance.cost.v_billing"
        assert registry.query_history == "governance.query.v_history"
        # Other values should be defaults
        assert registry.billing_list_prices == "system.billing.list_prices"

    def test_from_env_no_overrides(self, monkeypatch):
        """Test that from_env uses defaults when no env vars set."""
        # Clear any existing env vars
        for key in [
            "BURNT_TABLE_BILLING_USAGE",
            "BURNT_TABLE_BILLING_LIST_PRICES",
            "BURNT_TABLE_QUERY_HISTORY",
        ]:
            monkeypatch.delenv(key, raising=False)

        registry = TableRegistry.from_env()

        assert registry.billing_usage == "system.billing.usage"
        assert registry.query_history == "system.query.history"

    def test_from_env_with_overrides(self, monkeypatch):
        """Test that from_env loads from environment variables."""
        monkeypatch.setenv("BURNT_TABLE_BILLING_USAGE", "governance.cost.v_billing")
        monkeypatch.setenv(
            "BURNT_TABLE_BILLING_LIST_PRICES", "governance.cost.v_prices"
        )
        monkeypatch.setenv("BURNT_TABLE_QUERY_HISTORY", "governance.query.v_history")

        registry = TableRegistry.from_env()

        assert registry.billing_usage == "governance.cost.v_billing"
        assert registry.billing_list_prices == "governance.cost.v_prices"
        assert registry.query_history == "governance.query.v_history"
        # Other values should be defaults
        assert registry.compute_node_types == "system.compute.node_types"

    def test_to_sqlite_table_name(self):
        """Test conversion to SQLite-safe table names."""
        registry = TableRegistry()

        assert (
            registry.to_sqlite_table_name("system.billing.usage")
            == "system_billing_usage"
        )
        assert (
            registry.to_sqlite_table_name("system.query.history")
            == "system_query_history"
        )
        assert (
            registry.to_sqlite_table_name("governance.cost.v_billing")
            == "governance_cost_v_billing"
        )

    def test_format_sql(self):
        """Test SQL formatting with custom table paths."""
        registry = TableRegistry(
            billing_usage="governance.cost.v_billing",
            query_history="governance.query.v_history",
        )

        sql = "SELECT * FROM system.billing.usage WHERE sku_name = 'test'"
        formatted = registry.format_sql(sql)
        assert (
            formatted
            == "SELECT * FROM governance.cost.v_billing WHERE sku_name = 'test'"
        )

        sql = "SELECT * FROM system.query.history LIMIT 10"
        formatted = registry.format_sql(sql)
        assert formatted == "SELECT * FROM governance.query.v_history LIMIT 10"

        # Tables that weren't overridden should remain unchanged
        sql = "SELECT * FROM system.billing.list_prices"
        formatted = registry.format_sql(sql)
        assert formatted == "SELECT * FROM system.billing.list_prices"

    def test_format_sql_multiple_replacements(self):
        """Test SQL formatting with multiple table references."""
        registry = TableRegistry(
            billing_usage="gov.billing",
            query_history="gov.queries",
        )

        sql = """
            SELECT u.*, h.statement_text
            FROM system.billing.usage u
            JOIN system.query.history h ON u.usage_start_time = h.start_time
            WHERE u.sku_name = 'test'
        """
        formatted = registry.format_sql(sql)

        assert "FROM gov.billing u" in formatted
        assert "JOIN gov.queries h" in formatted

    def test_with_overrides(self):
        """Test creating registry with column overrides."""
        registry = TableRegistry()

        new_registry = registry.with_overrides(
            {"billing_usage": {"custom_sku": "sku_name"}}
        )

        assert new_registry.column_overrides == {
            "billing_usage": {"custom_sku": "sku_name"}
        }
        # Original registry should be unchanged
        assert registry.column_overrides == {}

    def test_with_overrides_merge(self):
        """Test that with_overrides merges with existing overrides."""
        registry = TableRegistry()
        registry = registry.with_overrides({"billing_usage": {"old_col": "new_col"}})
        registry = registry.with_overrides({"billing_usage": {"another": "col"}})

        assert registry.column_overrides["billing_usage"] == {
            "old_col": "new_col",
            "another": "col",
        }

    def test_frozen_immutable(self):
        """Test that the dataclass is frozen/immutable."""
        registry = TableRegistry()

        with pytest.raises(AttributeError):
            registry.billing_usage = "custom.path"
