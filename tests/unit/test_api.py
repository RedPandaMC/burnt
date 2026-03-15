import pytest

import burnt
from burnt.core.models import ClusterConfig, CostEstimate


def test_estimate_sql_string():
    cost = burnt.estimate("SELECT * FROM test")
    assert isinstance(cost, CostEstimate)
    assert cost.estimated_dbu > 0


def test_estimate_with_custom_cluster():
    cluster = ClusterConfig(
        instance_type="Standard_DS4_v2", num_workers=4, dbu_per_hour=1.5
    )
    cost = burnt.estimate("SELECT * FROM test", cluster=cluster)
    assert isinstance(cost, CostEstimate)


def test_estimate_file(tmp_path):
    f = tmp_path / "test.sql"
    f.write_text("SELECT * FROM test")
    # estimate() now accepts Path objects directly
    cost = burnt.estimate(f)
    assert isinstance(cost, CostEstimate)


def test_estimate_string_path(tmp_path):
    f = tmp_path / "test.sql"
    f.write_text("SELECT * FROM test")
    # estimate() accepts string path ending in .sql
    cost = burnt.estimate(str(f))
    assert isinstance(cost, CostEstimate)


def test_advise_no_context():
    # advise() with no args attempts current session → raises RuntimeError outside Databricks
    with pytest.raises(RuntimeError, match="No Databricks execution context"):
        burnt.advise()


def test_simulation_in_all():
    assert "Simulation" in dir(burnt)
    assert "SimulationResult" in dir(burnt)
    assert "MultiSimulationResult" in dir(burnt)
    assert "SimulationModification" in dir(burnt)


def test_removed_exports():
    assert not hasattr(burnt, "what_if")
    assert not hasattr(burnt, "compare")
    assert not hasattr(burnt, "estimate_file")
    assert not hasattr(burnt, "advise_current_session")
    assert not hasattr(burnt, "lint")
    assert not hasattr(burnt, "lint_file")
    assert not hasattr(burnt, "get_cluster_json")
