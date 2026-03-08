import pytest

import dburnrate
from dburnrate.core.models import ClusterConfig, CostEstimate


def test_lint_sql_string():
    issues = dburnrate.lint("SELECT * FROM a CROSS JOIN b")
    assert len(issues) > 0
    assert any(i.name == "cross_join" for i in issues)


def test_lint_file(tmp_path):
    f = tmp_path / "test.sql"
    f.write_text("SELECT * FROM a CROSS JOIN b")
    issues = dburnrate.lint_file(f)
    assert len(issues) > 0


def test_lint_file_not_found():
    with pytest.raises(FileNotFoundError):
        dburnrate.lint_file("does_not_exist.sql")


def test_estimate_sql_string():
    cost = dburnrate.estimate("SELECT * FROM test")
    assert isinstance(cost, CostEstimate)
    assert cost.estimated_dbu > 0


def test_estimate_with_custom_cluster():
    cluster = ClusterConfig(
        instance_type="Standard_DS4_v2", num_workers=4, dbu_per_hour=1.5
    )
    cost = dburnrate.estimate("SELECT * FROM test", cluster=cluster)
    assert isinstance(cost, CostEstimate)


def test_estimate_file(tmp_path):
    f = tmp_path / "test.sql"
    f.write_text("SELECT * FROM test")
    cost = dburnrate.estimate_file(f)
    assert isinstance(cost, CostEstimate)


def test_advise_current_session_stubbed():
    with pytest.raises(NotImplementedError):
        dburnrate.advise_current_session()
