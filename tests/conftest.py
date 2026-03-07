import pytest


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "unit: fast isolated tests with no external dependencies"
    )
    config.addinivalue_line(
        "markers", "integration: tests requiring Databricks connectivity"
    )
    config.addinivalue_line("markers", "slow: tests taking >5s")


def pytest_collection_modifyitems(config, items):
    for item in items:
        if (
            item.get_closest_marker("unit") is None
            and item.get_closest_marker("integration") is None
            and "tests/unit" in str(item.path)
        ):
            item.add_marker(pytest.mark.unit)
