"""Unit tests for `burnt doctor` command."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from burnt.cli.main import app

runner = CliRunner()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _invoke(*args: str) -> object:
    return runner.invoke(app, ["doctor", *args])


# ---------------------------------------------------------------------------
# 1. Always exits 0
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_doctor_exit_code_zero():
    result = _invoke()
    assert result.exit_code == 0


# ---------------------------------------------------------------------------
# 2. No credentials / no config / no cache
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_doctor_no_credentials(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    for var in (
        "DATABRICKS_HOST",
        "DATABRICKS_WORKSPACE_URL",
        "BURNT_WORKSPACE_URL",
        "DATABRICKS_TOKEN",
        "BURNT_TOKEN",
    ):
        monkeypatch.delenv(var, raising=False)

    result = _invoke()

    assert result.exit_code == 0
    assert "NOT SET" in result.output
    assert "SKIP" in result.output
    assert "NOT FOUND" in result.output


# ---------------------------------------------------------------------------
# 3. Connection test outcomes
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_doctor_connection_ok(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DATABRICKS_HOST", "https://adb-test.azuredatabricks.net")
    monkeypatch.setenv("DATABRICKS_TOKEN", "dapiABCDEF1234")

    ok_resp = MagicMock()
    ok_resp.status_code = 200
    ok_resp.json.return_value = {"warehouses": []}

    with patch("requests.get", return_value=ok_resp):
        result = _invoke()

    assert result.exit_code == 0
    assert "workspace reachable" in result.output


@pytest.mark.unit
def test_doctor_connection_timeout(tmp_path, monkeypatch):
    import requests as req_mod

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DATABRICKS_HOST", "https://adb-test.azuredatabricks.net")
    monkeypatch.setenv("DATABRICKS_TOKEN", "dapiABCDEF1234")

    with patch("requests.get", side_effect=req_mod.Timeout):
        result = _invoke()

    assert result.exit_code == 0
    assert "TIMEOUT" in result.output


@pytest.mark.unit
def test_doctor_connection_auth_error(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DATABRICKS_HOST", "https://adb-test.azuredatabricks.net")
    monkeypatch.setenv("DATABRICKS_TOKEN", "dapiABCDEF1234")

    auth_resp = MagicMock()
    auth_resp.status_code = 401
    auth_resp.json.return_value = {"warehouses": []}

    with patch("requests.get", return_value=auth_resp):
        result = _invoke()

    assert result.exit_code == 0
    assert "AUTH ERROR" in result.output


# ---------------------------------------------------------------------------
# 4. Token redaction
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_doctor_token_redacted(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DATABRICKS_HOST", "https://adb-test.azuredatabricks.net")
    monkeypatch.setenv("DATABRICKS_TOKEN", "dapiSECRETLONGTOKEN")

    ok_resp = MagicMock()
    ok_resp.status_code = 200
    ok_resp.json.return_value = {"warehouses": []}

    with patch("requests.get", return_value=ok_resp):
        result = _invoke()

    assert "dapiSE..." in result.output
    assert "SECRETLONGTOKEN" not in result.output


# ---------------------------------------------------------------------------
# 5. Missing dependency
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_doctor_missing_dependency(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    for var in ("DATABRICKS_HOST", "DATABRICKS_TOKEN"):
        monkeypatch.delenv(var, raising=False)

    from importlib.metadata import PackageNotFoundError

    original_version = __import__("importlib.metadata", fromlist=["version"]).version

    def patched_version(name: str) -> str:
        if name == "sqlglot":
            raise PackageNotFoundError(name)
        return original_version(name)

    with patch("importlib.metadata.version", side_effect=patched_version):
        result = _invoke()

    assert result.exit_code == 0
    assert "MISSING" in result.output


# ---------------------------------------------------------------------------
# 6. Config detection
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_doctor_with_burnt_toml(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    for var in ("DATABRICKS_HOST", "DATABRICKS_TOKEN"):
        monkeypatch.delenv(var, raising=False)

    (tmp_path / ".burnt.toml").write_text(
        '[lint]\nselect = ["ALL"]\nignore = ["python_udf"]\nfail-on = "warning"\n'
        "[cache]\nttl-seconds = 300\n"
    )

    result = _invoke()

    assert result.exit_code == 0
    assert ".burnt.toml" in result.output
    assert "warning" in result.output
    assert "300s" in result.output


@pytest.mark.unit
def test_doctor_no_config(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    for var in ("DATABRICKS_HOST", "DATABRICKS_TOKEN"):
        monkeypatch.delenv(var, raising=False)

    result = _invoke()

    assert result.exit_code == 0
    assert "NOT FOUND" in result.output
    assert "burnt init" in result.output


# ---------------------------------------------------------------------------
# 7. "Also found" secondary config
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_doctor_also_found_pyproject(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    for var in ("DATABRICKS_HOST", "DATABRICKS_TOKEN"):
        monkeypatch.delenv(var, raising=False)

    (tmp_path / ".burnt.toml").write_text("[lint]\nselect = [\"ALL\"]\n")
    (tmp_path / "pyproject.toml").write_text(
        "[tool.burnt]\nworkspace_url = \"https://test\"\n"
    )

    result = _invoke()

    assert result.exit_code == 0
    assert "Also found" in result.output
    assert "pyproject.toml" in result.output
    assert "lower priority" in result.output


@pytest.mark.unit
def test_doctor_also_found_burnt_toml(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    for var in ("DATABRICKS_HOST", "DATABRICKS_TOKEN"):
        monkeypatch.delenv(var, raising=False)

    # pyproject.toml is primary, .burnt.toml is secondary
    (tmp_path / "pyproject.toml").write_text(
        "[tool.burnt]\nworkspace_url = \"https://test\"\n"
    )
    (tmp_path / ".burnt.toml").write_text("[lint]\nselect = [\"ALL\"]\n")

    result = _invoke()

    assert result.exit_code == 0
    assert "Also found" in result.output
    assert ".burnt.toml" in result.output
    assert "lower priority" in result.output


# ---------------------------------------------------------------------------
# 8. Cache status
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_doctor_cache_present(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    for var in ("DATABRICKS_HOST", "DATABRICKS_TOKEN"):
        monkeypatch.delenv(var, raising=False)

    cache_dir = tmp_path / ".burnt" / "cache"
    cache_dir.mkdir(parents=True)
    (cache_dir / "a.json").write_bytes(b"x" * 1024)
    (cache_dir / "b.json").write_bytes(b"y" * 512)

    result = _invoke()

    assert result.exit_code == 0
    assert "2 files" in result.output


@pytest.mark.unit
def test_doctor_cache_absent(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    for var in ("DATABRICKS_HOST", "DATABRICKS_TOKEN"):
        monkeypatch.delenv(var, raising=False)

    result = _invoke()

    assert result.exit_code == 0
    assert "not found" in result.output


# ---------------------------------------------------------------------------
# 9. System table checks — no warehouse auto-detected
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_doctor_system_tables_skip_no_warehouse(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DATABRICKS_HOST", "https://adb-test.azuredatabricks.net")
    monkeypatch.setenv("DATABRICKS_TOKEN", "dapiABCDEF1234")

    ok_resp = MagicMock()
    ok_resp.status_code = 200
    ok_resp.json.return_value = {"warehouses": []}  # no warehouses

    with patch("requests.get", return_value=ok_resp):
        result = _invoke()

    assert result.exit_code == 0
    assert "SKIP" in result.output
    assert "--warehouse-id" in result.output


# ---------------------------------------------------------------------------
# 10. System table checks — warehouse provided, table check results
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_doctor_system_tables_ok(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DATABRICKS_HOST", "https://adb-test.azuredatabricks.net")
    monkeypatch.setenv("DATABRICKS_TOKEN", "dapiABCDEF1234")

    ok_conn = MagicMock()
    ok_conn.status_code = 200
    ok_conn.json.return_value = {}

    ok_sql = MagicMock()
    ok_sql.status_code = 200
    ok_sql.json.return_value = {"status": {"state": "SUCCEEDED"}}

    with patch("requests.get", return_value=ok_conn), patch(
        "requests.post", return_value=ok_sql
    ):
        result = _invoke("--warehouse-id", "abc123")

    assert result.exit_code == 0
    assert "system.billing.usage" in result.output


@pytest.mark.unit
def test_doctor_system_table_no_access(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DATABRICKS_HOST", "https://adb-test.azuredatabricks.net")
    monkeypatch.setenv("DATABRICKS_TOKEN", "dapiABCDEF1234")

    ok_conn = MagicMock()
    ok_conn.status_code = 200
    ok_conn.json.return_value = {}

    denied_sql = MagicMock()
    denied_sql.status_code = 200
    denied_sql.json.return_value = {
        "status": {
            "state": "FAILED",
            "error": {"message": "PERMISSION_DENIED: User does not have privilege SELECT"},
        }
    }

    with patch("requests.get", return_value=ok_conn), patch(
        "requests.post", return_value=denied_sql
    ):
        result = _invoke("--warehouse-id", "abc123")

    assert result.exit_code == 0
    assert "NO ACCESS" in result.output
    assert "Missing permissions affect" in result.output


# ---------------------------------------------------------------------------
# 11. Python version always shown
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_doctor_shows_python_version(tmp_path, monkeypatch):
    import sys

    monkeypatch.chdir(tmp_path)
    for var in ("DATABRICKS_HOST", "DATABRICKS_TOKEN"):
        monkeypatch.delenv(var, raising=False)

    result = _invoke()

    assert result.exit_code == 0
    expected = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    assert expected in result.output
