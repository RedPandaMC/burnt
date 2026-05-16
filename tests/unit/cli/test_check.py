"""Unit tests for the burnt CLI check command wired to _check.run()."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from typer.testing import CliRunner

from burnt._check import CheckResult, Finding
from burnt.cli.main import app
from burnt.graph.estimate import PyEstimate

runner = CliRunner()


def _make_result(
    rule_id: str = "BP014",
    severity: str = "warning",
    line: int | None = 7,
) -> CheckResult:
    return CheckResult(
        file_path="test.py",
        mode="python",
        findings=[
            Finding(
                rule_id=rule_id,
                severity=severity,  # type: ignore[arg-type]
                message="CROSS JOIN creates O(n*m) rows",
                suggestion="Use INNER JOIN with explicit ON clause",
                line_number=line,
            )
        ],
    )


def _make_clean_result() -> CheckResult:
    return CheckResult(file_path="test.py", mode="python", findings=[])


class TestCheckFile:
    @patch("burnt.cli.main.check_run")
    def test_check_single_file_finds_issues(self, mock_run) -> None:
        mock_run.return_value = _make_result()
        with runner.isolated_filesystem() as fs:
            Path(fs, "test.py").write_text("df.collect()")
            result = runner.invoke(app, ["check", str(Path(fs, "test.py"))])
        assert result.exit_code == 1
        assert "BP014" in result.output

    @patch("burnt.cli.main.check_run")
    def test_check_single_file_clean_exits_0(self, mock_run) -> None:
        mock_run.return_value = _make_clean_result()
        with runner.isolated_filesystem() as fs:
            Path(fs, "test.py").write_text("df.collect()")
            result = runner.invoke(app, ["check", str(Path(fs, "test.py"))])
        assert result.exit_code == 0
        assert "No cost anti-patterns found" in result.output

    @patch("burnt.cli.main.check_run")
    def test_check_inline_source(self, mock_run) -> None:
        mock_run.return_value = _make_result()
        result = runner.invoke(app, ["check", "SELECT * FROM t"])
        assert result.exit_code == 1
        assert "BP014" in result.output

    @patch("burnt.cli.main.check_run")
    def test_check_inline_source_clean(self, mock_run) -> None:
        mock_run.return_value = _make_clean_result()
        result = runner.invoke(app, ["check", "SELECT 1"])
        assert result.exit_code == 0


class TestCheckOutputFormats:
    @patch("burnt.cli.main.check_run")
    def test_json_output(self, mock_run) -> None:
        mock_run.return_value = _make_result()
        with runner.isolated_filesystem() as fs:
            Path(fs, "test.py").write_text("df.collect()")
            result = runner.invoke(
                app, ["check", str(Path(fs, "test.py")), "--output", "json"]
            )
        assert result.exit_code == 1
        assert '"rule_id": "BP014"' in result.output

    @patch("burnt.cli.main.check_run")
    def test_text_output(self, mock_run) -> None:
        mock_run.return_value = _make_result()
        with runner.isolated_filesystem() as fs:
            Path(fs, "test.py").write_text("df.collect()")
            result = runner.invoke(
                app, ["check", str(Path(fs, "test.py")), "--output", "text"]
            )
        assert result.exit_code == 1
        assert "WARNING" in result.output
        assert "BP014" in result.output

    @patch("burnt.cli.main.check_run")
    def test_table_output(self, mock_run) -> None:
        mock_run.return_value = _make_result()
        with runner.isolated_filesystem() as fs:
            Path(fs, "test.py").write_text("df.collect()")
            result = runner.invoke(
                app, ["check", str(Path(fs, "test.py")), "--output", "table"]
            )
        assert result.exit_code == 1
        assert "BP014" in result.output


class TestCheckFiltering:
    @patch("burnt.cli.main.check_run")
    def test_ignore_flag_result_clean(self, mock_run) -> None:
        mock_run.return_value = _make_clean_result()
        with runner.isolated_filesystem() as fs:
            Path(fs, "test.py").write_text("df.collect()")
            result = runner.invoke(
                app, ["check", str(Path(fs, "test.py")), "--ignore", "BP014"]
            )
        assert result.exit_code == 0
        mock_run.assert_called_once()

    @patch("burnt.cli.main.check_run")
    def test_select_flag_passes_only(self, mock_run) -> None:
        mock_run.return_value = _make_result()
        with runner.isolated_filesystem() as fs:
            Path(fs, "test.py").write_text("df.collect()")
            result = runner.invoke(
                app, ["check", str(Path(fs, "test.py")), "--select", "BP014"]
            )
        assert result.exit_code == 1
        mock_run.assert_called_once()
        call_kwargs = mock_run.call_args.kwargs
        assert "BP014" in (call_kwargs.get("only") or [])

    @patch("burnt.cli.main.check_run")
    def test_fail_on_info(self, mock_run) -> None:
        mock_run.return_value = _make_result(severity="info")
        with runner.isolated_filesystem() as fs:
            Path(fs, "test.py").write_text("df.collect()")
            result = runner.invoke(
                app, ["check", str(Path(fs, "test.py")), "--fail-on", "info"]
            )
        assert result.exit_code == 1

    @patch("burnt.cli.main.check_run")
    def test_fail_on_error_skips_warning(self, mock_run) -> None:
        mock_run.return_value = _make_clean_result()
        with runner.isolated_filesystem() as fs:
            Path(fs, "test.py").write_text("df.collect()")
            result = runner.invoke(
                app, ["check", str(Path(fs, "test.py")), "--fail-on", "error"]
            )
        assert result.exit_code == 0


class TestCheckDirectory:
    @patch("burnt.cli.main.check_run")
    def test_directory_no_files(self, mock_run) -> None:
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            result = runner.invoke(app, ["check", tmpdir])
            assert result.exit_code == 0
            assert "No .sql or .py files found" in result.output
            mock_run.assert_not_called()

    @patch("burnt.cli.main.check_run")
    def test_directory_with_files(self, mock_run) -> None:
        mock_run.return_value = _make_result()
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            Path(tmpdir).joinpath("test.py").write_text("df.collect()")
            result = runner.invoke(app, ["check", tmpdir])
            assert result.exit_code == 1
            assert mock_run.called


class TestExplainCost:
    @patch("burnt.cli.main.check_run")
    def test_explain_cost_renders_breakdown_tree(self, mock_run) -> None:
        result_with_cost = _make_result()
        result_with_cost.compute_seconds = 90.0
        result_with_cost.estimate = PyEstimate(
            estimated_dbu=0.02,
            breakdown={"n1": 30.0, "n2": 60.0},
            shuffle_bytes={"n2": 1_073_741_824},
            coverage_ratio=0.5,
        )
        mock_run.return_value = result_with_cost
        with runner.isolated_filesystem() as fs:
            Path(fs, "test.py").write_text("df.collect()")
            result = runner.invoke(
                app, ["check", str(Path(fs, "test.py")), "--explain-cost"]
            )
        assert "n1" in result.output
        assert "n2" in result.output
        assert "30.00s" in result.output
        assert "60.00s" in result.output

    @patch("burnt.cli.main.check_run")
    def test_explain_cost_no_estimate_skips_silently(self, mock_run) -> None:
        # estimate left unset
        mock_run.return_value = _make_result()
        with runner.isolated_filesystem() as fs:
            Path(fs, "test.py").write_text("df.collect()")
            result = runner.invoke(
                app, ["check", str(Path(fs, "test.py")), "--explain-cost"]
            )
        assert result.exit_code == 1
        assert "Tree" not in result.output  # no tree rendered


class TestCheckConfigError:
    @patch("burnt.cli.main.Settings.discover", side_effect=RuntimeError("bad config"))
    def test_config_error_exits_2(self, _mock) -> None:
        result = runner.invoke(app, ["check", "test.py"])
        assert result.exit_code == 2
        assert "Config error" in result.output
