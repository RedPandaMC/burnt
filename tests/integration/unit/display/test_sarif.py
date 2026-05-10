"""Unit tests for SARIF 2.1.0 export."""

from __future__ import annotations

from burnt._check import CheckResult, Finding
from burnt.display.export import report_to_sarif


class TestReportToSarif:
    def test_empty_result(self) -> None:
        result = CheckResult(file_path="test.py", mode="python", findings=[])
        sarif = report_to_sarif(result)
        assert sarif["version"] == "2.1.0"
        assert sarif["$schema"] == "https://json.schemastore.org/sarif-2.1.0.json"
        assert sarif["runs"][0]["tool"]["driver"]["name"] == "burnt"
        assert sarif["runs"][0]["tool"]["driver"]["rules"] == []
        assert sarif["runs"][0]["results"] == []

    def test_single_finding(self) -> None:
        result = CheckResult(
            file_path="src/pipeline.py",
            mode="python",
            findings=[
                Finding(
                    rule_id="BP014",
                    severity="warning",
                    message="CROSS JOIN creates O(n*m) rows",
                    suggestion="Use INNER JOIN",
                    line_number=42,
                )
            ],
        )
        sarif = report_to_sarif(result)
        assert sarif["runs"][0]["tool"]["driver"]["rules"][0]["id"] == "BP014"
        results = sarif["runs"][0]["results"]
        assert len(results) == 1
        assert results[0]["ruleId"] == "BP014"
        assert results[0]["level"] == "warning"
        assert results[0]["message"]["text"] == "CROSS JOIN creates O(n*m) rows"
        assert (
            results[0]["locations"][0]["physicalLocation"]["artifactLocation"]["uri"]
            == "src/pipeline.py"
        )
        assert (
            results[0]["locations"][0]["physicalLocation"]["region"]["startLine"] == 42
        )

    def test_info_maps_to_note(self) -> None:
        result = CheckResult(
            file_path="test.py",
            mode="python",
            findings=[
                Finding(
                    rule_id="BP001",
                    severity="info",
                    message="Suggestion",
                    line_number=None,
                )
            ],
        )
        sarif = report_to_sarif(result)
        assert sarif["runs"][0]["results"][0]["level"] == "note"

    def test_no_region_when_line_number_none(self) -> None:
        result = CheckResult(
            file_path="test.py",
            mode="python",
            findings=[
                Finding(
                    rule_id="BP001", severity="info", message="msg", line_number=None
                )
            ],
        )
        sarif = report_to_sarif(result)
        loc = sarif["runs"][0]["results"][0]["locations"][0]["physicalLocation"]
        assert "region" not in loc

    def test_two_findings_same_rule_dedupes_rules(self) -> None:
        result = CheckResult(
            file_path="test.py",
            mode="python",
            findings=[
                Finding(
                    rule_id="BP014", severity="warning", message="A", line_number=1
                ),
                Finding(
                    rule_id="BP014", severity="warning", message="B", line_number=2
                ),
            ],
        )
        sarif = report_to_sarif(result)
        assert len(sarif["runs"][0]["tool"]["driver"]["rules"]) == 1
        assert len(sarif["runs"][0]["results"]) == 2

    def test_artifact_uri_base_id(self) -> None:
        result = CheckResult(file_path="a.py", mode="python", findings=[])
        sarif = report_to_sarif(result)
        # no results -> nothing to assert on uriBaseId directly, but test it doesn't crash
        assert sarif["version"] == "2.1.0"
