"""Export results to JSON, Markdown, and SARIF."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from burnt._check import CheckResult


def _sarif_level(severity: str) -> str:
    return {"error": "error", "warning": "warning", "info": "note"}.get(
        severity, "warning"
    )


def report_to_sarif(result: CheckResult) -> dict:
    """Render a CheckResult as a SARIF 2.1.0 report dict."""
    findings = getattr(result, "findings", [])
    seen_rules: set[str] = set()
    rules: list[dict] = []
    sarif_results: list[dict] = []

    for f in findings:
        if f.rule_id not in seen_rules:
            seen_rules.add(f.rule_id)
            rules.append(
                {
                    "id": f.rule_id,
                    "name": f.rule_id.lower(),
                    "shortDescription": {"text": f.message},
                }
            )

        location: dict[str, Any] = {
            "physicalLocation": {
                "artifactLocation": {
                    "uri": getattr(result, "file_path", "unknown") or "unknown",
                    "uriBaseId": "%SRCROOT%",
                }
            }
        }
        if getattr(f, "line_number", None) is not None:
            location["physicalLocation"]["region"] = {"startLine": f.line_number}

        sarif_results.append(
            {
                "ruleId": f.rule_id,
                "level": _sarif_level(f.severity),
                "message": {"text": f.message},
                "locations": [location],
            }
        )

    return {
        "$schema": "https://json.schemastore.org/sarif-2.1.0.json",
        "version": "2.1.0",
        "runs": [
            {
                "tool": {
                    "driver": {
                        "name": "burnt",
                        "version": getattr(result, "__version__", "0.2.0"),
                        "informationUri": "https://github.com/relativelyunknown/burnt",
                        "rules": rules,
                    }
                },
                "results": sarif_results,
            }
        ],
    }


def to_json(result: Any) -> str:
    """Render result as JSON string.

    Args:
        result: CheckResult to render.

    Returns:
        JSON string.
    """
    data = {
        "file_path": getattr(result, "file_path", None),
        "mode": getattr(result, "mode", "python"),
        "compute_seconds": getattr(result, "compute_seconds", None),
        "findings": [
            {
                "rule_id": f.rule_id,
                "severity": f.severity,
                "message": f.message,
                "suggestion": f.suggestion,
                "line_number": f.line_number,
            }
            for f in getattr(result, "findings", [])
        ],
    }
    return json.dumps(data, indent=2)


def to_markdown(result: Any) -> str:
    """Render result as Markdown string.

    Args:
        result: CheckResult to render.

    Returns:
        Markdown string.
    """
    return result.to_markdown() if hasattr(result, "to_markdown") else ""
