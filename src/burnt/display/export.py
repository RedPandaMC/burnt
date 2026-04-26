"""Export results to JSON and Markdown."""

from __future__ import annotations

import json
from typing import Any


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
