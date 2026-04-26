"""HTML rendering for notebooks."""

from __future__ import annotations

from typing import Any


def to_html(result: Any) -> str:
    """Render result as HTML for notebooks.

    Args:
        result: CheckResult to render.

    Returns:
        HTML string.
    """
    findings = getattr(result, "findings", [])
    file_path = getattr(result, "file_path", None) or "unknown"
    mode = getattr(result, "mode", "python")
    compute = getattr(result, "compute_seconds", None)

    rows = []
    for f in findings:
        color = {"error": "#ffcccc", "warning": "#fff3cd", "info": "#d1ecf1"}.get(
            f.severity, "#f0f0f0"
        )
        suggestion = f"<br/><em>→ {f.suggestion}</em>" if f.suggestion else ""
        rows.append(
            f"""
            <tr style="background-color: {color};">
                <td style="padding: 8px; border: 1px solid #ccc;">{f.severity.upper()}</td>
                <td style="padding: 8px; border: 1px solid #ccc;">{f.rule_id}</td>
                <td style="padding: 8px; border: 1px solid #ccc;">{f.line_number or "—"}</td>
                <td style="padding: 8px; border: 1px solid #ccc;">{f.message}{suggestion}</td>
            </tr>
            """
        )

    compute_html = f"<p><strong>Compute:</strong> {compute:.1f}s</p>" if compute else ""

    return f"""
    <div style="font-family: sans-serif; margin: 20px 0; padding: 15px; border: 1px solid #ddd; border-radius: 8px;">
        <h3>🔥 burnt check: {file_path}</h3>
        <p><strong>Mode:</strong> {mode}</p>
        {compute_html}
        <table style="border-collapse: collapse; width: 100%;">
            <thead>
                <tr style="background-color: #f8f9fa;">
                    <th style="padding: 8px; border: 1px solid #ccc;">Severity</th>
                    <th style="padding: 8px; border: 1px solid #ccc;">Rule</th>
                    <th style="padding: 8px; border: 1px solid #ccc;">Line</th>
                    <th style="padding: 8px; border: 1px solid #ccc;">Message</th>
                </tr>
            </thead>
            <tbody>
                {"".join(rows)}
            </tbody>
        </table>
    </div>
    """
