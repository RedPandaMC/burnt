"""burnt.check() - Hybrid static + runtime analysis."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, Field


class Finding(BaseModel):
    """A single finding from burnt analysis."""

    rule_id: str
    severity: Literal["error", "warning", "info"]
    message: str
    suggestion: str = ""
    line_number: int | None = None
    file_path: str | None = None
    compute_seconds: float | None = None  # populated when runtime data available


class CheckResult(BaseModel):
    """Result of burnt.check() — static + runtime analysis."""

    file_path: str | None = None
    mode: str = "python"  # "python" | "sql" | "dlt"
    findings: list[Finding] = Field(default_factory=list)
    compute_seconds: float | None = None
    estimate: Any = None  # PyEstimate from providers backend
    graph: Any = None  # PyGraph or PyPipeline from Rust engine
    raw: Any = None  # Original AnalysisResultPy

    def display(self) -> None:
        """Render result as Rich table (terminal) or HTML (notebook)."""
        from burnt.display import auto_render

        auto_render(self)

    def to_json(self) -> dict:
        """Return structured dict for programmatic use."""
        return {
            "file_path": self.file_path,
            "mode": self.mode,
            "compute_seconds": self.compute_seconds,
            "findings": [f.model_dump() for f in self.findings],
        }

    def to_markdown(self) -> str:
        """Return Markdown string for PR descriptions."""
        lines = [f"# burnt check: {self.file_path or 'current notebook'}\n"]
        lines.append(f"**Mode:** {self.mode}\n")
        if self.compute_seconds is not None:
            lines.append(f"**Compute:** {self.compute_seconds:.1f}s\n")
        lines.append(f"**Findings:** {len(self.findings)}\n")
        lines.append("")
        for f in self.findings:
            ascii_map = {"error": "[X]", "warning": "[!]", "info": "[i]"}
            icon = ascii_map.get(f.severity, "•")
            lines.append(f"{icon} **{f.rule_id}** ({f.severity})")
            lines.append(f"   {f.message}")
            if f.suggestion:
                lines.append(f"   → {f.suggestion}")
            lines.append("")
        return "\n".join(lines)


def run(
    path: str | Path | None = None,
    *,
    max_cost: float | None = None,
    severity: Literal["error", "warning", "info"] = "warning",
    skip: list[str] | None = None,
    only: list[str] | None = None,
    cluster: str | None = None,
    json: bool = False,
    markdown: bool = False,
    session: Any = None,
) -> CheckResult:
    """Run hybrid analysis on a notebook, Python file, or SQL file.

    Uses the Rust engine for static analysis and optionally merges
    runtime metrics from an active session.
    """
    from burnt._engine import analyze_file, analyze_source

    target = str(path) if path else ""

    # --- Static analysis (Rust engine) ---
    if target and Path(target).exists() and Path(target).is_file():
        raw = analyze_file(target)
    else:
        # If no file, try to read current notebook or use inline source
        source = _read_source(target)
        raw = analyze_source(source, path=target or None)

    # --- Convert Rust findings to Python model ---
    findings: list[Finding] = [
        Finding(
            rule_id=getattr(f, "code", getattr(f, "rule_id", "UNKNOWN")),
            severity=str(getattr(f, "severity", "warning")).lower(),
            message=getattr(f, "message", getattr(f, "description", "")),
            suggestion=getattr(f, "suggestion", "") or "",
            line_number=getattr(f, "line_number", None),
            file_path=target or None,
        )
        for f in raw.findings
    ]

    result = CheckResult(
        file_path=target or None,
        mode=raw.mode,
        findings=findings,
        graph=raw.graph or raw.pipeline,
        raw=raw,
    )

    # --- Merge runtime metrics (if session active) ---
    if session is not None:
        _merge_runtime(result, session)

    # --- Severity filtering ---
    severity_order = {"error": 3, "warning": 2, "info": 1}
    threshold = severity_order.get(severity, 2)
    result.findings = [
        f for f in result.findings if severity_order.get(f.severity, 0) >= threshold
    ]

    # --- Skip / Only filtering ---
    if only:
        result.findings = [f for f in result.findings if f.rule_id in only]
    if skip:
        result.findings = [f for f in result.findings if f.rule_id not in skip]

    return result


def _read_source(target: str) -> str:
    """Read source from a file or return empty string."""
    if not target:
        return ""
    p = Path(target)
    if p.exists() and p.is_file():
        return p.read_text(encoding="utf-8")
    return ""


def _merge_runtime(result: CheckResult, session: Any) -> None:
    """Tag findings with actual runtime metrics from a session.

    The graph here may be either the Rust ``PyGraph`` (from
    ``analyze_file``/``analyze_source``) or the pure-Python ``PyGraph``.
    Both expose the same ``.nodes`` / ``.edges`` duck-typed surface, so
    ``enrich_graph`` and ``estimate`` consume them uniformly without
    mutating node fields.
    """
    if not hasattr(session, "stages"):
        return

    from burnt.graph.enrich import enrich_graph
    from burnt.graph.estimate import estimate

    graph = result.graph
    observed = enrich_graph(graph, session=session) if graph is not None else {}
    estimate = (
        estimate(graph, session, observed_input_bytes=observed)
        if graph is not None
        else None
    )

    if estimate is None or not estimate.breakdown:
        # No graph or no nodes — fall back to a flat sum so the
        # CheckResult surface stays populated.
        result.compute_seconds = sum(
            s.get("executorRunTime", 0) / 1000.0 for s in session.stages
        )
        return

    result.estimate = estimate
    result.compute_seconds = sum(estimate.breakdown.values())

    line_to_compute = {
        n.line_number: estimate.breakdown.get(n.id, 0.0)
        for n in (graph.nodes or [])
        if getattr(n, "line_number", None) is not None
    }
    for finding in result.findings:
        if finding.line_number is None:
            continue
        # Match within ±5 lines, same window the estimator uses.
        best = None
        for line, seconds in line_to_compute.items():
            delta = abs(line - finding.line_number)
            if delta > 5:
                continue
            if best is None or delta < best[0]:
                best = (delta, seconds)
        if best is not None:
            finding.compute_seconds = best[1]

    # Re-sort: highest compute seconds first; findings with no compute
    # info sink to the bottom while preserving their original order.
    result.findings.sort(
        key=lambda f: -(f.compute_seconds or 0.0),
    )


__all__ = ["CheckResult", "Finding", "run"]
