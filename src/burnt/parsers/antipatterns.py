"""Anti-pattern detection via Rust engine."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class Severity(StrEnum):
    """Severity levels for anti-patterns."""

    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


@dataclass
class AntiPattern:
    """Represents an anti-pattern detected in code."""

    name: str
    severity: Severity
    description: str
    suggestion: str
    line_number: int | None = None


def detect_antipatterns(source: str, language: str = "sql") -> list[AntiPattern]:
    """Detect anti-patterns using Rust engine.

    Args:
        source: Source code to analyze
        language: Language hint ("sql", "python", "pyspark", "auto")

    Returns:
        List of detected anti-patterns

    Raises:
        ImportError: If burnt._engine is not available
    """
    from burnt._engine import run_rules

    lang_map = {"pyspark": "python", "python": "python", "sql": "sql"}
    rust_lang = lang_map.get(language, language if language != "auto" else None)

    findings = run_rules(source, rust_lang)

    return [
        AntiPattern(
            name=f.code,
            severity=Severity(str(f.severity)),
            description=f.message,
            suggestion=f.suggestion or "",
            line_number=f.line_number,
        )
        for f in findings
    ]
