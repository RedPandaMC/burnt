"""Anti-pattern detection for SQL and PySpark."""

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
    """Detect anti-patterns in source code."""
    if language == "sql":
        return _detect_sql_antipatterns(source)
    elif language == "pyspark":
        return _detect_pyspark_antipatterns(source)
    return []


def _detect_sql_antipatterns(sql: str) -> list[AntiPattern]:
    """Detect SQL anti-patterns."""
    patterns = []

    if "CROSS JOIN" in sql.upper():
        patterns.append(
            AntiPattern(
                name="cross_join",
                severity=Severity.WARNING,
                description="CROSS JOIN creates O(n*m) rows",
                suggestion="Use INNER JOIN with explicit ON clause",
            )
        )

    if "SELECT *" in sql.upper() and "LIMIT" not in sql.upper():
        patterns.append(
            AntiPattern(
                name="select_star_no_limit",
                severity=Severity.INFO,
                description="SELECT * without LIMIT may return large result sets",
                suggestion="Add LIMIT clause or select specific columns",
            )
        )

    if "ORDER BY" in sql.upper() and "LIMIT" not in sql.upper():
        patterns.append(
            AntiPattern(
                name="order_by_no_limit",
                severity=Severity.WARNING,
                description="ORDER BY without LIMIT forces global sort",
                suggestion="Add LIMIT or remove ORDER BY if not needed",
            )
        )

    return patterns


def _detect_pyspark_antipatterns(source: str) -> list[AntiPattern]:
    """Detect PySpark anti-patterns."""
    patterns = []

    if ".collect()" in source and ".limit(" not in source:
        patterns.append(
            AntiPattern(
                name="collect_without_limit",
                severity=Severity.ERROR,
                description="collect() without limit() can OOM the driver",
                suggestion="Add .limit(n).collect() or use .take()",
            )
        )

    if "@udf" in source and "@pandas_udf" not in source:
        patterns.append(
            AntiPattern(
                name="python_udf",
                severity=Severity.WARNING,
                description="Python UDF has 10-100x overhead vs Pandas UDF",
                suggestion="Use @pandas_udf for vectorized operations",
            )
        )

    if ".repartition(1)" in source:
        patterns.append(
            AntiPattern(
                name="repartition_one",
                severity=Severity.WARNING,
                description="repartition(1) causes single partition bottleneck",
                suggestion="Use larger partition count or remove",
            )
        )

    if ".toPandas()" in source:
        patterns.append(
            AntiPattern(
                name="toPandas",
                severity=Severity.WARNING,
                description="toPandas() brings all data to driver",
                suggestion="Use Koalas/Pandas API on Spark or filter first",
            )
        )

    return patterns
