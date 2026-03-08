"""Anti-pattern detection for SQL and PySpark."""

from dataclasses import dataclass
from enum import StrEnum

from ..core.exceptions import ParseError
from .sql import detect_operations


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
    """Detect SQL anti-patterns using AST traversal."""
    patterns = []

    try:
        operations = detect_operations(sql)
    except ParseError:
        return patterns

    join_ops = [op for op in operations if op.name == "Join" and op.kind == "CROSS"]
    if join_ops:
        patterns.append(
            AntiPattern(
                name="cross_join",
                severity=Severity.WARNING,
                description="CROSS JOIN creates O(n*m) rows",
                suggestion="Use INNER JOIN with explicit ON clause",
            )
        )

    try:
        from sqlglot import exp, parse_one

        ast = parse_one(sql)
        has_select_star = any(
            isinstance(node, exp.Star) for node in ast.find_all(exp.Star)
        )
        has_limit = any(isinstance(node, exp.Limit) for node in ast.find_all(exp.Limit))
        if has_select_star and not has_limit:
            patterns.append(
                AntiPattern(
                    name="select_star_no_limit",
                    severity=Severity.INFO,
                    description="SELECT * without LIMIT may return large result sets",
                    suggestion="Add LIMIT clause or select specific columns",
                )
            )

        has_order_by = any(
            isinstance(node, exp.Order) for node in ast.find_all(exp.Order)
        )
        if has_order_by and not has_limit:
            patterns.append(
                AntiPattern(
                    name="order_by_no_limit",
                    severity=Severity.WARNING,
                    description="ORDER BY without LIMIT forces global sort",
                    suggestion="Add LIMIT or remove ORDER BY if not needed",
                )
            )
    except ParseError:
        pass

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
