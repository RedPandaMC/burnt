"""Anti-pattern detection for SQL and PySpark."""

from dataclasses import dataclass
from enum import StrEnum

from ..core.exceptions import ParseError
from .pyspark import analyze_pyspark
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

        # select_star (renamed, severity changed to ERROR)
        has_select_star = any(
            isinstance(node, exp.Star) for node in ast.find_all(exp.Star)
        )
        has_limit = any(isinstance(node, exp.Limit) for node in ast.find_all(exp.Limit))
        if has_select_star and not has_limit:
            patterns.append(
                AntiPattern(
                    name="select_star",
                    severity=Severity.ERROR,
                    description="SELECT * without LIMIT returns all rows and prevents column pruning",
                    suggestion="Add LIMIT clause or select specific columns",
                )
            )

        # order_by_no_limit
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

        # DROP vs CREATE OR REPLACE
        has_drop = any(isinstance(node, exp.Drop) for node in ast.find_all(exp.Drop))
        if has_drop:
            patterns.append(
                AntiPattern(
                    name="drop_table_deprecated",
                    severity=Severity.WARNING,
                    description="DROP TABLE followed by CREATE can cause data loss",
                    suggestion="Use CREATE OR REPLACE TABLE or TRUNCATE TABLE",
                )
            )

        # SDP PIVOT prohibition
        has_pivot = any(isinstance(node, exp.Pivot) for node in ast.find_all(exp.Pivot))
        if has_pivot:
            patterns.append(
                AntiPattern(
                    name="sdp_pivot_prohibited",
                    severity=Severity.ERROR,
                    description="PIVOT clause is not supported in Spark Declarative Pipelines",
                    suggestion="Use alternative transformation pattern",
                )
            )

    except ParseError:
        pass

    return patterns


def _detect_pyspark_antipatterns(source: str) -> list[AntiPattern]:
    """Detect PySpark anti-patterns using AST visitor."""
    patterns = []

    try:
        _, antipattern_dicts = analyze_pyspark(source)

        # Map dictionary anti-patterns to AntiPattern objects
        for ap_dict in antipattern_dicts:
            patterns.append(
                AntiPattern(
                    name=ap_dict["name"],
                    severity=Severity(ap_dict["severity"].lower()),
                    description=ap_dict["description"],
                    suggestion=ap_dict["suggestion"],
                    line_number=ap_dict.get("line"),
                )
            )

    except ParseError:
        # If AST parsing fails, fall back to string matching for basic patterns
        return _detect_pyspark_antipatterns_string(source)

    # Add missing patterns that require additional logic
    patterns.extend(_detect_additional_pyspark_antipatterns(source))

    return patterns


def _detect_pyspark_antipatterns_string(source: str) -> list[AntiPattern]:
    """Fallback string-based detection for when AST parsing fails."""
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
                severity=Severity.ERROR,  # Changed from WARNING to ERROR
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
                severity=Severity.ERROR,  # Changed from WARNING to ERROR
                description="toPandas() brings all data to driver",
                suggestion="Use Koalas/Pandas API on Spark or filter first",
            )
        )

    # Note: pandas_udf detection removed - too broad without checking function body

    return patterns


def _detect_additional_pyspark_antipatterns(source: str) -> list[AntiPattern]:
    """Detect additional PySpark anti-patterns not covered by AST visitor."""
    patterns = []

    # Detect DROP vs TRUNCATE/CREATE OR REPLACE
    if "DROP TABLE" in source.upper():
        patterns.append(
            AntiPattern(
                name="drop_table_deprecated",
                severity=Severity.WARNING,
                description="DROP TABLE followed by CREATE can cause data loss",
                suggestion="Use CREATE OR REPLACE TABLE or TRUNCATE TABLE",
                line_number=None,
            )
        )

    # Detect PIVOT in SQL context (SDP prohibits PIVOT)
    if "PIVOT" in source.upper():
        patterns.append(
            AntiPattern(
                name="sdp_pivot_prohibited",
                severity=Severity.ERROR,
                description="PIVOT clause is not supported in Spark Declarative Pipelines",
                suggestion="Use alternative transformation pattern",
                line_number=None,
            )
        )

    # Detect side effects in SDP functions
    if any(side_effect in source for side_effect in ["print(", "global ", "nonlocal "]):
        patterns.append(
            AntiPattern(
                name="sdp_side_effects",
                severity=Severity.WARNING,
                description="Side effects in SDP functions can cause non-deterministic behavior",
                suggestion="Remove print statements and avoid global variables",
                line_number=None,
            )
        )

    return patterns
