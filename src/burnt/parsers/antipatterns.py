"""Anti-pattern detection for SQL and PySpark."""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import StrEnum

from ..core.exceptions import ParseError
from .pyspark import analyze_pyspark
from .sql import detect_operations, parse_sql

# Matches: # burnt: ignore[rule_id] or # burnt: ignore[rule_a, rule_b]
_SUPPRESS_RE = re.compile(r"#\s*burnt:\s*ignore\[([^\]]+)\]")


class Severity(StrEnum):
    """Severity levels for anti-patterns."""

    ERROR = "error"
    WARNING = "warning"
    INFO = "info"
    STYLE = "style"


@dataclass
class AntiPattern:
    """Represents an anti-pattern detected in code."""

    name: str
    severity: Severity
    description: str
    suggestion: str
    line_number: int | None = None


def _parse_suppressions(source: str) -> dict[int, set[str]]:
    """Parse inline suppression comments and return {line_number: {rule_ids}}.

    Handles both 1-based line numbers from AST and the source lines.
    """
    suppressions: dict[int, set[str]] = {}
    for lineno, line in enumerate(source.splitlines(), start=1):
        m = _SUPPRESS_RE.search(line)
        if m:
            rule_ids = {r.strip() for r in m.group(1).split(",") if r.strip()}
            suppressions[lineno] = rule_ids
    return suppressions


def _apply_suppressions(
    patterns: list[AntiPattern], suppressions: dict[int, set[str]]
) -> list[AntiPattern]:
    """Remove findings suppressed by inline comments."""
    if not suppressions:
        return patterns
    result = []
    for p in patterns:
        line = p.line_number
        if line is not None and line in suppressions and p.name in suppressions[line]:
            continue
        result.append(p)
    return result


def detect_antipatterns(source: str, language: str = "sql") -> list[AntiPattern]:
    """Detect anti-patterns in source code."""
    if language == "sql":
        return _detect_sql_antipatterns(source)
    elif language == "pyspark":
        return _detect_pyspark_antipatterns(source)
    return []


def _detect_sql_antipatterns(sql: str) -> list[AntiPattern]:
    """Detect SQL anti-patterns using AST traversal (single parse)."""
    patterns = []

    # Parse once and share the tree — eliminates the double-parse bug
    try:
        from sqlglot import exp

        tree = parse_sql(sql)
    except ParseError:
        return patterns

    # CROSS JOIN — detected via detect_operations (shares the same tree)
    operations = detect_operations(sql, tree=tree)
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

    # select_star (no LIMIT)
    has_select_star = any(isinstance(node, exp.Star) for node in tree.find_all(exp.Star))
    has_limit = any(isinstance(node, exp.Limit) for node in tree.find_all(exp.Limit))
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
    has_order_by = any(isinstance(node, exp.Order) for node in tree.find_all(exp.Order))
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
    has_drop = any(isinstance(node, exp.Drop) for node in tree.find_all(exp.Drop))
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
    has_pivot = any(isinstance(node, exp.Pivot) for node in tree.find_all(exp.Pivot))
    if has_pivot:
        patterns.append(
            AntiPattern(
                name="sdp_pivot_prohibited",
                severity=Severity.ERROR,
                description="PIVOT clause is not supported in Spark Declarative Pipelines",
                suggestion="Use alternative transformation pattern",
            )
        )

    # ------------------------------------------------------------------
    # BQ001: NOT IN with subquery — silently returns empty when NULL present
    # ------------------------------------------------------------------
    for node in tree.find_all(exp.Not):
        inner = node.this
        if isinstance(inner, exp.In) and inner.args.get("query"):
            patterns.append(
                AntiPattern(
                    name="not_in_with_nulls",
                    severity=Severity.WARNING,
                    description="NOT IN (subquery) silently returns empty result when the subquery contains NULLs",
                    suggestion="Use NOT EXISTS or add WHERE col IS NOT NULL to the subquery",
                )
            )
            break

    # ------------------------------------------------------------------
    # BQ002: UNION without ALL — forces full dedup sort
    # ------------------------------------------------------------------
    for node in tree.find_all(exp.Union):
        if node.args.get("distinct", True):
            patterns.append(
                AntiPattern(
                    name="union_instead_of_union_all",
                    severity=Severity.WARNING,
                    description="UNION without ALL forces a full dedup sort across the combined result",
                    suggestion="Use UNION ALL if duplicates are acceptable",
                )
            )
            break

    # ------------------------------------------------------------------
    # BQ003: COUNT(DISTINCT col) at scale
    # In sqlglot, COUNT(DISTINCT col) is represented as Count(this=Distinct(...))
    # ------------------------------------------------------------------
    for node in tree.find_all(exp.Count):
        if isinstance(node.this, exp.Distinct):
            patterns.append(
                AntiPattern(
                    name="count_distinct_at_scale",
                    severity=Severity.INFO,
                    description="COUNT(DISTINCT col) requires full shuffle and sort — expensive at scale",
                    suggestion="Consider approx_count_distinct() for large datasets",
                )
            )
            break

    # ------------------------------------------------------------------
    # BQ006: LIKE with leading wildcard
    # ------------------------------------------------------------------
    for node in tree.find_all(exp.Like):
        expr = node.args.get("expression")
        if isinstance(expr, exp.Literal) and str(expr.this).startswith("%"):
            patterns.append(
                AntiPattern(
                    name="leading_wildcard_like",
                    severity=Severity.WARNING,
                    description="LIKE '%...' with leading wildcard prevents data skipping",
                    suggestion="Use full-text search or restructure to avoid leading wildcards",
                )
            )
            break

    # ------------------------------------------------------------------
    # BQ007: Division without zero guard
    # ------------------------------------------------------------------
    for node in tree.find_all(exp.Div):
        denom = node.right
        # Flag if denominator is not wrapped in NULLIF
        if (
            (not isinstance(denom, exp.Anonymous) or str(denom.this).upper() != "NULLIF")
            and not isinstance(denom, exp.Literal)
        ):
                patterns.append(
                    AntiPattern(
                        name="division_without_zero_guard",
                        severity=Severity.WARNING,
                        description="x / y without NULLIF guard causes runtime error on zero denominator",
                        suggestion="Use NULLIF(y, 0) in the denominator: x / NULLIF(y, 0)",
                    )
                )
                break

    return patterns


def _detect_pyspark_antipatterns(source: str) -> list[AntiPattern]:
    """Detect PySpark anti-patterns using AST visitor with inline suppression."""
    # Parse inline suppressions before running detection
    suppressions = _parse_suppressions(source)

    try:
        _, antipattern_dicts = analyze_pyspark(source)

        patterns = [
            AntiPattern(
                name=ap_dict["name"],
                severity=Severity(ap_dict["severity"].lower()),
                description=ap_dict["description"],
                suggestion=ap_dict["suggestion"],
                line_number=ap_dict.get("line"),
            )
            for ap_dict in antipattern_dicts
        ]

    except ParseError:
        # If AST parsing fails, fall back to string matching for basic patterns
        return _detect_pyspark_antipatterns_string(source)

    return _apply_suppressions(patterns, suppressions)


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
                severity=Severity.ERROR,
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
                severity=Severity.ERROR,
                description="toPandas() brings all data to driver",
                suggestion="Use Koalas/Pandas API on Spark or filter first",
            )
        )

    return patterns
