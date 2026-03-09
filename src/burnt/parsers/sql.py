"""SQL parsing for Databricks using sqlglot."""

from .._compat import require
from ..core.exceptions import ParseError
from ..core.models import OperationInfo, QueryProfile

OPERATION_WEIGHTS = {
    "MERGE": 20,
    "CROSS_JOIN": 50,
    "SHUFFLE_JOIN": 10,
    "GROUP_BY": 8,
    "WINDOW": 8,
    "COLLECT": 25,
    "PYTHON_UDF": 15,
    "PANDAS_UDF": 5,
    "ORDER_BY": 7,
    "DISTINCT": 6,
    "SUBQUERY": 3,
    "CTE": 2,
}


def parse_sql(sql: str, dialect: str = "databricks"):
    """Parse SQL string into AST."""
    require("sqlglot")
    from sqlglot import parse_one

    if not sql or not sql.strip():
        raise ParseError("Empty SQL string")

    try:
        return parse_one(sql, dialect=dialect)
    except Exception as e:
        raise ParseError(f"Failed to parse SQL: {e}") from e


def extract_tables(sql: str, dialect: str = "databricks") -> list[str]:
    """Extract table references from SQL."""
    require("sqlglot")
    from sqlglot import exp

    ast = parse_sql(sql, dialect)
    tables = []
    for table in ast.find_all(exp.Table):
        parts = []
        if table.catalog:
            parts.append(table.catalog)
        if table.db:
            parts.append(table.db)
        parts.append(table.name)
        tables.append(".".join(parts))
    return list(dict.fromkeys(tables))


def detect_operations(sql: str, dialect: str = "databricks") -> list[OperationInfo]:
    """Detect cost-affecting operations in SQL."""
    require("sqlglot")
    from sqlglot import exp

    ast = parse_sql(sql, dialect)
    operations = []

    for node in ast.walk():
        if isinstance(node, exp.Merge):
            operations.append(OperationInfo(name="Merge", kind="", weight=20))

        elif isinstance(node, exp.Join):
            kind = node.args.get("kind", "")
            if kind.upper() == "CROSS":
                operations.append(OperationInfo(name="Join", kind="CROSS", weight=50))
            else:
                operations.append(
                    OperationInfo(name="Join", kind=kind or "INNER", weight=10)
                )

        elif isinstance(node, exp.Group):
            operations.append(OperationInfo(name="GroupBy", kind="", weight=8))

        elif isinstance(node, exp.Window):
            operations.append(OperationInfo(name="Window", kind="", weight=8))

        elif isinstance(node, exp.Order):
            operations.append(OperationInfo(name="OrderBy", kind="", weight=7))

        elif isinstance(node, exp.Distinct):
            operations.append(OperationInfo(name="Distinct", kind="", weight=6))

        elif isinstance(node, exp.Subquery):
            operations.append(OperationInfo(name="Subquery", kind="", weight=3))

        elif isinstance(node, exp.CTE):
            operations.append(OperationInfo(name="CTE", kind="", weight=2))

    return operations


def compute_complexity(sql: str, dialect: str = "databricks") -> float:
    """Compute complexity score from operations."""
    ops = detect_operations(sql, dialect)
    return sum(op.weight for op in ops)


def analyze_query(sql: str, dialect: str = "databricks") -> QueryProfile:
    """Perform full query analysis."""
    ops = detect_operations(sql, dialect)
    tables = extract_tables(sql, dialect)
    complexity = compute_complexity(sql, dialect)

    return QueryProfile(
        sql=sql,
        dialect=dialect,
        operations=ops,
        tables=tables,
        complexity_score=complexity,
    )
