"""PySpark code analysis using AST."""

import ast as ast_module

from ..core.exceptions import ParseError
from ..core.models import OperationInfo

PYSPARK_WEIGHTS = {
    "groupBy": 8,
    "groupby": 8,
    "join": 10,
    "crossJoin": 50,
    "collect": 25,
    "toPandas": 25,
    "repartition": 5,
    "repartition(1)": 15,
    "write": 3,
    "writeStream": 8,
}


DECORATOR_WEIGHTS = {
    "udf": 15,
    "pandas_udf": 5,
    "pandas_udf(pandas_udf_type())": 5,
}


def analyze_pyspark(source: str) -> list[OperationInfo]:
    """Analyze PySpark code for cost-affecting operations."""
    try:
        tree = ast_module.parse(source)
    except SyntaxError as e:
        raise ParseError(f"Failed to parse PySpark: {e}") from e

    visitor = PySparkVisitor()
    visitor.visit(tree)
    return visitor.operations


class PySparkVisitor(ast_module.NodeVisitor):
    """AST visitor for PySpark code analysis."""

    def __init__(self):
        """Initialize visitor."""
        self.operations: list[OperationInfo] = []
        self._in_udf = False
        self._udf_type = None

    def visit_Call(self, node: ast_module.Call):
        """Visit function calls."""
        if isinstance(node.func, ast_module.Attribute):
            method_name = node.func.attr

            if method_name in PYSPARK_WEIGHTS:
                weight = PYSPARK_WEIGHTS[method_name]
                if (
                    method_name == "repartition"
                    and node.args
                    and isinstance(node.args[0], ast_module.Constant)
                    and node.args[0].value == 1
                ):
                    weight = 15
                self.operations.append(
                    OperationInfo(
                        name=method_name,
                        kind="",
                        weight=weight,
                    )
                )

        self.generic_visit(node)

    def visit_FunctionDef(self, node: ast_module.FunctionDef):
        """Visit function definitions."""
        for decorator in node.decorator_list:
            dec_name = self._get_decorator_name(decorator)
            if dec_name in DECORATOR_WEIGHTS:
                self.operations.append(
                    OperationInfo(
                        name=f"@{dec_name}",
                        kind="",
                        weight=DECORATOR_WEIGHTS[dec_name],
                    )
                )
        self.generic_visit(node)

    def _get_decorator_name(self, decorator) -> str:
        """Extract decorator name from AST node."""
        if isinstance(decorator, ast_module.Name):
            return decorator.id
        elif isinstance(decorator, ast_module.Call):
            return self._get_decorator_name(decorator.func)
        elif isinstance(decorator, ast_module.Attribute):
            return decorator.attr
        return ""
