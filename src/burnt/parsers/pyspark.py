"""PySpark code analysis using AST."""

import ast as ast_module
from enum import Enum

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

# Built-in function mappings for UDF replacement detection
BUILTIN_REPLACEMENTS = {
    # String operations
    "upper": ("upper()", "Use F.upper() instead of UDF"),
    "lower": ("lower()", "Use F.lower() instead of UDF"),
    "strip": ("trim()", "Use F.trim() instead of UDF"),
    "lstrip": ("ltrim()", "Use F.ltrim() instead of UDF"),
    "rstrip": ("rtrim()", "Use F.rtrim() instead of UDF"),
    "replace": ("regexp_replace()", "Use F.regexp_replace() instead of UDF"),
    "split": ("split()", "Use F.split() instead of UDF"),
    "startswith": ("startswith()", "Use F.startswith() instead of UDF"),
    "endswith": ("endswith()", "Use F.endswith() instead of UDF"),
    "contains": ("contains()", "Use F.contains() instead of UDF"),
    "len": (
        "length()",
        "Use F.length() for strings or F.size() for arrays instead of UDF",
    ),
    "substr": ("substring()", "Use F.substring() instead of UDF"),
    "slice": ("substring()", "Use F.substring() instead of UDF"),
    "concat": ("concat()", "Use F.concat() or F.concat_ws() instead of UDF"),
    # Math operations
    "abs": ("abs()", "Use F.abs() instead of UDF"),
    "round": ("round()", "Use F.round() instead of UDF"),
    "floor": ("floor()", "Use F.floor() instead of UDF"),
    "ceil": ("ceil()", "Use F.ceil() instead of UDF"),
    "sqrt": ("sqrt()", "Use F.sqrt() instead of UDF"),
    "pow": ("pow()", "Use F.pow() instead of UDF"),
    "exp": ("exp()", "Use F.exp() instead of UDF"),
    "log": ("log()", "Use F.log() instead of UDF"),
    "log10": ("log10()", "Use F.log10() instead of UDF"),
    "sin": ("sin()", "Use F.sin() instead of UDF"),
    "cos": ("cos()", "Use F.cos() instead of UDF"),
    "tan": ("tan()", "Use F.tan() instead of UDF"),
    # Date operations
    "year": ("year()", "Use F.year() instead of UDF"),
    "month": ("month()", "Use F.month() instead of UDF"),
    "day": ("dayofmonth()", "Use F.dayofmonth() instead of UDF"),
    "hour": ("hour()", "Use F.hour() instead of UDF"),
    "minute": ("minute()", "Use F.minute() instead of UDF"),
    "second": ("second()", "Use F.second() instead of UDF"),
    "strftime": ("date_format()", "Use F.date_format() instead of UDF"),
    "strptime": ("to_timestamp()", "Use F.to_timestamp() instead of UDF"),
}

SDP_DECORATORS = {"dp.table", "dp.materialized_view", "dp.temporary_view"}
SDP_PROHIBITED_OPS = {
    "collect",
    "count",
    "toPandas",
    "save",
    "saveAsTable",
    "start",
    "toTable",
}
JDBC_METHODS = {"format", "jdbc"}
JDBC_REQUIRED_OPTIONS = {"partitionColumn", "numPartitions", "lowerBound", "upperBound"}
WINDOW_METHODS = {"orderBy", "orderby", "rowsBetween", "rangeBetween"}


class Context(Enum):
    LOOP = "loop"
    SDP_FUNCTION = "sdp_function"
    WINDOW_EXPR = "window_expr"


def analyze_pyspark(source: str) -> tuple[list[OperationInfo], list[dict]]:
    """Analyze PySpark code for cost-affecting operations and anti-patterns."""
    try:
        tree = ast_module.parse(source)
    except SyntaxError as e:
        raise ParseError(f"Failed to parse PySpark: {e}") from e

    visitor = PySparkVisitor()
    visitor.visit(tree)
    return visitor.operations, visitor.antipatterns


class PySparkVisitor(ast_module.NodeVisitor):
    """AST visitor for PySpark code analysis with anti-pattern detection."""

    def __init__(self):
        """Initialize visitor."""
        self.operations: list[OperationInfo] = []
        self.antipatterns: list[dict] = []
        self._context_stack: list[Context] = []
        self._method_chains: dict[int, list[str]] = {}
        self._in_udf = False
        self._udf_type = None
        self._in_jdbc_read = False
        self._jdbc_options: set[str] = set()
        self._window_partition_present = False
        self._current_line: int = 0

    def visit_For(self, node: ast_module.For):
        """Visit for loops."""
        self._context_stack.append(Context.LOOP)
        self.generic_visit(node)
        self._context_stack.pop()

    def visit_While(self, node: ast_module.While):
        """Visit while loops."""
        self._context_stack.append(Context.LOOP)
        self.generic_visit(node)
        self._context_stack.pop()

    def visit_FunctionDef(self, node: ast_module.FunctionDef):
        """Visit function definitions."""
        self._current_line = node.lineno
        has_sdp_decorator = False
        is_udf = False
        udf_decorator_name = None

        for decorator in node.decorator_list:
            dec_name = self._get_decorator_name(decorator)
            if dec_name in SDP_DECORATORS:
                has_sdp_decorator = True
            if dec_name in DECORATOR_WEIGHTS:
                self.operations.append(
                    OperationInfo(
                        name=f"@{dec_name}",
                        kind="",
                        weight=DECORATOR_WEIGHTS[dec_name],
                    )
                )

            # Detect python_udf anti-pattern
            if dec_name == "udf":
                is_udf = True
                udf_decorator_name = "udf"
                self.antipatterns.append(
                    {
                        "name": "python_udf",
                        "severity": "ERROR",
                        "line": node.lineno,
                        "description": "Python UDF has 10-100x overhead vs Pandas UDF",
                        "suggestion": "Use @pandas_udf for vectorized operations",
                    }
                )

            # Detect pandas_udf anti-pattern
            if dec_name == "pandas_udf":
                is_udf = True
                udf_decorator_name = "pandas_udf"

        # Check UDF body for replaceable operations; for pandas_udf emit generic
        # warning only when no specific builtin replacement was found
        if is_udf and node.body:
            if udf_decorator_name == "pandas_udf":
                before = len(self.antipatterns)
                self._check_udf_for_builtin_replacement(node, "pandas_udf")
                if len(self.antipatterns) == before:
                    self.antipatterns.append(
                        {
                            "name": "pandas_udf",
                            "severity": "WARNING",
                            "line": node.lineno,
                            "description": (
                                "Pandas UDF has Arrow serialization overhead; "
                                "prefer native Spark functions"
                            ),
                            "suggestion": (
                                "Check if F.transform(), F.aggregate(), or column "
                                "expressions can replace this UDF"
                            ),
                        }
                    )
            else:
                self._check_udf_for_builtin_replacement(node, udf_decorator_name)

        if has_sdp_decorator:
            self._context_stack.append(Context.SDP_FUNCTION)

        self.generic_visit(node)

        if has_sdp_decorator:
            self._context_stack.pop()

    @staticmethod
    def _walk_no_nested_funcs(nodes):
        """Walk AST nodes without descending into nested function definitions."""
        for node in nodes:
            yield node
            if not isinstance(
                node, (ast_module.FunctionDef, ast_module.AsyncFunctionDef)
            ):
                yield from PySparkVisitor._walk_no_nested_funcs(
                    ast_module.iter_child_nodes(node)
                )

    def _check_udf_for_builtin_replacement(
        self, node: ast_module.FunctionDef, udf_type: str
    ):
        """Check if UDF body uses operations that have built-in Spark equivalents."""
        seen: set[str] = set()
        for stmt in self._walk_no_nested_funcs(node.body):
            if isinstance(stmt, ast_module.Call):
                func_name = None
                if isinstance(stmt.func, ast_module.Name):
                    func_name = stmt.func.id
                elif isinstance(stmt.func, ast_module.Attribute):
                    func_name = stmt.func.attr

                if func_name and func_name in BUILTIN_REPLACEMENTS and func_name not in seen:
                    seen.add(func_name)
                    builtin_func, suggestion = BUILTIN_REPLACEMENTS[func_name]
                    pattern_name = (
                        "pandas_udf_builtin_replacement"
                        if udf_type == "pandas_udf"
                        else "python_udf_builtin_replacement"
                    )
                    severity = "WARNING" if udf_type == "pandas_udf" else "ERROR"

                    self.antipatterns.append(
                        {
                            "name": pattern_name,
                            "severity": severity,
                            "line": getattr(stmt, "lineno", node.lineno),
                            "description": f"UDF uses '{func_name}()' which has built-in Spark equivalent",
                            "suggestion": f"{suggestion} (function: {builtin_func})",
                        }
                    )

    def visit_Call(self, node: ast_module.Call):
        """Visit function calls."""
        self._current_line = getattr(node, "lineno", self._current_line)

        if isinstance(node.func, ast_module.Attribute):
            method_name = node.func.attr

            self._track_method_call(method_name, node)
            self._detect_cost_operations(method_name, node)
            self._detect_antipatterns(method_name, node)
            self._detect_jdbc_operations(method_name, node)
            self._detect_window_operations(method_name, node)

        elif isinstance(node.func, ast_module.Name):
            # Track regular function calls too
            method_name = node.func.id
            self._track_method_call(method_name, node)

        self.generic_visit(node)

    def _track_method_call(self, method_name: str, node: ast_module.Call):
        """Track method chains for count_without_filter detection."""
        line_num = self._current_line
        if line_num not in self._method_chains:
            self._method_chains[line_num] = []
        self._method_chains[line_num].append(method_name)

    def _get_preceding_chain_methods(self, node: ast_module.Call) -> list[str]:
        """Return method names that precede this call in the receiver chain.

        For ``df.filter(...).limit(10).collect()``, when called on the
        ``collect`` node this returns ``["limit", "filter"]``.  This walks the
        static AST structure of the call (not visit order), so it is reliable.
        """
        methods: list[str] = []
        obj = node.func.value if isinstance(node.func, ast_module.Attribute) else None
        while (
            isinstance(obj, ast_module.Call)
            and isinstance(obj.func, ast_module.Attribute)
        ):
            methods.append(obj.func.attr)
            obj = obj.func.value
        return methods

    def _detect_cost_operations(self, method_name: str, node: ast_module.Call):
        """Detect cost-affecting operations."""
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

    def _detect_antipatterns(self, method_name: str, node: ast_module.Call):
        """Detect various anti-patterns."""
        line_num = self._current_line

        # collect_without_limit — only flag when no preceding limit() or take()
        if method_name == "collect":
            preceding = self._get_preceding_chain_methods(node)
            if "limit" not in preceding and "take" not in preceding:
                self.antipatterns.append(
                    {
                        "name": "collect_without_limit",
                        "severity": "ERROR",
                        "line": line_num,
                        "description": "collect() without limit() can OOM the driver",
                        "suggestion": "Add .limit(n).collect() or use .take()",
                    }
                )

        # toPandas
        if method_name == "toPandas":
            self.antipatterns.append(
                {
                    "name": "toPandas",
                    "severity": "ERROR",
                    "line": line_num,
                    "description": "toPandas() brings all data to driver",
                    "suggestion": "Use Koalas/Pandas API on Spark or filter first",
                }
            )

        # repartition_one
        if (
            method_name == "repartition"
            and node.args
            and isinstance(node.args[0], ast_module.Constant)
            and node.args[0].value == 1
        ):
            self.antipatterns.append(
                {
                    "name": "repartition_one",
                    "severity": "WARNING",
                    "line": line_num,
                    "description": "repartition(1) causes single partition bottleneck",
                    "suggestion": "Use larger partition count or remove",
                }
            )

        # withColumn_in_loop
        if (
            method_name in {"withColumn", "withColumnRenamed"}
            and Context.LOOP in self._context_stack
        ):
            self.antipatterns.append(
                {
                    "name": "withColumn_in_loop",
                    "severity": "WARNING",
                    "line": line_num,
                    "description": f".{method_name}() inside a loop causes plan bloat",
                    "suggestion": "Combine transformations before the loop or use foldLeft",
                }
            )

        # sdp_prohibited_ops
        if (
            method_name in SDP_PROHIBITED_OPS
            and Context.SDP_FUNCTION in self._context_stack
        ):
            self.antipatterns.append(
                {
                    "name": "sdp_prohibited_ops",
                    "severity": "ERROR",
                    "line": line_num,
                    "description": f".{method_name}() is prohibited in SDP functions",
                    "suggestion": "Remove this operation from SDP pipeline code",
                }
            )

        # count_without_filter — flag bare .count() with no filter/where/groupBy
        if method_name == "count":
            preceding = self._get_preceding_chain_methods(node)
            safe_preceding = {"filter", "where", "groupBy", "groupby", "having"}
            if not any(m in safe_preceding for m in preceding):
                self.antipatterns.append(
                    {
                        "name": "count_without_filter",
                        "severity": "WARNING",
                        "line": line_num,
                        "description": "count() on unfiltered DataFrame scans all records",
                        "suggestion": (
                            "Add .filter()/.where() before .count() to reduce scanned rows, "
                            "or use approx_count_distinct() for estimates"
                        ),
                    }
                )

    def _detect_jdbc_operations(self, method_name: str, node: ast_module.Call):
        """Detect JDBC operations and track options."""
        if (
            method_name == "format"
            and node.args
            and isinstance(node.args[0], ast_module.Constant)
        ):
            if node.args[0].value == "jdbc":
                self._in_jdbc_read = True

        elif method_name == "jdbc":
            self._in_jdbc_read = True

        elif method_name == "option" and self._in_jdbc_read and node.args:
            if isinstance(node.args[0], ast_module.Constant) and isinstance(
                node.args[0].value, str
            ):
                option_name = node.args[0].value
                if option_name in JDBC_REQUIRED_OPTIONS:
                    self._jdbc_options.add(option_name)

        elif method_name in {"load", "save"} and self._in_jdbc_read:
            missing_options = JDBC_REQUIRED_OPTIONS - self._jdbc_options
            if missing_options:
                self.antipatterns.append(
                    {
                        "name": "jdbc_incomplete_partition",
                        "severity": "ERROR",
                        "line": self._current_line,
                        "description": f"JDBC read missing required options: {', '.join(sorted(missing_options))}",
                        "suggestion": "Add partitionColumn, numPartitions, lowerBound, and upperBound options",
                    }
                )
            self._in_jdbc_read = False
            self._jdbc_options.clear()

    def _detect_window_operations(self, method_name: str, node: ast_module.Call):
        """Detect window function anti-patterns."""
        if method_name == "partitionBy":
            self._window_partition_present = True
            if Context.WINDOW_EXPR not in self._context_stack:
                self._context_stack.append(Context.WINDOW_EXPR)

        elif method_name in WINDOW_METHODS:
            if Context.WINDOW_EXPR in self._context_stack:
                self._context_stack.remove(Context.WINDOW_EXPR)

            # window_without_partition_by
            if (
                method_name in {"orderBy", "orderby"}
                and not self._window_partition_present
            ):
                self.antipatterns.append(
                    {
                        "name": "window_without_partition_by",
                        "severity": "ERROR",
                        "line": self._current_line,
                        "description": "Window .orderBy() without .partitionBy() causes global sort",
                        "suggestion": "Add .partitionBy() before .orderBy() or use .orderBy().limit()",
                    }
                )
                self._window_partition_present = False

    def _get_decorator_name(self, decorator) -> str:
        """Extract decorator name from AST node."""
        if isinstance(decorator, ast_module.Name):
            return decorator.id
        elif isinstance(decorator, ast_module.Call):
            return self._get_decorator_name(decorator.func)
        elif isinstance(decorator, ast_module.Attribute):
            if isinstance(decorator.value, ast_module.Name):
                return f"{decorator.value.id}.{decorator.attr}"
            elif isinstance(decorator.value, ast_module.Attribute):
                base_name = self._get_decorator_name(decorator.value)
                return f"{base_name}.{decorator.attr}"
        return ""
