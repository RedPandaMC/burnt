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

# Names treated as the Window class (common aliases)
_WINDOW_CLASS_NAMES = {"Window", "W"}


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
        self._in_udf = False
        self._udf_type = None
        self._current_line: int = 0

    def visit_For(self, node: ast_module.For):
        """Visit for loops — also detect 'for row in df.collect()' (BP026)."""
        # BP026: iterating_over_collect
        iter_node = node.iter
        if (
            isinstance(iter_node, ast_module.Call)
            and isinstance(iter_node.func, ast_module.Attribute)
            and iter_node.func.attr == "collect"
        ):
            self.antipatterns.append(
                {
                    "name": "iterating_over_collect",
                    "severity": "ERROR",
                    "line": node.lineno,
                    "description": "for row in df.collect() brings all data to driver and iterates row-by-row",
                    "suggestion": "Use DataFrame transformations; avoid driver-side iteration",
                }
            )

        self._context_stack.append(Context.LOOP)
        self.generic_visit(node)
        self._context_stack.pop()

    def visit_While(self, node: ast_module.While):
        """Visit while loops."""
        self._context_stack.append(Context.LOOP)
        self.generic_visit(node)
        self._context_stack.pop()

    def visit_Global(self, node: ast_module.Global):
        """Detect global statements inside SDP functions (side effect)."""
        if Context.SDP_FUNCTION in self._context_stack:
            self.antipatterns.append(
                {
                    "name": "sdp_side_effects",
                    "severity": "WARNING",
                    "line": node.lineno,
                    "description": "global statement in SDP function causes non-deterministic behavior",
                    "suggestion": "Remove global variables from SDP pipeline code",
                }
            )
        self.generic_visit(node)

    def visit_Nonlocal(self, node: ast_module.Nonlocal):
        """Detect nonlocal statements inside SDP functions (side effect)."""
        if Context.SDP_FUNCTION in self._context_stack:
            self.antipatterns.append(
                {
                    "name": "sdp_side_effects",
                    "severity": "WARNING",
                    "line": node.lineno,
                    "description": "nonlocal statement in SDP function causes non-deterministic behavior",
                    "suggestion": "Remove nonlocal variables from SDP pipeline code",
                }
            )
        self.generic_visit(node)

    def visit_Import(self, node: ast_module.Import):
        """Detect non-canonical import aliases for pyspark.sql.functions / types."""
        for alias in node.names:
            if alias.name == "pyspark.sql.functions":
                asname = alias.asname or ""
                if asname and asname != "F":
                    self.antipatterns.append(
                        {
                            "name": "non_canonical_functions_alias",
                            "severity": "WARNING",
                            "line": node.lineno,
                            "description": f"pyspark.sql.functions imported as '{asname}' — use 'F'",
                            "suggestion": "Use: import pyspark.sql.functions as F",
                        }
                    )
        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast_module.ImportFrom):
        """Detect star imports and non-canonical aliases for pyspark modules."""
        module = node.module or ""

        if module == "pyspark.sql.functions":
            for alias in node.names:
                if alias.name == "*":
                    self.antipatterns.append(
                        {
                            "name": "star_import_pyspark_functions",
                            "severity": "ERROR",
                            "line": node.lineno,
                            "description": (
                                "from pyspark.sql.functions import * shadows Python "
                                "built-ins (max, min, sum, map, round)"
                            ),
                            "suggestion": "Use: from pyspark.sql import functions as F",
                        }
                    )

        if module == "pyspark.sql":
            for alias in node.names:
                if alias.name == "functions":
                    asname = alias.asname or ""
                    if asname and asname != "F":
                        self.antipatterns.append(
                            {
                                "name": "non_canonical_functions_alias",
                                "severity": "WARNING",
                                "line": node.lineno,
                                "description": f"pyspark.sql.functions imported as '{asname}' — use 'F'",
                                "suggestion": "Use: from pyspark.sql import functions as F",
                            }
                        )
                if alias.name == "types":
                    asname = alias.asname or ""
                    if asname and asname != "T":
                        self.antipatterns.append(
                            {
                                "name": "non_canonical_types_alias",
                                "severity": "STYLE",
                                "line": node.lineno,
                                "description": f"pyspark.sql.types imported as '{asname}' — use 'T'",
                                "suggestion": "Use: from pyspark.sql import types as T",
                            }
                        )

        self.generic_visit(node)

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

            self._detect_cost_operations(method_name, node)
            self._detect_antipatterns(method_name, node)
            self._detect_new_cost_rules(method_name, node)
            self._detect_style_rules(method_name, node)
            self._detect_jdbc_operations(method_name, node)
            self._detect_window_operations(method_name, node)

        elif isinstance(node.func, ast_module.Name):
            func_name = node.func.id
            # Detect print() calls in SDP context (side effect)
            if func_name == "print" and Context.SDP_FUNCTION in self._context_stack:
                self.antipatterns.append(
                    {
                        "name": "sdp_side_effects",
                        "severity": "WARNING",
                        "line": self._current_line,
                        "description": "print() in SDP function causes non-deterministic behavior",
                        "suggestion": "Remove print statements from SDP pipeline code",
                    }
                )

        self.generic_visit(node)

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

        # sdp_pivot_prohibited — .pivot() call in SDP context
        if method_name == "pivot" and Context.SDP_FUNCTION in self._context_stack:
            self.antipatterns.append(
                {
                    "name": "sdp_pivot_prohibited",
                    "severity": "ERROR",
                    "line": line_num,
                    "description": "PIVOT is not supported in Spark Declarative Pipelines",
                    "suggestion": "Use alternative transformation pattern",
                }
            )

        # expression_join_duplicate_cols (BNT-C03) — .join(other, left["key"] == right["key"])
        # creates duplicate key columns that cause AnalysisException downstream
        if method_name == "join" and node.args and len(node.args) >= 2:
            cond = node.args[1]
            if isinstance(cond, (ast_module.Compare, ast_module.BoolOp)):
                # expression-form join condition — may produce duplicate cols
                self.antipatterns.append(
                    {
                        "name": "expression_join_duplicate_cols",
                        "severity": "WARNING",
                        "line": line_num,
                        "description": (
                            ".join() with expression condition creates duplicate key columns "
                            "causing AnalysisException in downstream .select()/.drop()"
                        ),
                        "suggestion": "Use string key: .join(other, 'key', how='inner') to auto-deduplicate",
                    }
                )

        # join_missing_how_keyword (BNT-C04) — .join() without how= kwarg
        if method_name == "join":
            has_how_kwarg = any(
                kw.arg == "how" for kw in node.keywords
            )
            has_positional_how = len(node.args) >= 3
            if not has_how_kwarg and not has_positional_how and len(node.args) >= 1:
                self.antipatterns.append(
                    {
                        "name": "join_missing_how_keyword",
                        "severity": "WARNING",
                        "line": line_num,
                        "description": ".join() without how= uses implicit inner join — intent unclear",
                        "suggestion": "Add how='inner' (or the intended join type) as a keyword argument",
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

    def _detect_new_cost_rules(self, method_name: str, node: ast_module.Call):
        """Detect BP020-BP031 new cost-semantic rules."""
        line_num = self._current_line
        preceding = self._get_preceding_chain_methods(node)

        # BP021: repartition_before_write — .repartition(n) immediately before .write
        if method_name == "repartition" and "write" in self._get_following_chain_methods(node):
            self.antipatterns.append(
                {
                    "name": "repartition_before_write",
                    "severity": "WARNING",
                    "line": line_num,
                    "description": ".repartition(n) before .write causes an extra full shuffle",
                    "suggestion": "Use .coalesce(n) to reduce partitions without a full shuffle",
                }
            )

        # BP022: schema_inference_on_read — .read.csv()/.json() without .schema()
        if method_name in {"csv", "json", "text"} and "schema" not in preceding:
            # Only flag when directly reading (not when called on a schema-already-set builder)
            self.antipatterns.append(
                {
                    "name": "schema_inference_on_read",
                    "severity": "WARNING",
                    "line": line_num,
                    "description": f".read.{method_name}() without .schema() triggers a full scan to infer types",
                    "suggestion": "Provide an explicit schema with .schema(StructType(...))",
                }
            )

        # BP025: show_left_in — .show() call in any file
        if method_name == "show":
            self.antipatterns.append(
                {
                    "name": "show_left_in",
                    "severity": "WARNING",
                    "line": line_num,
                    "description": ".show() in production code triggers a full plan stage",
                    "suggestion": "Remove .show() from production code; use logging or observability tools",
                }
            )

        # BP026: iterating_over_collect — detected when visiting For loops;
        # here detect the pattern inside the AST as collect() inside iter context
        # (handled in visit_For via context; also detect for-in-collect directly)

        # BP027: join_without_how — .join(df, key) without how= (2-arg form)
        # Already handled in _detect_antipatterns above (join_missing_how_keyword)

        # BP030: spark_sql_f_string — spark.sql(f"...") or spark.sql("".format(...))
        if method_name == "sql" and node.args:
                arg = node.args[0]
                # f-string
                if isinstance(arg, ast_module.JoinedStr):
                    self.antipatterns.append(
                        {
                            "name": "spark_sql_f_string",
                            "severity": "ERROR",
                            "line": line_num,
                            "description": "spark.sql(f'...') bypasses the plan cache and risks SQL injection",
                            "suggestion": "Use parameterized SQL: spark.sql('SELECT * FROM {t}', t=df)",
                        }
                    )
                # "...".format(...) — Call on a string constant
                elif (
                    isinstance(arg, ast_module.Call)
                    and isinstance(arg.func, ast_module.Attribute)
                    and arg.func.attr == "format"
                    and isinstance(arg.func.value, ast_module.Constant)
                ):
                    self.antipatterns.append(
                        {
                            "name": "spark_sql_f_string",
                            "severity": "ERROR",
                            "line": line_num,
                            "description": "spark.sql('...'.format(...)) bypasses the plan cache and risks SQL injection",
                            "suggestion": "Use parameterized SQL: spark.sql('SELECT * FROM {t}', t=df)",
                        }
                    )

        # BNT-S01: schema_inference_enabled — .option("inferSchema", "true")
        if method_name == "option" and node.args and len(node.args) >= 2:
            k = node.args[0]
            v = node.args[1]
            if (
                isinstance(k, ast_module.Constant)
                and isinstance(k.value, str)
                and k.value.lower() == "inferschema"
                and isinstance(v, ast_module.Constant)
                and str(v.value).lower() in {"true", "1"}
            ):
                self.antipatterns.append(
                    {
                        "name": "schema_inference_enabled",
                        "severity": "WARNING",
                        "line": line_num,
                        "description": ".option('inferSchema','true') triggers a full scan — fails silently on schema drift",
                        "suggestion": "Provide an explicit schema with .schema(StructType(...))",
                    }
                )

        # BNT-N02: aggregation_without_alias — F.avg/sum/count etc. without .alias()
        if method_name in {"agg", "aggregate"}:
            for arg in node.args:
                # Each arg to agg() that is a Call without .alias() on top
                if isinstance(arg, ast_module.Call) and not self._has_alias(arg):
                    func_name = ""
                    if isinstance(arg.func, ast_module.Attribute):
                        func_name = arg.func.attr
                    elif isinstance(arg.func, ast_module.Name):
                        func_name = arg.func.id
                    if func_name in {
                        "avg", "mean", "sum", "count", "min", "max",
                        "first", "last", "stddev", "variance", "collect_list",
                        "collect_set", "approx_count_distinct",
                    }:
                        self.antipatterns.append(
                            {
                                "name": "aggregation_without_alias",
                                "severity": "WARNING",
                                "line": line_num,
                                "description": f"{func_name}() without .alias() produces an unusable auto-name",
                                "suggestion": f"Add .alias('result_name') to the {func_name}() call",
                            }
                        )

        # BNT-W01: window_missing_frame_spec — Window.orderBy() without frame spec
        if method_name in {"orderBy", "orderby"}:
            # Walk chain to see if rowsBetween or rangeBetween is present
            chain_methods = self._get_preceding_chain_methods(node)
            has_frame = "rowsBetween" in chain_methods or "rangeBetween" in chain_methods
            # Only flag for Window-based chains (base must be Window)
            obj = node.func.value if isinstance(node.func, ast_module.Attribute) else None
            base_name = self._get_chain_base_name(obj)
            if base_name in _WINDOW_CLASS_NAMES and not has_frame:
                self.antipatterns.append(
                    {
                        "name": "window_missing_frame_spec",
                        "severity": "WARNING",
                        "line": line_num,
                        "description": "Window.orderBy() without .rowsBetween()/.rangeBetween() uses default frame — silent correctness difference",
                        "suggestion": "Specify .rowsBetween(Window.unboundedPreceding, Window.currentRow) explicitly",
                    }
                )

        # BNT-W03: first_last_without_ignorenulls
        if method_name in {"first", "last"}:
            has_ignorenulls = any(
                kw.arg == "ignorenulls" for kw in node.keywords
            )
            if not has_ignorenulls:
                self.antipatterns.append(
                    {
                        "name": "first_last_without_ignorenulls",
                        "severity": "WARNING",
                        "line": line_num,
                        "description": f"F.{method_name}() without ignorenulls=True has inconsistent behavior across Spark versions",
                        "suggestion": f"Use F.{method_name}(col, ignorenulls=True)",
                    }
                )

        # BNT-D05: debug_call_in_production — .display() (Databricks display)
        if method_name == "display":
            self.antipatterns.append(
                {
                    "name": "debug_call_in_production",
                    "severity": "WARNING",
                    "line": line_num,
                    "description": ".display() is a Databricks notebook debug call — not for production",
                    "suggestion": "Remove .display() from production code; use structured logging",
                }
            )

        # BNT-P01: streaming_await_termination
        if method_name == "awaitTermination":
            self.antipatterns.append(
                {
                    "name": "streaming_await_termination",
                    "severity": "WARNING",
                    "line": line_num,
                    "description": ".awaitTermination() is unnecessary in Databricks Jobs",
                    "suggestion": "Remove .awaitTermination() from Databricks Job notebooks",
                }
            )

    def _detect_style_rules(self, method_name: str, node: ast_module.Call):
        """Detect remaining BNT-* style rules (P3)."""
        line_num = self._current_line

        # BNT-C01: df_bracket_or_dot_reference — df['col'] or df.col outside join
        # Detected via visit_Subscript and visit_Attribute on known DataFrame variables
        # (complex parent tracking required — skipped in this pass)

        # BNT-C02: selectexpr_in_production
        if method_name == "selectExpr":
            self.antipatterns.append(
                {
                    "name": "selectexpr_in_production",
                    "severity": "INFO",
                    "line": line_num,
                    "description": ".selectExpr() embeds logic in opaque strings that static analysis cannot inspect",
                    "suggestion": "Prefer the Column API (.select(F.col(...))) for lintable code",
                }
            )

        # BNT-N04: with_column_renamed_prefer_alias
        if method_name == "withColumnRenamed":
            self.antipatterns.append(
                {
                    "name": "with_column_renamed_prefer_alias",
                    "severity": "STYLE",
                    "line": line_num,
                    "description": ".withColumnRenamed() is verbose; prefer .alias() inside .select()",
                    "suggestion": "Use df.select(F.col('old').alias('new'), ...) instead",
                }
            )

        # BNT-M01: backslash_chain_continuation — detected at token level, not AST
        # AST does not retain backslash continuations; skipped here

        # BNT-M03: consecutive_with_column_chain (>3 consecutive .withColumn())
        # Tracked in visit_Assign/visit_Expr via assignment chain tracking — approximated here
        # by checking if withColumn appears in the preceding chain multiple times
        if method_name == "withColumn":
            preceding = self._get_preceding_chain_methods(node)
            with_col_count = sum(1 for m in preceding if m == "withColumn")
            if with_col_count >= 3:
                self.antipatterns.append(
                    {
                        "name": "consecutive_with_column_chain",
                        "severity": "WARNING",
                        "line": line_num,
                        "description": f"Chain of {with_col_count + 1} consecutive .withColumn() calls causes O(n²) Catalyst analysis",
                        "suggestion": "Use .withColumns({...}) (Spark 3.3+) or a single .select() statement",
                    }
                )

        # BNT-S02: global_auto_merge_schema — spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", ...)
        if method_name == "set" and node.args and len(node.args) >= 1:
            key = node.args[0]
            if (
                isinstance(key, ast_module.Constant)
                and isinstance(key.value, str)
                and "automerge" in key.value.lower()
            ):
                self.antipatterns.append(
                    {
                        "name": "global_auto_merge_schema",
                        "severity": "WARNING",
                        "line": line_num,
                        "description": "spark.conf.set autoMerge causes unintended schema changes across all write operations",
                        "suggestion": "Use .option('mergeSchema','true') on individual write operations instead",
                    }
                )

        # BNT-Q02: create_temp_view_in_production
        if method_name in {"createOrReplaceTempView", "createTempView"}:
            self.antipatterns.append(
                {
                    "name": "create_temp_view_in_production",
                    "severity": "WARNING",
                    "line": line_num,
                    "description": f".{method_name}() introduces session-global mutable state that breaks test isolation",
                    "suggestion": "Use Spark 3.4+ parameterized SQL or pass DataFrames directly",
                }
            )

        # BNT-L01: empty_string_instead_of_null — F.lit("") or F.lit("NA")
        if method_name == "lit" and node.args:
            arg = node.args[0]
            if isinstance(arg, ast_module.Constant) and arg.value in {"", "NA", "N/A", "null", "NULL", "None"}:
                self.antipatterns.append(
                    {
                        "name": "empty_string_instead_of_null",
                        "severity": "WARNING",
                        "line": line_num,
                        "description": f"F.lit({arg.value!r}) used to represent missing data breaks IS NULL checks and aggregations",
                        "suggestion": "Use F.lit(None).cast(t) to represent missing values",
                    }
                )

        # BNT-SP1: sparksession_in_transform — SparkSession.builder.getOrCreate() in non-entrypoint
        if method_name == "getOrCreate":
            # Check if in a function that is not main/conftest-like
            self.antipatterns.append(
                {
                    "name": "sparksession_in_transform",
                    "severity": "WARNING",
                    "line": line_num,
                    "description": "SparkSession.builder.getOrCreate() inside a function — creates coupling to infrastructure",
                    "suggestion": "Create SparkSession in main() or conftest.py; inject as a parameter",
                }
            )

        # BNT-SP2: conf_set_in_transform — any spark.conf.set in non-setup context
        # (already handled by BNT-S02 for autoMerge; BNT-J01 for shuffle.partitions)
        if method_name == "set" and node.args and len(node.args) >= 1:
            key = node.args[0]
            if (
                isinstance(key, ast_module.Constant)
                and isinstance(key.value, str)
                and "shuffle.partitions" in key.value
            ):
                self.antipatterns.append(
                    {
                        "name": "shuffle_partitions_in_code",
                        "severity": "WARNING",
                        "line": line_num,
                        "description": "spark.sql.shuffle.partitions set in application code — hard to tune without code changes",
                        "suggestion": "Set shuffle partitions in cluster/job configuration or via AQE",
                    }
                )

        # BNT-T02: collect_comparison_in_test — .collect() in assert or comparison
        # Detected in visit_Assert; also flag when collect() appears in a Compare node
        # (approximated: flag collect() calls that are the RHS/LHS of == comparison)
        # Complex parent tracking required — detect a simpler proxy: collect() == collect()

        # BNT-D02: hardcoded_catalog_schema_name
        if method_name in {"table", "sql"} and node.args:
            arg = node.args[0]
            if isinstance(arg, ast_module.Constant) and isinstance(arg.value, str):
                val = arg.value
                # Flag if it looks like a hardcoded catalog/schema (3-level or 2-level name)
                # and does not contain parameter placeholders
                if "." in val and not val.startswith("{") and len(val.split(".")) >= 2:
                    self.antipatterns.append(
                        {
                            "name": "hardcoded_catalog_schema_name",
                            "severity": "WARNING",
                            "line": line_num,
                            "description": f"Hardcoded catalog/schema reference '{val}' breaks across environments",
                            "suggestion": "Define catalog/schema as pipeline configuration parameters, not code literals",
                        }
                    )

        # BNT-D04: python_udf_native_exists — alias of python_udf_builtin_replacement
        # (already detected in _check_udf_for_builtin_replacement)

    @staticmethod
    def _has_alias(node: ast_module.Call) -> bool:
        """Return True if node is wrapped in a .alias() call."""
        # Caller already has the inner Call; it's aliased if it's the func.value
        # of an outer Call whose method is 'alias'. Since we see the inner call
        # here we can't detect this without parent tracking — leave as False for now.
        # (Detection will be enhanced with parent-tracking in a later pass.)
        return False

    def _get_following_chain_methods(self, node: ast_module.Call) -> list[str]:
        """Cannot reliably get 'following' methods in outer-first traversal — return empty."""
        # In the outer-first AST traversal, when we process repartition() we haven't
        # yet seen the outer .write call. This detection is better handled by checking
        # if write follows repartition in the same assignment chain, which requires
        # parent tracking. Returning empty for now; JDBC-style fix can be applied later.
        return []

    def _get_chain_base_name(self, obj) -> str | None:
        """Walk a call chain to find the base Name node and return its id."""
        while obj is not None:
            if isinstance(obj, ast_module.Call) and isinstance(obj.func, ast_module.Attribute):
                obj = obj.func.value
            elif isinstance(obj, ast_module.Attribute):
                obj = obj.value
            elif isinstance(obj, ast_module.Name):
                return obj.id
            else:
                return None
        return None

    def _detect_jdbc_operations(self, method_name: str, node: ast_module.Call):
        """Detect JDBC reads/writes missing required partition options.

        Python ast visits call chains outermost-first (root to leaf), so a state
        machine that sets flags on format("jdbc") and reads them on load() breaks
        on real multi-line chains where load() is processed before format("jdbc").

        Fix: when load()/save() is encountered, walk the receiver chain directly
        to determine whether this is a JDBC operation and which options are set.
        """
        if method_name not in {"load", "save"}:
            return

        is_jdbc = False
        options_found: set[str] = set()

        # Walk from load/save receiver up the chain
        obj = node.func.value if isinstance(node.func, ast_module.Attribute) else None
        while isinstance(obj, ast_module.Call) and isinstance(obj.func, ast_module.Attribute):
            m = obj.func.attr
            if m == "format" and obj.args and isinstance(obj.args[0], ast_module.Constant):
                if obj.args[0].value == "jdbc":
                    is_jdbc = True
            elif m == "jdbc":
                is_jdbc = True
            elif (
                m == "option"
                and obj.args
                and isinstance(obj.args[0], ast_module.Constant)
                and isinstance(obj.args[0].value, str)
            ):
                options_found.add(obj.args[0].value)
            obj = obj.func.value

        if is_jdbc:
            missing = JDBC_REQUIRED_OPTIONS - options_found
            if missing:
                self.antipatterns.append(
                    {
                        "name": "jdbc_incomplete_partition",
                        "severity": "ERROR",
                        "line": self._current_line,
                        "description": f"JDBC read missing required options: {', '.join(sorted(missing))}",
                        "suggestion": "Add partitionColumn, numPartitions, lowerBound, and upperBound options",
                    }
                )

    def _detect_window_operations(self, method_name: str, node: ast_module.Call):
        """Detect Window.orderBy() without .partitionBy().

        Python ast visits chains outermost-first, so orderBy() is always visited
        before partitionBy() in the same chain. The old instance-flag approach
        (_window_partition_present) therefore bleeds state across Window specs.

        Fix: when orderBy()/orderby() is encountered, walk the receiver chain
        to check for partitionBy() AND confirm the chain starts from Window.
        """
        if method_name not in {"orderBy", "orderby"}:
            return

        partition_present = False
        base_name: str | None = None

        obj = node.func.value if isinstance(node.func, ast_module.Attribute) else None
        while obj is not None:
            if isinstance(obj, ast_module.Call) and isinstance(obj.func, ast_module.Attribute):
                if obj.func.attr in {"partitionBy", "partitionby"}:
                    partition_present = True
                obj = obj.func.value
            elif isinstance(obj, ast_module.Attribute):
                obj = obj.value
            elif isinstance(obj, ast_module.Name):
                base_name = obj.id
                break
            else:
                break

        # Only flag if the chain base is the Window class (not a plain DataFrame)
        if base_name not in _WINDOW_CLASS_NAMES:
            return

        if not partition_present:
            self.antipatterns.append(
                {
                    "name": "window_without_partition_by",
                    "severity": "WARNING",
                    "line": self._current_line,
                    "description": "Window .orderBy() without .partitionBy() causes global sort",
                    "suggestion": "Add .partitionBy() before .orderBy() or use .orderBy().limit()",
                }
            )

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
