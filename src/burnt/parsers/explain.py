"""Parser for Databricks EXPLAIN COST output."""

from __future__ import annotations

import re

from ..core.exceptions import ParseError
from ..core.models import ExplainPlan, OperationInfo

# ---------------------------------------------------------------------------
# Unit conversion tables
# ---------------------------------------------------------------------------

_SIZE_MULTIPLIERS: dict[str, int] = {
    "B": 1,
    "KiB": 1024,
    "MiB": 1024**2,
    "GiB": 1024**3,
    "TiB": 1024**4,
}

_ROW_MULTIPLIERS: dict[str, int] = {
    "": 1,
    "K": 1_000,
    "M": 1_000_000,
    "B": 1_000_000_000,
}

# ---------------------------------------------------------------------------
# Compiled regex patterns (per docs/explain-cost-schema.md Section 2)
# ---------------------------------------------------------------------------

# Matches the "== Optimized Logical Plan ==" section header (exact line).
_HEADER_PATTERN = re.compile(r"^== Optimized Logical Plan ==$", re.MULTILINE)

# Extracts the optimized plan section up to the next "==" header or end of string.
_SECTION_PATTERN = re.compile(
    r"== Optimized Logical Plan ==\n(.*?)(?=\n==|\Z)",
    re.DOTALL,
)

# Matches a Statistics(...) block, capturing size and optional row count.
_STATS_PATTERN = re.compile(
    r"Statistics\(sizeInBytes=(?P<size_val>[\d.]+)\s*(?P<size_unit>[KMGTiB]+)"
    r"(?:,\s*rowCount=(?P<row_val>[\d.E+]+)\s*(?P<row_unit>[KMB]?))?\)",
    re.IGNORECASE,
)

# Detects join type names.
_JOIN_PATTERN = re.compile(
    r"\b(BroadcastHashJoin|SortMergeJoin|ShuffledHashJoin|CartesianProduct)\b"
)

# Detects Exchange and Sort shuffle operators (after stripping indentation markers).
_SHUFFLE_LINE_PATTERN = re.compile(
    r"^\s*(?::[- ]+|[+]- )?(Exchange |Sort \[)",
    re.MULTILINE,
)

# Counts plan depth — lines containing "+- ".
_DEPTH_PATTERN = re.compile(r"\+- ", re.MULTILINE)

# Operator classification: first token after stripping indentation markers.
_OP_TOKEN_PATTERN = re.compile(r"^\s*(?::[- ]+|[+]- )?\s*(\w+)")

# Weights and kinds for operator tokens (from Section 4 / Appendix).
_OP_TABLE: dict[str, tuple[str, int]] = {
    "BroadcastHashJoin": ("join", 5),
    "SortMergeJoin": ("join", 15),
    "ShuffledHashJoin": ("join", 10),
    "CartesianProduct": ("join", 50),
    "Exchange": ("shuffle", 8),
    "Sort": ("sort", 3),
    "Aggregate": ("aggregate", 5),
    "Relation": ("scan", 1),
    "CTERelationRef": ("scan", 1),
    "CTERelationDef": ("scan", 1),
    "WithCTE": ("scan", 1),
}


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


def _size_to_bytes(val: str, unit: str) -> int:
    """Convert a size string and unit to bytes."""
    multiplier = _SIZE_MULTIPLIERS.get(unit, 1)
    return int(float(val) * multiplier)


def _rows_to_int(val: str, unit: str) -> int:
    """Convert a row count string and unit suffix to an integer."""
    multiplier = _ROW_MULTIPLIERS.get(unit, 1)
    return int(float(val) * multiplier)


def _extract_optimized_section(text: str) -> str:
    """Extract the Optimized Logical Plan section from a full EXPLAIN output.

    Raises ParseError if the required header is not found.
    """
    if not _HEADER_PATTERN.search(text):
        raise ParseError("Missing '== Optimized Logical Plan ==' header")
    match = _SECTION_PATTERN.search(text)
    if not match:
        raise ParseError("Missing '== Optimized Logical Plan ==' header")
    return match.group(1)


def _is_sort_not_join(line: str) -> bool:
    """Return True if the line starts with 'Sort [' (not SortMergeJoin)."""
    stripped = re.sub(r"^\s*(?::[- ]+|[+]- )\s*", "", line)
    return stripped.startswith("Sort [")


def _classify_operator(line: str) -> OperationInfo | None:
    """Return an OperationInfo for a plan line, or None if operator is skipped."""
    token_match = _OP_TOKEN_PATTERN.match(line)
    if not token_match:
        return None
    token = token_match.group(1)

    # Sort lines must be followed by '[' to qualify as sort operators.
    if token == "Sort" and not _is_sort_not_join(line):
        return None

    entry = _OP_TABLE.get(token)
    if entry is None:
        return None
    kind, weight = entry
    return OperationInfo(name=token, kind=kind, weight=weight)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def parse_explain_cost(text: str) -> ExplainPlan:
    """Parse a Databricks EXPLAIN COST output string into a structured ExplainPlan.

    Raises ParseError if the input is empty, whitespace-only, or missing the
    required '== Optimized Logical Plan ==' header.
    """
    if not text or not text.strip():
        raise ParseError("Empty EXPLAIN COST output")

    plan_section = _extract_optimized_section(text)

    # ---- statistics --------------------------------------------------------
    total_size_bytes = 0
    estimated_rows: int | None = None
    all_have_row_count = True
    has_any_stats = False
    root_row_count_set = False

    for stats_match in _STATS_PATTERN.finditer(plan_section):
        size_bytes = _size_to_bytes(
            stats_match.group("size_val"), stats_match.group("size_unit")
        )
        has_any_stats = True
        total_size_bytes = max(total_size_bytes, size_bytes)

        row_val_str = stats_match.group("row_val")
        if row_val_str:
            row_unit = (stats_match.group("row_unit") or "").strip()
            rows = _rows_to_int(row_val_str, row_unit)
            if not root_row_count_set:
                estimated_rows = rows
                root_row_count_set = True
        else:
            all_have_row_count = False

    stats_complete = has_any_stats and all_have_row_count

    # ---- join types --------------------------------------------------------
    join_types: list[str] = []
    for join_match in _JOIN_PATTERN.finditer(plan_section):
        jt = join_match.group(1)
        if jt not in join_types:
            join_types.append(jt)

    # ---- shuffle count -----------------------------------------------------
    shuffle_count = len(_SHUFFLE_LINE_PATTERN.findall(plan_section))

    # ---- plan depth --------------------------------------------------------
    plan_depth = len(_DEPTH_PATTERN.findall(plan_section))

    # ---- operator list -----------------------------------------------------
    operations: list[OperationInfo] = []
    for line in plan_section.splitlines():
        op = _classify_operator(line)
        if op is not None:
            operations.append(op)

    return ExplainPlan(
        total_size_bytes=total_size_bytes,
        estimated_rows=estimated_rows,
        join_types=join_types,
        shuffle_count=shuffle_count,
        plan_depth=plan_depth,
        stats_complete=stats_complete,
        raw_plan=text,
        operations=operations,
    )
