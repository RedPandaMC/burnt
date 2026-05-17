"""Table-spec enrichment plumbing for ``burnt._check``.

After the resolved-graph refactor, the only Python-side helpers
exposed here are:

* :class:`TableSpec` — dataclass holding the per-table metadata that
  feeds into ``estimate()`` as a second-priority input-bytes source.
* :class:`TableSpecSource` — Protocol for any object that can fetch
  specs given a sequence of ``PyTableRef``-shaped values.
* :class:`DescribeTableSource` — the production implementation,
  cross-platform across OSS Spark and Databricks (UC + non-UC),
  issuing ``DESCRIBE TABLE EXTENDED <fqn>`` and parsing the rows it
  emits.
* :func:`enrich_table_specs` — collects unique non-path table refs out
  of a resolved graph (or any graph-like object with a
  ``distinct_table_refs`` or ``.nodes[].tables_referenced``), calls a
  source, and returns ``{fqn: TableSpec}``.

The previous ``enrich_graph`` REST-correlation helper lived here too;
that logic now sits behind ``burnt._engine._resolve_graph`` and is
reachable only through ``_check`` per the architectural firewall.
The legacy ``enrich_dlt`` stub stays as-is — it has no DLT body and
can be replaced when that integration lands.
"""

from __future__ import annotations

import contextlib
import re
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, Protocol


@dataclass(frozen=True, slots=True)
class TableSpec:
    """Lightweight Python-side payload describing a Spark table.

    All numeric fields are ``int | None``. ``None`` means the source
    couldn't determine the value — not zero. The estimator distinguishes
    between the two when computing scaling-input fallbacks.
    """

    fqn: str
    size_bytes: int | None = None
    num_files: int | None = None
    num_partitions: int | None = None
    row_count: int | None = None
    file_format: str | None = None
    location: str | None = None
    is_managed: bool | None = None
    partition_columns: tuple[str, ...] = ()


class TableSpecSource(Protocol):
    """Anything that can produce :class:`TableSpec`s from a sequence of refs.

    Implementations are expected to be side-effect-free at the Python
    level and to absorb their own errors — :func:`enrich_table_specs`
    will not introspect raised exceptions.
    """

    def fetch(self, refs: Iterable[Any]) -> dict[str, TableSpec]:
        ...


# ---------------------------------------------------------------------------
# DescribeTableSource — the production source
# ---------------------------------------------------------------------------


class DescribeTableSource:
    """Issue ``DESCRIBE TABLE EXTENDED <fqn>`` per non-path ref.

    Works on OSS Spark (≥3.0), EMR, Glue, Dataproc, on-prem clusters,
    and Databricks (UC + non-UC) — anywhere an active ``SparkSession``
    is reachable. Skips path-based refs because ``DESCRIBE`` does not
    accept paths consistently across file formats.

    Per-ref exceptions are caught: a failing ``fqn`` simply does not
    appear in the result. This stops one bad table from poisoning the
    rest of an enrichment pass.
    """

    def __init__(self, spark: Any) -> None:
        self._spark = spark

    def fetch(self, refs: Iterable[Any]) -> dict[str, TableSpec]:
        out: dict[str, TableSpec] = {}
        for ref in _dedupe_refs(refs):
            if getattr(ref, "is_path_read", False):
                continue
            fqn = getattr(ref, "fqn", None) or getattr(ref, "raw", None)
            if not fqn:
                continue
            with contextlib.suppress(Exception):
                rows = list(self._spark.sql(f"DESCRIBE TABLE EXTENDED {fqn}").collect())
                spec = _parse_describe_rows(fqn, rows)
                if spec is not None:
                    out[fqn] = spec
        return out


def enrich_table_specs(
    graph_like: Any,
    *,
    source: TableSpecSource,
) -> dict[str, TableSpec]:
    """Return ``{fqn: TableSpec}`` for unique refs in ``graph_like``.

    ``graph_like`` may be:

    * A ``PyResolvedGraph`` (exposes ``distinct_table_refs``-equivalent
      via iterating ``node_ids()`` → ``overlay`` → ``…`` is not
      necessary; we walk the underlying ``graph.nodes``).
    * A bare ``PyGraph`` / pure-Python ``PyGraph`` with ``.nodes``.
    * Anything else with a ``.nodes`` attribute whose entries carry
      ``tables_referenced``.

    Unknown / unreachable fqns are simply absent — never raised.
    """
    refs = _dedupe_refs(_collect_refs(graph_like))
    if not refs:
        return {}
    try:
        return source.fetch(refs)
    except Exception:
        return {}


def enrich_dlt(
    pipeline_id: str,
    *,
    warehouse_id: str | None = None,
) -> dict[str, Any]:
    """Stub — DLT pipeline enrichment is tracked separately."""
    return {"pipeline_id": pipeline_id, "warehouse_id": warehouse_id, "tables": []}


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------


def _collect_refs(graph_like: Any) -> Iterable[Any]:
    """Walk ``graph_like.graph.nodes`` (if a resolved graph) or
    ``graph_like.nodes`` (if a bare graph) and yield every ``TableRef``."""
    if graph_like is None:
        return
    # PyResolvedGraph exposes its inner graph via .graph; bare graphs are
    # already that shape.
    inner = getattr(graph_like, "graph", graph_like)
    nodes = getattr(inner, "nodes", None) or []
    for node in nodes:
        for tref in getattr(node, "tables_referenced", []) or []:
            yield tref


def _dedupe_refs(refs: Iterable[Any]) -> list[Any]:
    """Dedupe by ``fqn`` while preserving first-seen order."""
    seen: set[str] = set()
    out: list[Any] = []
    for r in refs:
        fqn = getattr(r, "fqn", None) or getattr(r, "raw", None)
        if not fqn or fqn in seen:
            continue
        seen.add(fqn)
        out.append(r)
    return out


# Spark's DESCRIBE TABLE EXTENDED output uses two columns: col_name and
# data_type. The "# Detailed Table Information" section is what we want.
_BYTES_RE = re.compile(
    r"([\d,]+)\s*bytes(?:,\s*([\d,]+)\s*rows)?", re.IGNORECASE
)


def _parse_describe_rows(fqn: str, rows: list[Any]) -> TableSpec | None:
    """Parse the list of Rows returned by ``DESCRIBE TABLE EXTENDED``.

    Each row exposes ``col_name`` and ``data_type`` attributes (Spark's
    standard column names for this output). We pull out the well-known
    extended-information rows; anything we don't recognise is ignored.
    """
    info: dict[str, str] = {}
    partition_cols: list[str] = []
    in_partition_section = False
    in_detailed_section = False

    for row in rows:
        col = _row_value(row, "col_name") or ""
        val = _row_value(row, "data_type") or ""
        col_s = col.strip()
        val_s = val.strip()

        if col_s.startswith("#"):
            section = col_s.lower()
            in_detailed_section = "detailed" in section and "information" in section
            in_partition_section = (
                "partition" in section and "information" in section
            )
            continue

        if in_partition_section and col_s and not col_s.startswith("#"):
            # The partition information block lists column headers followed
            # by rows with col_name + data_type. We capture the names.
            if col_s.lower() not in ("col_name", ""):
                partition_cols.append(col_s)
            continue

        if in_detailed_section and col_s:
            info[col_s.lower()] = val_s

    size_bytes, row_count = _parse_statistics(info.get("statistics"))
    is_managed = _parse_managed(info.get("type"))

    return TableSpec(
        fqn=fqn,
        size_bytes=size_bytes,
        num_files=None,
        num_partitions=None,
        row_count=row_count,
        file_format=(info.get("provider") or None) or None,
        location=info.get("location") or None,
        is_managed=is_managed,
        partition_columns=tuple(partition_cols),
    )


def _row_value(row: Any, key: str) -> Any:
    """Spark Row supports both attribute and index access; try both."""
    value = getattr(row, key, None)
    if value is not None:
        return value
    try:
        return row[key]
    except Exception:
        return None


def _parse_statistics(stats: str | None) -> tuple[int | None, int | None]:
    """Extract ``(size_bytes, row_count)`` from a Spark stats string.

    Format: ``"1234 bytes"`` or ``"1234 bytes, 50 rows"``.
    """
    if not stats:
        return (None, None)
    m = _BYTES_RE.search(stats)
    if m is None:
        return (None, None)
    size = int(m.group(1).replace(",", ""))
    rows = int(m.group(2).replace(",", "")) if m.group(2) else None
    return (size, rows)


def _parse_managed(type_field: str | None) -> bool | None:
    if not type_field:
        return None
    t = type_field.strip().upper()
    if t == "MANAGED":
        return True
    if t == "EXTERNAL":
        return False
    return None
