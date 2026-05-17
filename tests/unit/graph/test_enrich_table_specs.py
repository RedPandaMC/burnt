"""Unit tests for burnt.graph.enrich (TableSpec era)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from burnt.core.enums import NodeKind, ScalingType
from burnt.graph.enrich import (
    DescribeTableSource,
    TableSpec,
    enrich_table_specs,
)
from burnt.graph.model import PyGraph, PyNode, PyTableRef


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


@dataclass
class _Row:
    """Stand-in for a Spark Row with attribute access."""

    col_name: str
    data_type: str = ""

    def __getitem__(self, key: str) -> Any:
        return getattr(self, key)


class _FakeSparkDF:
    def __init__(self, rows: list[_Row]) -> None:
        self._rows = rows

    def collect(self) -> list[_Row]:
        return self._rows


class _FakeSparkSession:
    """Captures issued DESCRIBE queries and replays canned row sets."""

    def __init__(self, table_to_rows: dict[str, list[_Row]]) -> None:
        self._tables = table_to_rows
        self.issued_queries: list[str] = []

    def sql(self, query: str) -> _FakeSparkDF:
        self.issued_queries.append(query)
        # Strip leading "DESCRIBE TABLE EXTENDED " to find the fqn key.
        prefix = "DESCRIBE TABLE EXTENDED "
        if not query.startswith(prefix):
            return _FakeSparkDF([])
        fqn = query[len(prefix):]
        return _FakeSparkDF(self._tables.get(fqn, []))


# ---------------------------------------------------------------------------
# enrich_table_specs basic shape
# ---------------------------------------------------------------------------


def _graph_with_refs(refs_per_node: list[list[PyTableRef]]) -> PyGraph:
    g = PyGraph()
    for i, refs in enumerate(refs_per_node):
        g.add_node(
            PyNode(
                id=f"n{i}",
                kind=NodeKind.READ,
                scaling_type=ScalingType.LINEAR,
                tables_referenced=refs,
            )
        )
    return g


def test_enrich_table_specs_routes_unique_refs_to_source() -> None:
    class _CapturingSource:
        def __init__(self) -> None:
            self.calls: list[list[Any]] = []

        def fetch(self, refs: Any) -> dict[str, TableSpec]:
            refs_list = list(refs)
            self.calls.append(refs_list)
            return {r.fqn: TableSpec(fqn=r.fqn, size_bytes=10) for r in refs_list}

    g = _graph_with_refs(
        [
            [PyTableRef(raw="cat.s.t", table="t", catalog="cat", schema="s")],
            [
                PyTableRef(raw="cat.s.t", table="t", catalog="cat", schema="s"),
                PyTableRef(raw="other", table="other"),
            ],
        ]
    )
    src = _CapturingSource()
    out = enrich_table_specs(g, source=src)
    # Dedupe by fqn — cat.s.t appears twice in the graph but once in the call.
    assert len(src.calls) == 1
    fqns_sent = sorted(r.fqn for r in src.calls[0])
    assert fqns_sent == ["cat.s.t", "other"]
    assert sorted(out.keys()) == ["cat.s.t", "other"]


def test_enrich_table_specs_swallows_source_errors() -> None:
    class _BrokenSource:
        def fetch(self, refs: Any) -> dict[str, TableSpec]:
            raise RuntimeError("simulated source failure")

    g = _graph_with_refs([[PyTableRef(raw="x", table="x")]])
    assert enrich_table_specs(g, source=_BrokenSource()) == {}


def test_enrich_table_specs_no_refs_no_source_call() -> None:
    g = _graph_with_refs([[], []])

    class _MustNotBeCalled:
        def fetch(self, refs: Any) -> dict[str, TableSpec]:
            raise AssertionError("source should not be invoked")

    assert enrich_table_specs(g, source=_MustNotBeCalled()) == {}


def test_enrich_table_specs_accepts_resolved_graph_wrapper() -> None:
    """Duck-typed acceptance — anything with .graph.nodes works."""

    @dataclass
    class _FakeResolved:
        graph: Any

    class _Source:
        def fetch(self, refs: Any) -> dict[str, TableSpec]:
            return {r.fqn: TableSpec(fqn=r.fqn) for r in refs}

    g = _graph_with_refs([[PyTableRef(raw="cat.s.t", table="t", catalog="cat", schema="s")]])
    out = enrich_table_specs(_FakeResolved(graph=g), source=_Source())
    assert "cat.s.t" in out


# ---------------------------------------------------------------------------
# DescribeTableSource — issued query + row parsing
# ---------------------------------------------------------------------------


def test_describe_source_skips_path_reads() -> None:
    session = _FakeSparkSession(table_to_rows={})
    src = DescribeTableSource(session)
    refs = [
        PyTableRef(
            raw="s3://b/k", table="k", is_path_read=True, path="s3://b/k"
        ),
        PyTableRef(raw="cat.s.t", table="t", catalog="cat", schema="s"),
    ]
    src.fetch(refs)
    # Only the non-path ref triggers a query.
    assert session.issued_queries == ["DESCRIBE TABLE EXTENDED cat.s.t"]


def test_describe_source_parses_extended_information_section() -> None:
    rows = [
        _Row("id", "bigint"),
        _Row("user_id", "string"),
        _Row("", ""),
        _Row("# Detailed Table Information", ""),
        _Row("Name", "cat.s.t"),
        _Row("Type", "MANAGED"),
        _Row("Location", "s3://bucket/warehouse/cat/s/t"),
        _Row("Provider", "delta"),
        _Row("Statistics", "2147483648 bytes, 5000000 rows"),
        _Row("", ""),
        _Row("# Partition Information", ""),
        _Row("col_name", "data_type"),
        _Row("dt", "string"),
        _Row("region", "string"),
    ]
    session = _FakeSparkSession({"cat.s.t": rows})
    src = DescribeTableSource(session)
    out = src.fetch([PyTableRef(raw="cat.s.t", table="t", catalog="cat", schema="s")])
    spec = out["cat.s.t"]
    assert spec.size_bytes == 2_147_483_648
    assert spec.row_count == 5_000_000
    assert spec.is_managed is True
    assert spec.file_format == "delta"
    assert spec.location == "s3://bucket/warehouse/cat/s/t"
    assert spec.partition_columns == ("dt", "region")


def test_describe_source_handles_missing_statistics() -> None:
    rows = [
        _Row("id", "bigint"),
        _Row("# Detailed Table Information", ""),
        _Row("Provider", "parquet"),
        _Row("Type", "EXTERNAL"),
    ]
    session = _FakeSparkSession({"cat.s.t": rows})
    src = DescribeTableSource(session)
    spec = src.fetch([PyTableRef(raw="cat.s.t", table="t", catalog="cat", schema="s")])[
        "cat.s.t"
    ]
    assert spec.size_bytes is None
    assert spec.row_count is None
    assert spec.is_managed is False
    assert spec.file_format == "parquet"


def test_describe_source_per_ref_failures_drop_fqn_not_others() -> None:
    class _PartiallyFailingSession:
        def __init__(self) -> None:
            self.issued_queries: list[str] = []

        def sql(self, query: str) -> Any:
            self.issued_queries.append(query)
            if "bad" in query:
                raise RuntimeError("simulated table missing")

            class _OK:
                def collect(self_inner) -> list[_Row]:
                    return [
                        _Row("# Detailed Table Information", ""),
                        _Row("Provider", "parquet"),
                    ]

            return _OK()

    session = _PartiallyFailingSession()
    src = DescribeTableSource(session)
    out = src.fetch(
        [
            PyTableRef(raw="bad.t", table="t", catalog="bad"),
            PyTableRef(raw="good.t", table="t", catalog="good"),
        ]
    )
    assert "bad.t" not in out
    assert "good.t" in out
    assert out["good.t"].file_format == "parquet"


def test_describe_source_dedupes_same_fqn_once() -> None:
    """If the graph references the same table twice we issue one DESCRIBE."""
    session = _FakeSparkSession(
        {
            "cat.s.t": [_Row("# Detailed Table Information", ""), _Row("Provider", "delta")]
        }
    )
    src = DescribeTableSource(session)
    ref = PyTableRef(raw="cat.s.t", table="t", catalog="cat", schema="s")
    src.fetch([ref, ref, ref])
    assert session.issued_queries == ["DESCRIBE TABLE EXTENDED cat.s.t"]
