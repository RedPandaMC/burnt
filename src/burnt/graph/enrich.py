"""Graph enrichment helpers consumed by ``burnt._check`` only.

The previous ``enrich_graph`` REST-correlation helper has been removed:
that logic now lives in Rust (``src/burnt-engine/src/resolved/merge.rs``)
behind the ``_resolve_graph`` PyO3 entry point, and the single
consumer (``_check._merge_runtime``) imports the resolved graph
directly. The line-number regex is no longer duplicated between
Python and Rust.

What remains in this module is the table-spec enrichment plumbing
that lands in a follow-up commit: ``TableSpec``, the
``TableSpecSource`` protocol, ``DescribeTableSource``, and
``enrich_table_specs``. The stub ``enrich_dlt`` is preserved for
its existing call site.
"""

from __future__ import annotations

from typing import Any


def enrich_dlt(
    pipeline_id: str,
    *,
    warehouse_id: str | None = None,
) -> dict[str, Any]:
    """Stub — DLT pipeline enrichment is tracked separately."""
    return {"pipeline_id": pipeline_id, "warehouse_id": warehouse_id, "tables": []}
