"""Unit tests for the EXPLAIN COST output parser."""

import pytest

from burnt.core.exceptions import ParseError
from burnt.parsers.explain import parse_explain_cost

# ---------------------------------------------------------------------------
# Fixture plan strings (no network calls)
# ---------------------------------------------------------------------------

SIMPLE_SCAN_NO_ROWCOUNT = """== Optimized Logical Plan ==
Aggregate [product_id#70], [product_id#70, count(1) AS cnt#71]   Statistics(sizeInBytes=8.0 B)
+- Relation catalog.raw.events [product_id#70] parquet   Statistics(sizeInBytes=8.0 B)
"""

FULL_STATS_PLAN = """== Optimized Logical Plan ==
Aggregate [customer_id#1], [customer_id#1, sum(amount#2) AS total#3]   Statistics(sizeInBytes=8.0 B)
+- Filter (amount#2 > 0)   Statistics(sizeInBytes=13.5 MiB, rowCount=191.9K)
   +- Relation catalog.sales.orders [customer_id#1,amount#2] parquet   Statistics(sizeInBytes=23.5 MiB, rowCount=1.5M)
"""

BROADCAST_JOIN_PLAN = """== Optimized Logical Plan ==
Project [order_id#10, customer_name#21, amount#11]   Statistics(sizeInBytes=1.2 MiB, rowCount=42.0K)
+- BroadcastHashJoin [customer_id#12], [customer_id#22], Inner, BuildRight   Statistics(sizeInBytes=2.1 MiB, rowCount=42.0K)
   :- Filter (isnotnull(status#13) AND (status#13 = SHIPPED))               Statistics(sizeInBytes=18.4 MiB, rowCount=420.0K)
   :  +- Relation catalog.sales.orders [order_id#10,customer_id#12,amount#11,status#13] parquet   Statistics(sizeInBytes=40.2 MiB, rowCount=1.0M)
   +- Relation catalog.sales.customers [customer_id#22,customer_name#21] parquet   Statistics(sizeInBytes=512.0 KiB, rowCount=50.0K)
"""

SORT_MERGE_JOIN_PLAN = """== Optimized Logical Plan ==
Sort [fact_id#30 ASC NULLS FIRST]   Statistics(sizeInBytes=28.6 GiB, rowCount=102.4M)
+- SortMergeJoin [dim_b_key#32], [key#51], Inner   Statistics(sizeInBytes=28.6 GiB, rowCount=102.4M)
   :- Exchange hashpartitioning(dim_b_key#32, 200)   Statistics(sizeInBytes=22.1 GiB, rowCount=102.4M)
   :  +- Relation catalog.dw.fact_table [fact_id#30] parquet   Statistics(sizeInBytes=134.6 GiB, rowCount=2.88B)
   +- Exchange hashpartitioning(key#51, 200)   Statistics(sizeInBytes=800.0 MiB, rowCount=8.0M)
      +- Relation catalog.dw.dimension_b [key#51,dim_value#52] parquet   Statistics(sizeInBytes=1.2 GiB, rowCount=8.0M)
"""

MULTI_SECTION_PLAN = """== Parsed Logical Plan ==
'Aggregate [...]
+- 'Filter ...

== Analyzed Logical Plan ==
Aggregate [sku_name#1], [sku_name#1, sum(price#2) AS total_price#3]
+- Filter ...

== Optimized Logical Plan ==
Aggregate [sku_name#1], [sku_name#1, sum(price#2) AS total_price#3]   Statistics(sizeInBytes=8.0 B)
+- Filter (isnotnull(region#4) AND (region#4 = EMEA))                 Statistics(sizeInBytes=13.5 MiB, rowCount=191.9K)
   +- Relation catalog.sales.line_items [sku_name#1,price#2,region#4] parquet   Statistics(sizeInBytes=23.5 MiB, rowCount=1.5M)

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- == Final Plan ==
"""

CTE_PLAN = """== Optimized Logical Plan ==
Aggregate [customer_id#60], [customer_id#60, sum(amount#61) AS total#62]   Statistics(sizeInBytes=16.0 B)
+- Filter (amount#61 > scalar-subquery#63 [])   Statistics(sizeInBytes=5.8 MiB, rowCount=86.0K)
   :  +- Aggregate [], [avg(amount#64) AS avg(amount)#65]   Statistics(sizeInBytes=8.0 B)
   :     +- Relation catalog.sales.orders [amount#64] parquet   Statistics(sizeInBytes=40.2 MiB, rowCount=1.0M)
   +- CTERelationRef recent_orders, [order_id#66,customer_id#60,amount#61]   Statistics(sizeInBytes=12.2 MiB, rowCount=180.0K)
      +- WithCTE   Statistics(sizeInBytes=12.2 MiB, rowCount=180.0K)
         +- CTERelationDef recent_orders#67   Statistics(sizeInBytes=12.2 MiB, rowCount=180.0K)
            +- Filter (isnotnull(order_date#68) AND (order_date#68 >= 2025-01-01))   Statistics(sizeInBytes=12.2 MiB, rowCount=180.0K)
               +- Relation catalog.sales.orders [order_id#66,customer_id#60,amount#61,order_date#68] parquet   Statistics(sizeInBytes=40.2 MiB, rowCount=1.0M)
"""

PLAN_WITH_SCIENTIFIC_NOTATION = """== Optimized Logical Plan ==
Aggregate [id#1], [id#1, count(1) AS cnt#2]   Statistics(sizeInBytes=1.5 GiB, rowCount=1.62E+6)
+- Relation catalog.events [id#1] parquet   Statistics(sizeInBytes=4.0 GiB, rowCount=1.62E+6)
"""

NO_STATS_PLAN = """== Optimized Logical Plan ==
Aggregate [id#1], [id#1, count(1) AS cnt#2]
+- Relation catalog.events [id#1] parquet
"""

PLACEHOLDER_ONLY_PLAN = """== Optimized Logical Plan ==
Aggregate [product_id#70], [product_id#70, count(1) AS cnt#71]   Statistics(sizeInBytes=8.0 B)
+- Relation catalog.raw.events [product_id#70] parquet   Statistics(sizeInBytes=8.0 B)
"""


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestParseExplainCostErrors:
    """Tests for input validation and error handling."""

    def test_empty_string_raises_parse_error(self) -> None:
        """Empty string input must raise ParseError."""
        with pytest.raises(ParseError, match="Empty EXPLAIN COST output"):
            parse_explain_cost("")

    def test_whitespace_only_raises_parse_error(self) -> None:
        """Whitespace-only input must raise ParseError."""
        with pytest.raises(ParseError, match="Empty EXPLAIN COST output"):
            parse_explain_cost("   \n\t  ")

    def test_missing_header_raises_parse_error(self) -> None:
        """Input without the required header must raise ParseError."""
        with pytest.raises(
            ParseError, match="Missing '== Optimized Logical Plan ==' header"
        ):
            parse_explain_cost("SELECT * FROM foo")

    def test_simple_plan_no_header_raises_parse_error(self) -> None:
        """Plan text without section header raises ParseError."""
        with pytest.raises(ParseError):
            parse_explain_cost("Aggregate [...]\n+- Relation ... parquet")


class TestParseExplainCostStats:
    """Tests for Statistics block parsing."""

    def test_simple_scan_no_rowcount_stats_incomplete(self) -> None:
        """Plan with sizeInBytes only → stats_complete=False, estimated_rows=None."""
        result = parse_explain_cost(SIMPLE_SCAN_NO_ROWCOUNT)
        assert result.stats_complete is False
        assert result.estimated_rows is None

    def test_full_stats_plan_stats_complete(self) -> None:
        """Plan where all non-placeholder operators have rowCount → stats_complete=True."""
        result = parse_explain_cost(FULL_STATS_PLAN)
        # Root aggregate has no rowCount (8.0 B placeholder), so stats_complete=False
        assert result.stats_complete is False
        # estimated_rows comes from the first Stats block that has rowCount
        assert result.estimated_rows == 191_900  # 191.9K

    def test_broadcast_join_plan_estimated_rows(self) -> None:
        """Plan with all operators having rowCount → stats_complete=True."""
        result = parse_explain_cost(BROADCAST_JOIN_PLAN)
        assert result.stats_complete is True
        # Root (Project) rowCount=42.0K
        assert result.estimated_rows == 42_000

    def test_size_unit_mib_normalization(self) -> None:
        """23.5 MiB should normalize to correct byte count."""
        result = parse_explain_cost(FULL_STATS_PLAN)
        # Max size in plan is 23.5 MiB = 23.5 * 1048576
        expected = int(23.5 * 1024**2)
        assert result.total_size_bytes == expected

    def test_size_unit_kib_normalization(self) -> None:
        """512.0 KiB in broadcast join plan should normalize to correct bytes."""
        result = parse_explain_cost(BROADCAST_JOIN_PLAN)
        # Max is 40.2 MiB (orders table)
        expected = int(40.2 * 1024**2)
        assert result.total_size_bytes == expected

    def test_row_unit_k_normalization(self) -> None:
        """rowCount=191.9K should produce 191900."""
        result = parse_explain_cost(FULL_STATS_PLAN)
        assert result.estimated_rows == 191_900

    def test_row_unit_m_normalization(self) -> None:
        """rowCount=1.5M in the leaf node should produce 1500000."""
        result = parse_explain_cost(FULL_STATS_PLAN)
        # 191.9K is the first rowCount seen (Filter node), not 1.5M
        assert result.estimated_rows == 191_900

    def test_row_unit_b_normalization(self) -> None:
        """rowCount=2.88B should produce 2880000000 (billions, not bytes)."""
        result = parse_explain_cost(SORT_MERGE_JOIN_PLAN)
        # Root is Sort with rowCount=102.4M
        assert result.estimated_rows == 102_400_000

    def test_scientific_notation_rowcount(self) -> None:
        """rowCount=1.62E+6 should parse correctly to 1620000."""
        result = parse_explain_cost(PLAN_WITH_SCIENTIFIC_NOTATION)
        assert result.estimated_rows == 1_620_000

    def test_placeholder_size_total_bytes_is_max(self) -> None:
        """Placeholder 8.0 B stats → total_size_bytes is 8 (max of placeholders)."""
        result = parse_explain_cost(PLACEHOLDER_ONLY_PLAN)
        assert result.total_size_bytes == 8
        assert result.stats_complete is False

    def test_no_stats_blocks(self) -> None:
        """Plan with no Statistics blocks → total_size_bytes=0, stats_complete=False."""
        result = parse_explain_cost(NO_STATS_PLAN)
        assert result.total_size_bytes == 0
        assert result.estimated_rows is None
        assert result.stats_complete is False

    def test_gib_size_normalization(self) -> None:
        """28.6 GiB should normalize to correct byte count."""
        result = parse_explain_cost(SORT_MERGE_JOIN_PLAN)
        # Max is 134.6 GiB
        expected = int(134.6 * 1024**3)
        assert result.total_size_bytes == expected


class TestParseExplainCostJoins:
    """Tests for join type detection."""

    def test_broadcast_hash_join_detected(self) -> None:
        """BroadcastHashJoin should appear in join_types."""
        result = parse_explain_cost(BROADCAST_JOIN_PLAN)
        assert "BroadcastHashJoin" in result.join_types

    def test_sort_merge_join_detected(self) -> None:
        """SortMergeJoin should appear in join_types."""
        result = parse_explain_cost(SORT_MERGE_JOIN_PLAN)
        assert "SortMergeJoin" in result.join_types

    def test_no_joins_in_simple_scan(self) -> None:
        """Simple scan plan should have empty join_types."""
        result = parse_explain_cost(SIMPLE_SCAN_NO_ROWCOUNT)
        assert result.join_types == []

    def test_join_types_deduplicated(self) -> None:
        """Same join type appearing twice should only appear once in join_types."""
        plan = """== Optimized Logical Plan ==
SortMergeJoin [a#1], [b#2], Inner   Statistics(sizeInBytes=1.0 GiB, rowCount=1.0M)
+- SortMergeJoin [c#3], [d#4], Inner   Statistics(sizeInBytes=500.0 MiB, rowCount=500.0K)
   +- Relation t1 parquet   Statistics(sizeInBytes=100.0 MiB, rowCount=1.0M)
   +- Relation t2 parquet   Statistics(sizeInBytes=100.0 MiB, rowCount=1.0M)
+- Relation t3 parquet   Statistics(sizeInBytes=100.0 MiB, rowCount=1.0M)
"""
        result = parse_explain_cost(plan)
        assert result.join_types.count("SortMergeJoin") == 1


class TestParseExplainCostShuffleAndDepth:
    """Tests for shuffle_count and plan_depth."""

    def test_sort_merge_join_shuffle_count(self) -> None:
        """SortMergeJoin plan with 2 Exchange + 1 Sort → shuffle_count=3."""
        result = parse_explain_cost(SORT_MERGE_JOIN_PLAN)
        # Sort [ + 2x Exchange = 3
        assert result.shuffle_count == 3

    def test_broadcast_join_no_shuffle(self) -> None:
        """BroadcastHashJoin does not require Exchange — shuffle_count=0."""
        result = parse_explain_cost(BROADCAST_JOIN_PLAN)
        assert result.shuffle_count == 0

    def test_plan_depth_counts_plus_minus_lines(self) -> None:
        """Plan depth should count lines containing '+- '."""
        result = parse_explain_cost(FULL_STATS_PLAN)
        # FULL_STATS_PLAN has 2 lines with "+- "
        assert result.plan_depth == 2

    def test_plan_depth_sort_merge_join(self) -> None:
        """Sort merge join plan depth should count all '+- ' lines."""
        result = parse_explain_cost(SORT_MERGE_JOIN_PLAN)
        # Count "+- " occurrences in the optimized section
        import re

        section_start = SORT_MERGE_JOIN_PLAN.index(
            "== Optimized Logical Plan ==\n"
        ) + len("== Optimized Logical Plan ==\n")
        section_text = SORT_MERGE_JOIN_PLAN[section_start:]
        expected_depth = len(re.findall(r"\+- ", section_text))
        assert result.plan_depth == expected_depth


class TestParseExplainCostMultiSection:
    """Tests for multi-section EXPLAIN output."""

    def test_multi_section_only_parses_optimized(self) -> None:
        """Physical Plan section operators should NOT be counted."""
        result = parse_explain_cost(MULTI_SECTION_PLAN)
        # The Physical Plan section has "+- == Final Plan ==" — should not be counted
        # Optimized plan has 2 "+- " lines
        assert result.plan_depth == 2

    def test_multi_section_stats_from_optimized_only(self) -> None:
        """Statistics should only come from the Optimized Logical Plan section."""
        result = parse_explain_cost(MULTI_SECTION_PLAN)
        expected = int(23.5 * 1024**2)
        assert result.total_size_bytes == expected


class TestParseExplainCostCTE:
    """Tests for CTE node handling."""

    def test_cte_plan_parses_without_error(self) -> None:
        """Plan with WithCTE/CTERelationDef/CTERelationRef should parse successfully."""
        result = parse_explain_cost(CTE_PLAN)
        assert result is not None

    def test_cte_operations_include_cte_nodes(self) -> None:
        """CTERelationRef, WithCTE, CTERelationDef should appear in operations."""
        result = parse_explain_cost(CTE_PLAN)
        op_names = [op.name for op in result.operations]
        assert "CTERelationRef" in op_names
        assert "WithCTE" in op_names
        assert "CTERelationDef" in op_names

    def test_cte_plan_join_types_empty(self) -> None:
        """CTE plan with no joins should have empty join_types."""
        result = parse_explain_cost(CTE_PLAN)
        assert result.join_types == []


class TestParseExplainCostOperations:
    """Tests for OperationInfo extraction."""

    def test_aggregate_in_operations(self) -> None:
        """Aggregate operators should appear in operations list."""
        result = parse_explain_cost(SIMPLE_SCAN_NO_ROWCOUNT)
        op_names = [op.name for op in result.operations]
        assert "Aggregate" in op_names

    def test_relation_in_operations(self) -> None:
        """Relation (scan) operators should appear in operations list."""
        result = parse_explain_cost(SIMPLE_SCAN_NO_ROWCOUNT)
        op_names = [op.name for op in result.operations]
        assert "Relation" in op_names

    def test_broadcast_join_in_operations(self) -> None:
        """BroadcastHashJoin should appear in operations with correct kind and weight."""
        result = parse_explain_cost(BROADCAST_JOIN_PLAN)
        join_ops = [op for op in result.operations if op.name == "BroadcastHashJoin"]
        assert len(join_ops) == 1
        assert join_ops[0].kind == "join"
        assert join_ops[0].weight == 5

    def test_exchange_in_operations_with_correct_weight(self) -> None:
        """Exchange operators should appear in operations with kind='shuffle', weight=8."""
        result = parse_explain_cost(SORT_MERGE_JOIN_PLAN)
        exchange_ops = [op for op in result.operations if op.name == "Exchange"]
        assert len(exchange_ops) > 0
        for op in exchange_ops:
            assert op.kind == "shuffle"
            assert op.weight == 8

    def test_raw_plan_stored_verbatim(self) -> None:
        """raw_plan field should store the original input text."""
        result = parse_explain_cost(SIMPLE_SCAN_NO_ROWCOUNT)
        assert result.raw_plan == SIMPLE_SCAN_NO_ROWCOUNT
