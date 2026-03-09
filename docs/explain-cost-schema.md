# EXPLAIN COST Output Format — Parsing Specification

> This document is the authoritative reference for implementing the `ExplainPlan` parser
> in `src/burnt/parsers/explain.py`. An executor can implement the parser by reading
> only this file — no Databricks documentation is needed.

---

## Section 1: Example EXPLAIN COST Outputs

All examples below are realistic synthetic outputs produced by
`spark.sql("EXPLAIN COST <query>")` on Databricks Runtime 13.x–16.x. The output is a
plain-text string (not JSON). Every parser input begins with the literal header line
`== Optimized Logical Plan ==`.

---

### Example 1: Simple table scan with aggregation

SQL:
```sql
SELECT sku_name, SUM(price) AS total_price
FROM catalog.sales.line_items
WHERE region = 'EMEA'
GROUP BY sku_name
```

EXPLAIN COST output:
```
== Optimized Logical Plan ==
Aggregate [sku_name#1], [sku_name#1, sum(price#2) AS total_price#3]   Statistics(sizeInBytes=8.0 B)
+- Filter (isnotnull(region#4) AND (region#4 = EMEA))                 Statistics(sizeInBytes=13.5 MiB, rowCount=191.9K)
   +- Relation catalog.sales.line_items [sku_name#1,price#2,region#4] parquet   Statistics(sizeInBytes=23.5 MiB, rowCount=1.5M)
```

Key observations:
- The root node (Aggregate) shows only `sizeInBytes` — this is the output size estimate.
- Each child line is indented with spaces and prefixed by `+- `.
- `rowCount` appears on leaf and intermediate nodes once table statistics exist.
- The Filter reduces rowCount from 1.5M to 191.9K (predicate selectivity applied).

---

### Example 2: Two-table join (BroadcastHashJoin)

SQL:
```sql
SELECT o.order_id, c.customer_name, o.amount
FROM catalog.sales.orders o
JOIN catalog.sales.customers c ON o.customer_id = c.customer_id
WHERE o.status = 'SHIPPED'
```

EXPLAIN COST output:
```
== Optimized Logical Plan ==
Project [order_id#10, customer_name#21, amount#11]   Statistics(sizeInBytes=1.2 MiB, rowCount=42.0K)
+- BroadcastHashJoin [customer_id#12], [customer_id#22], Inner, BuildRight   Statistics(sizeInBytes=2.1 MiB, rowCount=42.0K)
   :- Filter (isnotnull(status#13) AND (status#13 = SHIPPED))               Statistics(sizeInBytes=18.4 MiB, rowCount=420.0K)
   :  +- Relation catalog.sales.orders [order_id#10,customer_id#12,amount#11,status#13] parquet   Statistics(sizeInBytes=40.2 MiB, rowCount=1.0M)
   +- Relation catalog.sales.customers [customer_id#22,customer_name#21] parquet   Statistics(sizeInBytes=512.0 KiB, rowCount=50.0K)
```

Key observations:
- `BroadcastHashJoin` appears as the node name on the join line.
- `:- ` (colon-hyphen) denotes the left child; `+- ` denotes the right child of a join.
- Both `:- ` and `+- ` lines contain child nodes; both count as plan depth indicators.
- `BuildRight` indicates the smaller (broadcast) side — customers table here.
- No `Exchange` operator: broadcast joins do NOT shuffle data.

---

### Example 3: Large join (SortMergeJoin) with multiple shuffles

SQL:
```sql
SELECT f.fact_id, d1.dim_value AS dim_a, d2.dim_value AS dim_b, f.metric
FROM catalog.dw.fact_table f
JOIN catalog.dw.dimension_a d1 ON f.dim_a_key = d1.key
JOIN catalog.dw.dimension_b d2 ON f.dim_b_key = d2.key
ORDER BY f.fact_id
```

EXPLAIN COST output:
```
== Optimized Logical Plan ==
Sort [fact_id#30 ASC NULLS FIRST]   Statistics(sizeInBytes=28.6 GiB, rowCount=102.4M)
+- SortMergeJoin [dim_b_key#32], [key#51], Inner   Statistics(sizeInBytes=28.6 GiB, rowCount=102.4M)
   :- Exchange hashpartitioning(dim_b_key#32, 200)   Statistics(sizeInBytes=22.1 GiB, rowCount=102.4M)
   :  +- SortMergeJoin [dim_a_key#31], [key#41], Inner   Statistics(sizeInBytes=22.1 GiB, rowCount=102.4M)
   :     :- Exchange hashpartitioning(dim_a_key#31, 200)   Statistics(sizeInBytes=18.8 GiB, rowCount=256.0M)
   :     :  +- Relation catalog.dw.fact_table [fact_id#30,dim_a_key#31,dim_b_key#32,metric#33] parquet   Statistics(sizeInBytes=134.6 GiB, rowCount=2.88B)
   :     +- Exchange hashpartitioning(key#41, 200)   Statistics(sizeInBytes=3.2 GiB, rowCount=40.0M)
   :        +- Relation catalog.dw.dimension_a [key#41,dim_value#42] parquet   Statistics(sizeInBytes=4.8 GiB, rowCount=40.0M)
   +- Exchange hashpartitioning(key#51, 200)   Statistics(sizeInBytes=800.0 MiB, rowCount=8.0M)
      +- Relation catalog.dw.dimension_b [key#51,dim_value#52] parquet   Statistics(sizeInBytes=1.2 GiB, rowCount=8.0M)
```

Key observations:
- `SortMergeJoin` requires both sides to be shuffled — `Exchange hashpartitioning(...)` appears before each join input.
- Three shuffle `Exchange` operators appear plus one `Sort` — shuffle_count = 4.
- `rowCount=2.88B` uses the `B` suffix for billions (not bytes — context determines meaning).
- `sizeInBytes=134.6 GiB` is the largest single-operator size; `total_size_bytes` should capture the maximum across all nodes.
- Plan depth (lines with `+- ` or `:- `) = 9 in this example.

---

### Example 4: Subquery and CTE

SQL:
```sql
WITH recent_orders AS (
  SELECT order_id, customer_id, amount
  FROM catalog.sales.orders
  WHERE order_date >= '2025-01-01'
)
SELECT customer_id, SUM(amount) AS total
FROM recent_orders
WHERE amount > (SELECT AVG(amount) FROM catalog.sales.orders)
GROUP BY customer_id
```

EXPLAIN COST output:
```
== Optimized Logical Plan ==
Aggregate [customer_id#60], [customer_id#60, sum(amount#61) AS total#62]   Statistics(sizeInBytes=16.0 B)
+- Filter (amount#61 > scalar-subquery#63 [])   Statistics(sizeInBytes=5.8 MiB, rowCount=86.0K)
   :  +- Aggregate [], [avg(amount#64) AS avg(amount)#65]   Statistics(sizeInBytes=8.0 B)
   :     +- Relation catalog.sales.orders [amount#64] parquet   Statistics(sizeInBytes=40.2 MiB, rowCount=1.0M)
   +- CTERelationRef recent_orders, [order_id#66,customer_id#60,amount#61]   Statistics(sizeInBytes=12.2 MiB, rowCount=180.0K)
      +- WithCTE   Statistics(sizeInBytes=12.2 MiB, rowCount=180.0K)
         +- CTERelationDef recent_orders#67   Statistics(sizeInBytes=12.2 MiB, rowCount=180.0K)
            +- Filter (isnotnull(order_date#68) AND (order_date#68 >= 2025-01-01))   Statistics(sizeInBytes=12.2 MiB, rowCount=180.0K)
               +- Relation catalog.sales.orders [order_id#66,customer_id#60,amount#61,order_date#68] parquet   Statistics(sizeInBytes=40.2 MiB, rowCount=1.0M)
```

Key observations:
- Subqueries appear inline as child nodes under the `Filter` that references them.
- CTE materialisation uses three node types: `WithCTE`, `CTERelationDef <name>#id`, `CTERelationRef <name>`.
- All three CTE node types should be counted as scan-equivalent operators (kind="scan", weight=1).
- The scalar subquery block is indented under the filter using `:  +-` style.
- `estimated_rows` is taken from the **root node** (Aggregate here) — if the root has no rowCount, the value is `None`.

---

### Example 5: Query with no statistics (ANALYZE TABLE not run)

SQL:
```sql
SELECT product_id, COUNT(*) AS cnt
FROM catalog.raw.events
GROUP BY product_id
```

EXPLAIN COST output:
```
== Optimized Logical Plan ==
Aggregate [product_id#70], [product_id#70, count(1) AS cnt#71]   Statistics(sizeInBytes=8.0 B)
+- Relation catalog.raw.events [product_id#70] parquet   Statistics(sizeInBytes=8.0 B)
```

Key observations:
- When `ANALYZE TABLE` has NOT been run, Spark cannot estimate row counts.
- All `Statistics(...)` blocks contain **only** `sizeInBytes` — no `rowCount` field.
- `sizeInBytes=8.0 B` on a leaf Relation is Spark's default placeholder when statistics are completely missing (not a real file size).
- `stats_complete` should be `False`; confidence should be `"low"`.
- The absence of rowCount is the distinguishing signal — not the small byte value.

---

## Section 2: Regex Patterns

All patterns should be compiled with `re.MULTILINE`. Column reference suffixes like
`#10` are Spark's internal attribute IDs and should be ignored during parsing.

### 2.1 Detect the required header

```python
HEADER_PATTERN = re.compile(r"^== Optimized Logical Plan ==$", re.MULTILINE)
```

Use `HEADER_PATTERN.search(text)` to verify the input is valid EXPLAIN COST output.
If not found, raise `ParseError("Missing '== Optimized Logical Plan ==' header")`.

### 2.2 Match a Statistics block on any line

```python
STATS_PATTERN = re.compile(
    r"Statistics\(sizeInBytes=(?P<size_val>[\d.]+)\s*(?P<size_unit>[KMGTiB]+)"
    r"(?:,\s*rowCount=(?P<row_val>[\d.E+]+)\s*(?P<row_unit>[KMB]?))?\)",
    re.IGNORECASE,
)
```

This pattern matches both forms:
- `Statistics(sizeInBytes=23.5 MiB, rowCount=1.5M)` — complete stats
- `Statistics(sizeInBytes=8.0 B)` — size only, no rowCount

Named capture groups:
- `size_val` — numeric value as string, e.g. `"23.5"`
- `size_unit` — unit string, e.g. `"MiB"`, `"GiB"`, `"B"`, `"KiB"`, `"TiB"`
- `row_val` — numeric value or scientific notation, e.g. `"1.5"`, `"2.88"`, `"1.62E+6"` (optional)
- `row_unit` — optional suffix `K`, `M`, or `B`; empty string means plain integer

### 2.3 Detect join type on a line

```python
JOIN_PATTERN = re.compile(
    r"\b(BroadcastHashJoin|SortMergeJoin|ShuffledHashJoin|CartesianProduct)\b"
)
```

Apply `JOIN_PATTERN.findall(line)` to each line. Collect all unique matches across the
entire plan into `join_types: list[str]`.

### 2.4 Detect shuffle / sort operators

```python
SHUFFLE_PATTERN = re.compile(
    r"^\s*(?::[-\s]+|[+][-\s]+)?"  # optional indentation markers
    r"(Exchange\s+(?:hashpartitioning|SinglePartition|RoundRobinPartitioning)"
    r"|Sort\s*\[)",
    re.MULTILINE,
)
```

Count all matches across the plan as `shuffle_count`. Both `Exchange` variants and
`Sort [...]` lines each add 1 to the count.

Simplified alternative — count lines that start (after stripping indentation) with
`Exchange` or `Sort [`:

```python
SHUFFLE_LINE_PATTERN = re.compile(
    r"^\s*(?::[- ]+|[+]- )?(Exchange |Sort \[)",
    re.MULTILINE,
)
```

### 2.5 Count plan depth

```python
DEPTH_PATTERN = re.compile(r"\+- ", re.MULTILINE)
```

`plan_depth = len(DEPTH_PATTERN.findall(text))`.

Note: Lines using `:- ` (left child of a binary node) are NOT counted separately —
only `+- ` is used as the depth metric. This avoids double-counting binary splits.

---

## Section 3: Unit Normalization

### 3.1 Size units → bytes multiplier

| Unit string | Multiplier        | Notes                        |
|-------------|-------------------|------------------------------|
| `B`         | 1                 | bytes                        |
| `KiB`       | 1 024             | kibibytes                    |
| `MiB`       | 1 048 576         | mebibytes (2^20)             |
| `GiB`       | 1 073 741 824     | gibibytes (2^30)             |
| `TiB`       | 1 099 511 627 776 | tebibytes (2^40)             |

Conversion function:

```python
SIZE_MULTIPLIERS: dict[str, int] = {
    "B":   1,
    "KiB": 1024,
    "MiB": 1024 ** 2,
    "GiB": 1024 ** 3,
    "TiB": 1024 ** 4,
}

def size_to_bytes(val: str, unit: str) -> int:
    multiplier = SIZE_MULTIPLIERS[unit]
    return int(float(val) * multiplier)
```

Important: Databricks always uses binary prefixes (KiB, MiB, GiB, TiB), never SI
prefixes (KB, MB, GB). The unit string from the regex will always match one of the
five entries in the table above.

### 3.2 Row count units → multiplier

| Unit suffix | Multiplier  | Example match        |
|-------------|-------------|----------------------|
| (empty)     | 1           | `rowCount=50000`     |
| `K`         | 1 000       | `rowCount=191.9K`    |
| `M`         | 1 000 000   | `rowCount=1.5M`      |
| `B`         | 1 000 000 000 | `rowCount=2.88B`   |

Note the ambiguity: `B` in `sizeInBytes` context means bytes, but `B` in `rowCount`
context means billions. The regex captures them separately (`size_unit` vs `row_unit`),
so disambiguation is automatic.

Row count values may also appear in scientific notation: `rowCount=1.62E+6`. Parse
these with `float(val)` before multiplying — Python's `float()` handles `E` notation
natively. Then cast to `int` after multiplication.

Conversion function:

```python
ROW_MULTIPLIERS: dict[str, int] = {
    "":  1,
    "K": 1_000,
    "M": 1_000_000,
    "B": 1_000_000_000,
}

def rows_to_int(val: str, unit: str) -> int:
    return int(float(val) * ROW_MULTIPLIERS[unit])
```

---

## Section 4: Field Mapping to OperationInfo

Each detected operator line maps to one `OperationInfo(name, kind, weight)` instance.
The `name` field is the exact operator string found in the plan. The `kind` and
`weight` fields follow the table below.

| Plan node string         | `name` value         | `kind`      | `weight` |
|--------------------------|----------------------|-------------|----------|
| `BroadcastHashJoin`      | `BroadcastHashJoin`  | `"join"`    | 5        |
| `SortMergeJoin`          | `SortMergeJoin`      | `"join"`    | 15       |
| `ShuffledHashJoin`       | `ShuffledHashJoin`   | `"join"`    | 10       |
| `CartesianProduct`       | `CartesianProduct`   | `"join"`    | 50       |
| `Exchange ...`           | `Exchange`           | `"shuffle"` | 8        |
| `Sort [...]`             | `Sort`               | `"sort"`    | 3        |
| `Aggregate [...]`        | `Aggregate`          | `"aggregate"` | 5      |
| `Relation ...`           | `Relation`           | `"scan"`    | 1        |
| `CTERelationRef ...`     | `CTERelationRef`     | `"scan"`    | 1        |
| `CTERelationDef ...`     | `CTERelationDef`     | `"scan"`    | 1        |
| `WithCTE`                | `WithCTE`            | `"scan"`    | 1        |

Notes on operator detection:

- A line is classified by matching the **first word** after stripping indentation markers
  (`+- `, `:- `, `   `).
- `Exchange` must be followed by a space and one of `hashpartitioning`,
  `SinglePartition`, or `RoundRobinPartitioning` to be a shuffle. A bare `Exchange`
  without a recognized partitioning keyword should be treated as weight=8 anyway.
- `Sort` is only a sort operator when followed by `[` (sort key list). `SortMergeJoin`
  contains the word "Sort" but is detected by the full `JOIN_PATTERN` first — do not
  double-count.
- `Filter`, `Project`, `Window`, and similar pass-through operators are NOT added to
  `OperationInfo` — they add no significant compute weight.
- The `complexity_score` on `QueryProfile` is the sum of all `weight` values across the
  produced `OperationInfo` list, matching the pattern already used in `sql.py`.

---

## Section 5: Confidence Scoring

Confidence reflects how much trust to place in the EXPLAIN COST estimates. It maps
directly to `CostEstimate.confidence` (already a `Literal["low", "medium", "high"]`
field in `models.py`).

### Rules (applied in priority order)

| Condition                                             | Confidence  |
|-------------------------------------------------------|-------------|
| `stats_complete=True` (every operator has `rowCount`) | `"high"`    |
| `stats_complete=False` but at least one `sizeInBytes` is > 8 bytes | `"medium"` |
| No statistics at all, OR all `sizeInBytes` == 8 bytes (placeholder) | `"low"` |

### Definition of `stats_complete`

Set `stats_complete = True` if and only if **every** `Statistics(...)` block in the
plan text contains a `rowCount` field. A single `Statistics(sizeInBytes=X)` without
`rowCount` sets `stats_complete = False`.

### Placeholder detection

Spark emits `Statistics(sizeInBytes=8.0 B)` as a default placeholder when it has no
real table metadata. Detecting this sentinel: both value `8.0` AND unit `B`. However,
a table that genuinely holds a few rows may also have `sizeInBytes=8.0 B`. Use
`stats_complete` as the primary signal, not the byte value, to avoid false classification.

The 8-byte placeholder rule is a secondary heuristic: if `stats_complete=False` AND
all `sizeInBytes` values after normalization are `<= 8`, emit `confidence="low"`.
If even one `sizeInBytes > 8 bytes`, emit `confidence="medium"`.

---

## Section 6: Edge Cases

### 6.1 Empty or whitespace-only input

```
raise ParseError("Empty EXPLAIN COST output")
```

Check: `if not text or not text.strip()`.

### 6.2 Missing header

If `HEADER_PATTERN.search(text)` returns `None`, raise:

```
raise ParseError("Missing '== Optimized Logical Plan ==' header")
```

Do not attempt to parse the rest of the text. The caller may be passing raw SQL or
an EXPLAIN output from a different mode (SIMPLE, EXTENDED without COST, etc.).

### 6.3 CTE nodes

Three CTE-related node names appear in plans:

- `WithCTE` — marks the CTE block container
- `CTERelationDef <name>#<id>` — the definition (body) of a CTE
- `CTERelationRef <name>, [...]` — a usage site of the CTE

All three map to `kind="scan"`, `weight=1`. They are detected by checking whether the
first token of the stripped line matches `WithCTE`, `CTERelationDef`, or `CTERelationRef`.
CTE nodes have their own `Statistics(...)` blocks and should be counted in
`stats_complete` evaluation like any other operator.

### 6.4 Subqueries

Scalar and correlated subqueries appear inline under the `Filter` node that references
them, indented with `:-` markers. There is no special node type — they are parsed
as regular child nodes. Do not add a separate `OperationInfo` entry for the `Filter`
wrapper itself; the subquery's own operators (Aggregate, Relation, etc.) are added
individually. This matches how `sql.py` currently detects `exp.Subquery` — the
subquery's contents are traversed normally.

### 6.5 Very large rowCount values (> 1 billion)

Row counts using the `B` suffix (billions) convert to values > 1 000 000 000.
These are valid and should NOT be capped. Store them as Python `int` (arbitrary
precision). Example: `rowCount=2.88B` → `2_880_000_000`.

Scientific notation: `rowCount=1.62E+6` → `float("1.62E+6") * 1 = 1_620_000` (unit
is empty string here, not `M` — scientific notation is used without a suffix).
Parse: `int(float("1.62E+6"))` → `1620000`.

### 6.6 EXPLAIN without COST keyword (no Statistics blocks)

If `HEADER_PATTERN` matches but zero `STATS_PATTERN` matches are found, the input
is a plan from `EXPLAIN SIMPLE` or `EXPLAIN EXTENDED` (without COST). In this case:

- `total_size_bytes = 0`
- `estimated_rows = None`
- `stats_complete = False`
- `confidence = "low"`
- Still parse `join_types`, `shuffle_count`, and `plan_depth` from operator names.

### 6.7 Physical plan sections

Some EXPLAIN modes emit multiple sections:
```
== Parsed Logical Plan ==
...
== Analyzed Logical Plan ==
...
== Optimized Logical Plan ==
...
== Physical Plan ==
...
```

Parse ONLY the text between `== Optimized Logical Plan ==` and the next `==` header
(or end of string). The physical plan section contains different notation and should
be ignored to avoid double-counting operators.

Extraction:

```python
import re

def extract_optimized_section(text: str) -> str:
    match = re.search(
        r"== Optimized Logical Plan ==\n(.*?)(?=\n==|\Z)",
        text,
        re.DOTALL,
    )
    if not match:
        raise ParseError("Missing '== Optimized Logical Plan ==' header")
    return match.group(1)
```

---

## Section 7: ExplainPlan Pydantic Model Specification

This model should be defined in `src/burnt/core/models.py` alongside the existing
models, or in a new file `src/burnt/parsers/explain.py` if preferred.

```python
from __future__ import annotations
from pydantic import BaseModel, field_validator


class ExplainPlan(BaseModel):
    """Parsed representation of a Databricks EXPLAIN COST output."""

    total_size_bytes: int
    # Max sizeInBytes across ALL operators in the plan, converted to bytes.
    # Represents the largest data movement seen anywhere in the query.
    # Use max (not sum) to avoid double-counting pipeline-fused stages.

    estimated_rows: int | None
    # rowCount from the ROOT operator (top of the plan tree — first Statistics block).
    # None when root operator has no rowCount (stats not collected).

    join_types: list[str]
    # All distinct join operator strings found in the plan.
    # Possible values: "BroadcastHashJoin", "SortMergeJoin",
    #                  "ShuffledHashJoin", "CartesianProduct"
    # Order: deduplicated, preserving first-seen order.

    shuffle_count: int
    # Number of Exchange + Sort operator lines in the plan.
    # Each Exchange line counts as 1. Each Sort [ line counts as 1.
    # SortMergeJoin itself does NOT count — only its child Exchange/Sort nodes.

    plan_depth: int
    # Count of lines containing "+- " in the optimized plan section.
    # Proxy for operator tree height / query complexity.

    stats_complete: bool
    # True iff every Statistics(...) block in the plan contains a rowCount field.
    # False if ANY Statistics block is missing rowCount.

    raw_plan: str
    # The original unparsed EXPLAIN COST text, stored verbatim for debugging.

    operations: list[OperationInfo] = []
    # One OperationInfo per detected operator (join, shuffle, sort, aggregate, scan).
    # Used to populate QueryProfile.operations downstream.
```

### Relationship to existing models

`ExplainPlan` feeds into `QueryProfile` (defined in `models.py`) as follows:

```
ExplainPlan.operations  →  QueryProfile.operations
ExplainPlan             →  QueryProfile.complexity_score  (sum of operation weights)
ExplainPlan.stats_complete + sizeInBytes  →  CostEstimate.confidence
ExplainPlan.total_size_bytes  →  CostEstimate.breakdown["scan_bytes"]
ExplainPlan.shuffle_count  →  CostEstimate.breakdown["shuffle_count"]
```

The parser function signature should be:

```python
def parse_explain_cost(text: str) -> ExplainPlan:
    """Parse a Databricks EXPLAIN COST output string into a structured ExplainPlan."""
    ...
```

It should raise `ParseError` (from `src/burnt/core/exceptions.py`) for all
invalid inputs, consistent with the existing `parse_sql()` function in `sql.py`.

---

## Appendix: Quick Reference — Operator Classification Table

| First token (stripped)   | kind        | weight | Count in shuffle_count? |
|--------------------------|-------------|--------|------------------------|
| `BroadcastHashJoin`      | join        | 5      | No                     |
| `SortMergeJoin`          | join        | 15     | No                     |
| `ShuffledHashJoin`       | join        | 10     | No                     |
| `CartesianProduct`       | join        | 50     | No                     |
| `Exchange`               | shuffle     | 8      | Yes                    |
| `Sort`                   | sort        | 3      | Yes                    |
| `Aggregate`              | aggregate   | 5      | No                     |
| `Relation`               | scan        | 1      | No                     |
| `CTERelationRef`         | scan        | 1      | No                     |
| `CTERelationDef`         | scan        | 1      | No                     |
| `WithCTE`                | scan        | 1      | No                     |
| `Filter`                 | (skip)      | —      | No                     |
| `Project`                | (skip)      | —      | No                     |
| `Window`                 | (skip)      | —      | No                     |
| `SubqueryAlias`          | (skip)      | —      | No                     |

Skipped operators contribute nothing to `OperationInfo` but their `Statistics` blocks
ARE included in `stats_complete` evaluation and `total_size_bytes` computation.
