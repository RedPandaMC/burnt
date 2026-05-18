# Graph-DSL Reference

The graph-DSL is an S-expression pattern language for querying the `ResolvedGraph` produced by the Spark/SQL/SDP analysis pipeline. Every rule's `[graph]` block contains a `detect` expression (and an optional `exclude` expression) written in this language.

---

## Grammar

```
pattern    := "(" head props* body* ")"
head       := prefix separator kind
prefix     := "op" | "ast" | "edge" | "overlay" | "fact"
separator  := ":" | "/"        ; op/edge/overlay/fact use ":", ast uses "/"
kind       := IDENT             ; e.g. Read, Call, DataFlow, source
props      := ":" IDENT value   ; property assertion
body       := capture | predicate | pattern
capture    := "@" IDENT
predicate  := "(" "#" IDENT pred-arg* ")"
pred-arg   := value | predicate | pattern
value      := STRING | NUMBER | BOOL | IDENT | capture-ref | size | list
capture-ref := "@" IDENT
size       := NUMBER ("Gi"|"Mi"|"Ki"|"GB"|"MB"|"KB"|"GiB"|"MiB"|"KiB")
list       := "[" value* "]"
```

Strings are delimited by `"..."`. Inside a DSL string, `\\` is a literal backslash and `\"` is a literal double-quote. All other characters are literal. The parser does not support single-quoted strings.

Predicates begin with `#`. Property assertions begin with `:`.

---

## Head Prefixes

### `op:<Kind>`

Selects a graph `Node` by `OperationKind`. The kind is case-sensitive.

| Kind | What it matches |
|------|----------------|
| `op:Read` | `spark.read.*`, `spark.readStream.*`, SQL `SELECT` |
| `op:Transform` | `select`, `filter`, `where`, `withColumn`, `groupBy`, `agg`, `join` (not terminal) |
| `op:Shuffle` | `join`, `groupBy`, `repartition`, `sort`, `orderBy`, `distinct`, SQL `JOIN` / `GROUP BY` / `ORDER BY` |
| `op:Action` | `collect`, `show`, `count`, `take`, `write.save`, trigger-initiating calls |
| `op:Write` | `write.save*`, `write.insertInto`, `saveAsTable`, SQL `INSERT` / `MERGE` / `CREATE TABLE AS` |
| `op:UdfCall` | `udf(...)`, `pandas_udf(...)`, decorated UDF calls |
| `op:Maintenance` | `VACUUM`, `OPTIMIZE`, `ANALYZE`, `CONVERT TO DELTA`, `cache()` / `persist()` |
| `op:Unknown` | Calls the builder could not classify |

```toml
# Fires on any collect() call
detect = "(op:Action (ast/Call :method \"collect\"))"
```

### `ast/<Kind>`

Walks the current node's `AstShape`, matching the named AST variant. Appears nested inside an `op:*` pattern.

| Kind | What it matches |
|------|----------------|
| `ast/Call` | A Python call expression. Properties: `:method`, `:receiver`, `:arg/N`. |
| `ast/Decorator` | A Python `@decorator` on a function or class. Properties: `:name`. |
| `ast/Assignment` | An assignment expression. Properties: `:lhs`. |
| `ast/FunctionDef` | A function or method definition. |
| `ast/SqlStatement` | A top-level SQL statement node. |
| `ast/SqlExpression` | A SQL expression sub-node. |

```toml
# Fires on withColumn() calls
detect = "(op:Transform (ast/Call :method \"withColumn\"))"
```

### `edge:<Kind>`

Matches a graph edge by `edge_type`. Use `:from @a :to @b` to bind endpoints.

```toml
# Finds DataFlow edges between two captured nodes
detect = "(edge:DataFlow :from @src :to @dst)"
```

### `overlay:<Kind>`

Matches a present overlay on the graph (e.g. `Stage`, `PlanSubtree`, `TableSpec`). The `:where` property constrains the overlay to the current node.

```toml
detect = "(overlay:TableSpec :where @n)"
```

### `fact:<Name>`

A one-shot match that fires once per source file. The named fact binds a capture variable to a synthetic value (e.g. the full source text for `fact:source`). Predicates then run against that capture.

| Name | Bound capture type | Description |
|------|-------------------|-------------|
| `fact:source` | `String` — full source text | Fires once; use `#match?` to scan for raw-text patterns |

```toml
# Fires when the source contains a wildcard import
detect = """
(fact:source @src
  (#match? @src "from\\s+\\S+\\s+import\\s+\\*"))
"""
```

---

## Properties

Property assertions narrow the match to nodes with a specific field value. All properties are written as `:key value` pairs inside the pattern.

| Property | Applicable heads | Value type | Semantics |
|----------|-----------------|------------|-----------|
| `:method` | `ast/Call`, `op:*` | String | The method name at the call site |
| `:receiver` | `ast/Call` | String | The receiver object name |
| `:arg/N` | `ast/Call` | String | The N-th positional argument (0-indexed) |
| `:name` | `ast/Decorator`, `ast/Assignment` | String | Decorator name or assigned variable name |
| `:from` | `edge:*` | Capture | Source endpoint of an edge |
| `:to` | `edge:*` | Capture | Destination endpoint of an edge |
| `:where` | `overlay:*` | Capture | Overlay is attached to this node |
| `:namespace` | `op:*` | String | Import namespace (e.g. `"dlt"`, `"spark"`) |
| `:lhs` | `ast/Assignment` | String | Left-hand side variable name |

---

## Captures

`@name` binds the matched node to `name`. The binding is visible to subsequent predicates in the same pattern body and to the `[graph.finding]` template.

```toml
# Capture the matched node and reference it in a predicate
detect = "(op:Transform @n (ast/Call :method \"withColumn\") (#in-loop @n))"
```

A capture appears at most once as a binding (defining occurrence). The same `@name` may appear as a reference in multiple predicates.

---

## Predicates

All predicates are called with `(#name args...)`. Arguments are values, other predicates, or nested patterns.

### Composition

| Predicate | Semantics |
|-----------|-----------|
| `(#and p1 p2 ...)` | True iff all inner predicates are true |
| `(#or p1 p2 ...)` | True iff any inner predicate is true |
| `(#not p)` | True iff `p` is false |
| `(#xor p1 p2)` | True iff exactly one of `p1`, `p2` is true |
| `(#implies p1 p2)` | True iff `p1` is false or `p2` is true |
| `(#when p1 p2)` | Evaluates `p2` only when `p1` is true; short-circuits otherwise |

```toml
# withColumn in a loop, but not inside a UDF
detect = """
(op:Transform @n
  (ast/Call :method "withColumn")
  (#and (#in-loop @n) (#not (#method-chain-contains @n "udf"))))
"""
```

### String / Value

| Predicate | Semantics |
|-----------|-----------|
| `(#eq? @cap "value")` | Capture equals string exactly |
| `(#not-eq? @cap "value")` | Capture does not equal string |
| `(#match? @cap "regex")` | Capture matches regex (Rust `regex` crate syntax) |
| `(#not-match? @cap "regex")` | Capture does not match regex |
| `(#in @cap ["a" "b" ...])` | Capture is one of the listed strings |
| `(#starts-with @cap "prefix")` | Capture starts with prefix |
| `(#ends-with @cap "suffix")` | Capture ends with suffix |
| `(#contains @cap "sub")` | Capture contains substring |
| `(#kind @cap "KindName")` | Capture's node kind equals `KindName` |

`#match?` regex strings use Rust `regex` crate syntax. Inline flags are supported (`(?i)` for case-insensitive, `(?s:...)` to make `.` match newlines). Backreferences are not supported. Double-quotes inside a DSL string must be escaped as `\"`.

```toml
# Case-insensitive match for mergeSchema=true
detect = """
(fact:source @src
  (#match? @src "(?i)mergeSchema[^\\n]*true"))
"""
```

### Numeric Comparison

| Predicate | Semantics |
|-----------|-----------|
| `(#gt @n N)` | Numeric capture > N |
| `(#gte @n N)` | Numeric capture >= N |
| `(#lt @n N)` | Numeric capture < N |
| `(#lte @n N)` | Numeric capture <= N |
| `(#eq @n N)` | Numeric capture == N |

`N` may be a plain number or a size literal (`1Gi`, `500MB`, etc.).

### Quantifiers

| Predicate | Semantics |
|-----------|-----------|
| `(#count pattern :gt N)` | Count of nodes matching `pattern` in the graph > N |
| `(#all pattern pred)` | All nodes matching `pattern` also satisfy `pred` |
| `(#any pattern pred)` | At least one node matching `pattern` satisfies `pred` |
| `(#none pattern pred)` | No node matching `pattern` satisfies `pred` |
| `(#exists pattern)` | At least one node matches `pattern` anywhere in the graph |
| `(#exists-here pattern)` | At least one node matches `pattern` in the current node's scope |
| `(#unique @cap)` | The captured value is unique across all matches so far |

### Traversal

| Predicate | Semantics |
|-----------|-----------|
| `(#descendants @n pattern)` | Collect descendant nodes of `@n` matching `pattern` |
| `(#ancestors @n pattern)` | Collect ancestor nodes of `@n` matching `pattern` |
| `(#siblings @n pattern)` | Collect sibling nodes of `@n` matching `pattern` |
| `(#receiver-of @n)` | True iff `@n` has the role of a receiver in a method chain |
| `(#not-receiver-of @n)` | True iff `@n` is not a receiver |
| `(#callees-of @n)` | Collect all callee nodes transitively reachable from `@n` |

### Extraction (value-producing)

Extraction predicates produce a value rather than a boolean. They are typically passed as an argument to a comparison predicate.

| Predicate | Produces |
|-----------|---------|
| `(#value-of @n)` | The literal value bound to `@n` |
| `(#method-of @n)` | The method name of the call node `@n` |
| `(#method-chain-of @n)` | The full dot-separated method chain as a string |
| `(#line-of @n)` | The 1-based source line of `@n` |
| `(#column-of @n)` | The 1-based source column of `@n` |
| `(#fqn-of @n)` | The fully-qualified table/view name of `@n` |
| `(#overlay-of @n)` | The overlay payload attached to `@n` |
| `(#source-of @n)` | The raw source-code snippet for `@n` |

```toml
# Fires when the method name is "repartition" or "coalesce"
detect = """
(op:Shuffle @n
  (#in (#method-of @n) ["repartition" "coalesce"]))
"""
```

### Mutation (finding-augmenting)

| Predicate | Effect |
|-----------|--------|
| `(#prop "key" "value")` | Attach an arbitrary key-value pair to the emitted finding |
| `(#fires-rule "CODE")` | Cross-reference finding to rule `CODE` (stub — no-op today) |

### Domain-specific

| Predicate | Semantics |
|-----------|-----------|
| `(#in-loop @n)` | True iff `@n` appears inside a `for` or `while` loop body |
| `(#method-chain-contains @n "sub")` | True iff any element of `@n`'s method chain contains `"sub"` |
| `(#kwargs/missing @n ["k1" "k2" ...])` | True iff none of the listed keyword arguments appear at the call site |
| `(#kwargs/has @n ["k1" "k2" ...])` | True iff at least one of the listed keyword arguments appears |
| `(#self-join? @n)` | True iff the `join` call's receiver and first argument are the same variable (detects `df.join(df, ...)`) |
| `(#shares-receiver @a @b)` | True iff both nodes are called on the same root receiver variable |
| `(#not-receiver-of @n)` | True iff `@n` is not in receiver position |
| `(#binds @n @var)` | True iff `@n` writes to variable `@var` (inspects `scope.writes`) |
| `(#reads @n @var)` | True iff `@n` reads variable `@var` (inspects `scope.reads`) |
| `(#has-overlay @n "Kind")` | True iff an overlay of kind `"Kind"` is attached to `@n` |
| `(#has-provenance @n "tag")` | True iff `@n`'s provenance tag matches |
| `(#observed-bytes-gt @n N)` | True iff the observed byte count on `@n` exceeds N bytes |
| `(#table-spec-size-gt @n N)` | True iff the TableSpec overlay on `@n` estimates more than N bytes |

---

## Finding Overrides

A `[graph.finding]` sub-table inside the rule's TOML overrides specific fields of the default finding.

```toml
[graph.finding]
severity   = "error"        # override rule severity for this detection path
confidence = "high"         # "high" | "medium" | "low" | "none"
message    = "Custom message text"
suggestion = "Custom suggestion text"
line       = "@call.line"   # capture-ref or literal line number
```

All fields are optional. When absent:

- `severity` inherits from `[rule].severity`
- `confidence` defaults to `high`
- `message` defaults to `[rule].description`
- `suggestion` defaults to `[rule].suggestion`
- `line` resolves from the anchor node's `line_number`

The `line` field accepts a capture reference (`@name` or `@name.line`) or a plain integer. A capture reference resolves to the captured node's source line.

---

## Rule Anatomy

A complete rule TOML has three sections: `[rule]`, `[tests]`, and `[graph]`.

```toml
[rule]
id          = "collect_without_limit"
code        = "BP008"
severity    = "warning"
language    = "python"
description = "collect() materialises the full dataset to the driver — use limit() first or switch to a streaming sink"
suggestion  = "Add .limit(N) before .collect(), or replace with a write operation"
category    = "Performance"
tags        = ["pyspark", "driver", "oom"]
platform    = "all"

[tests]
pass = [
    "df.limit(100).collect()",
]
fail = [
    "df.collect()",
    "spark.sql('SELECT * FROM t').collect()",
]

[graph]
detect = """
(op:Action @n
  (ast/Call :method "collect"))
"""

[graph.finding]
confidence = "high"
```

`language` is matched case-insensitively. `"notebook"` routes through the Python graph builder. `"all"` matches every language.

The `detect` expression fires if at least one match is found. `exclude` (optional, same syntax) suppresses the finding when it also matches — useful for carving out safe patterns:

```toml
[graph]
detect = """
(op:Read (ast/Call :method "json"))
"""
exclude = """
(op:Read @n
  (ast/Call :method "json")
  (#method-chain-contains @n "schema"))
"""
```

---

## Regex Notes

The DSL string lexer terminates a string on the first unescaped `"`. Patterns that need to match a literal double-quote must use `\"` inside the DSL string (which becomes the two-character sequence `\"` at the regex level, not needed since `"` is not special in regex). In practice, write patterns that avoid matching `"` characters at all: use `[^)]*` to span an argument list, `[^\n]*` to span to end-of-line, or `'[^']*'` to match single-quoted strings.

Character classes containing `"` (e.g. `['"]`) will prematurely terminate the DSL string. Rewrite as two branches in an `#or`:

```toml
detect = """
(fact:source @src
  (#or
    (#match? @src "\\.join\\s*\\(\\s*'[^']*'")
    (#match? @src "\\.join\\s*\\(\\s*[a-zA-Z_]")))
"""
```
