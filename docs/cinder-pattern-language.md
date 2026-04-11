# Cinder Pattern Language (CPL) v2.0

Cinder Pattern Language (CPL) provides human-friendly syntax for writing tree-sitter patterns to detect code anti-patterns in burnt.

## Rule File Structure

```toml
# BP008 - collect() without limit()
# Category: Performance
# Severity: error
# Language: python

[rule]
id = "collect_without_limit"
code = "BP008"
severity = "error"
language = "python"
description = "collect() without limit() can OOM the driver"
suggestion = "Add .limit(n).collect() or use .take(n)"
category = "Performance"

[query]
# CPL patterns - matches df.collect()
# Use array format for multiple patterns
cpl_detect = [
    """
    $df.collect()
    $method == "collect"
    """,
]

# CPL exclude - df.limit().collect() is OK
cpl_exclude = [
    """
    $df.limit().collect()
    """,
]

[tests]
pass = [
    "df.limit(100).collect()",
    "df.take(10)",
]
fail = [
    "df.collect()",
]
```

## CPL Syntax

### Captures

Use `$variable_name` to capture a value:

```python
$df.collect()           # Captures df as @df, method as @method
$df.limit($n)          # Captures df as @df, n as @n
```

### Predicates

Use predicates to add constraints:

```python
$method == "collect"    # Exact match - emits (#eq? @method "collect")
$method != "forbidden"  # Negation - emits (#not-eq? @method "forbidden")
$cmd =~ "^(run|sql)$"  # Regex match - emits (#match? @cmd "^(run|sql)$")
```

### Numeric Predicates

Numeric comparisons for detecting values outside acceptable ranges:

```python
$n > 0                  # Greater than - emits (#gt? @n "0")
$n >= 10                # Greater than or equal - emits (#gte? @n "10")
$n < 100                # Less than - emits (#lt? @n "100")
$n <= 1000              # Less than or equal - emits (#lte? @n "1000")
```

### Method Chains

The CPL compiler automatically captures method names in chains:

```python
$df.limit().collect()   # @method = "collect", @parent_method = "limit"
```

### Wildcards

Use `_` for anonymous nodes (wildcards):

```python
$df.repartition($n)    # Captures the DataFrame and argument, ignores method details
```

## Multiple Patterns

Use arrays to specify multiple detection patterns:

```toml
[query]
cpl_detect = [
    """
    $df.collect()
    $method == "collect"
    """,
    """
    $df.take($n)
    $method == "take"
    """,
    """
    $df.first()
    $method == "first"
    """,
]
```

Use arrays for multiple exclusion patterns:

```toml
cpl_exclude = [
    """
    $df.limit().collect()
    """,
    """
    $df.take(1)
    """,
]
```

## Context-Based Rules

Some rules require deeper analysis beyond simple pattern matching. These are implemented as context-based rules in `src/burnt-engine/src/rules/context.rs`.

### How Context Rules Work

Context rules have `cpl_detect` patterns that serve as markers for the rule system, but the actual detection logic is implemented in Rust functions that analyze the source code semantically.

Example context-based rules:
- **BP001**: Cell has no comments - analyzes cell structure
- **BP002**: Long lines (>120 chars) - analyzes line lengths
- **BP003/BP004**: Magic commands - detects `# MAGIC` comments
- **BP020**: withColumn in loop - detects loop context
- **BNT-I01**: Star imports - detects `from pyspark.sql.functions import *`

### Context Rule Implementation

Context rules are implemented in `analyze_context_for_rule()` function:

```rust
pub fn analyze_context_for_rule(
    rule_code: &str,
    source: &str,
    context_config: &ContextConfig,
) -> Vec<Finding> {
    match rule_code {
        "BP001" => check_cell_no_comment(source),
        "BP002" => check_long_line(source),
        "BP003" => check_magic_in_plain(source),
        "BP004" => check_deprecated_magic(source),
        // ... more rules
        _ => vec![],
    }
}
```

## Compilation

CPL patterns are compiled to tree-sitter S-expressions at build time:

```python
# CPL input
$df.collect()
$method == "collect"

# Compiled sexp
(module
  (expression_statement
    (call
      (attribute
        (identifier) @df
        (identifier) @method)
      (argument_list)))
  (#eq? @method "collect"))
```

## Directory Structure

```
rules/
├── Performance/
│   ├── python/
│   │   ├── BP008_collect_without_limit.toml
│   │   └── ...
│   └── sql/
│       └── ...
├── SQL/
│   └── ...
├── SDP/
│   └── ...
├── Style/
│   └── ...
├── Notebook/
│   └── ...
└── Delta/
    └── ...
```

## Adding a New Rule

### Pattern-Based Rule (Simple)

1. Create a `.toml` file in the appropriate category directory
2. Fill in the `[rule]` section with metadata
3. Write CPL patterns in `[query]` using `cpl_detect` array
4. Add test cases in `[tests]`
5. Run `cargo build` to compile

### Context-Based Rule (Complex)

For rules requiring semantic analysis:

1. Create the `.toml` file with a marker pattern in `cpl_detect`
2. Implement the detection logic in `context.rs`:
   - Add a function `check_<rule_name>(source: &str) -> Vec<Finding>`
   - Add the rule code to `analyze_context_for_rule()` match statement
3. Write tests in the `#[cfg(test)]` module of `context.rs`

## Pattern Best Practices

1. **Be specific**: Use method predicates to avoid false positives
   ```python
   # Good: specific method
   $df.collect()
   $method == "collect"

   # Less specific: matches any method named collect
   $df.collect()
   ```

2. **Use wildcards**: Don't capture what you don't need
   ```python
   # Good: only capture what matters
   $df.collect()

   # Unnecessary complexity
   $df.collect() $x == "y"
   ```

3. **Chain context**: Leverage method chain predicates for better matching
   ```python
   # Detects collect after any operation
   $df.limit().collect()

   # Detects collect only after filter
   $df.filter($cond).collect()
   ```

## Common Patterns

### DataFrame Actions
```python
$df.collect()
$df.count()
$df.take($n)
$df.first()
$df.show()
```

### DataFrame Transformations
```python
$df.filter($cond)
$df.withColumn($name, $expr)
$df.select($cols)
$df.join($other)
```

### Caching Operations
```python
$df.cache()
$df.persist()
$df.unpersist()
```

### SQL Statements
```python
SELECT * FROM $tbl
SELECT $cols FROM $tbl WHERE $cond
```

## Implementation

The CPL system consists of:

| Component | Location | Purpose |
|-----------|----------|---------|
| CPL Compiler | `src/rules/cinder/compiler.rs` | Transforms CPL to tree-sitter sexp |
| Context Analyzer | `src/rules/context.rs` | Semantic analysis for complex rules |
| Dataflow Analyzer | `src/rules/dataflow.rs` | Dataflow tracking for cache/performance |
| Rule Pipeline | `src/rules/mod.rs` | Orchestrates rule execution |

## Migration from v1

### Removed: Tier System
The `tier` field is no longer used. All rules are treated equally.

### Changed: Multiple Patterns
Old format (single pattern):
```toml
cpl_detect = """
$df.collect()
"""
cpl_detect_alt = """
$df.take($n)
"""
```

New format (array):
```toml
cpl_detect = [
    """
    $df.collect()
    """,
    """
    $df.take($n)
    """,
]
```

### Added: Numeric Predicates
```python
$n > 0       # New: greater than
$n >= 0      # New: greater than or equal
$n < 100     # New: less than
$n <= 1000   # New: less than or equal
```

### Added: Context-Based Rules
Rules requiring semantic analysis now have dedicated functions in `context.rs`:
- `check_cell_no_comment()`
- `check_long_line()`
- `check_magic_in_plain()`
- `check_deprecated_magic()`
- And more...
