# Cinder Pattern Language (CPL)

Cinder is a progressive DSL (Domain Specific Language) for writing code analysis rules in `burnt`. It provides a human-friendly way to express tree-sitter patterns for detecting code anti-patterns.

## Overview

Cinder consists of:
1. **Unified Rule Files**: Each rule is defined in a single `.toml` file
2. **CPL Patterns**: Write actual code snippets with `$METAVARIABLES` 
3. **Auto-generated Tests**: Inline test cases that validate rule behavior

## Rule File Structure

```toml
# BP008 - collect() without limit()
# Category: BestPractice
# Severity: error
# Language: python
# Tier: 1

[rule]
id = "collect_without_limit"
code = "BP008"
severity = "error"
language = "python"
description = "collect() without limit() can OOM the driver"
suggestion = "Add .limit(n).collect() or use .take(n)"
category = "BestPractice"
tier = 1

[query]
# CPL pattern - write Python code with $METAVARIABLES
detect = "$DF.collect()"
exclude = "$DF.limit($N).collect()"

[tests]
pass = [
    "df.limit(10).collect()",
    "df.take(10)"
]
fail = [
    "df.collect()",
    "spark.table('orders').filter(x == 1).collect()"
]
```

## Directory Structure

```
rules/
├── tier1/
│   ├── python/
│   │   ├── BP008_collect_without_limit.toml
│   │   ├── BP010_python_udf.toml
│   │   └── ...
│   ├── sql/
│   │   ├── BP009_select_star.toml
│   │   └── ...
│   └── dlt/
│       └── ...
├── tier2/
│   └── python/
│       └── ...
└── tier3/
    └── python/
        └── ...
```

## CPL Syntax

### Metavariables

Use `$VAR` to match any expression:
```python
$DF.collect()    # Matches: df.collect(), spark.table('x').collect(), etc.
```

### Method Chains

The CPL compiler transforms code-like patterns into tree-sitter S-expressions:
```python
$DF.limit($N).collect()
```

### String Literals

Match specific string values:
```python
$DF.repartition($:1)    # $:1 means "match literal integer 1"
```

## Building Rules

Rules are compiled at build time by `build.rs`. The process:

1. Scans `rules/` directory for `.toml` files
2. Parses rule metadata and CPL patterns
3. Compiles CPL patterns to tree-sitter S-expressions
4. Generates `registry.rs` with compiled rules
5. Auto-generates test functions from inline `[tests]` blocks

## Adding a New Rule

1. Create a new `.toml` file in the appropriate `rules/tier{N}/{language}/` directory
2. Fill in the `[rule]` section with metadata
3. Write tree-sitter patterns in `[query]`
4. Add test cases in `[tests]`
5. Run `cargo build` to compile the new rule

## Example: BP008 Pattern

The pattern for detecting `collect()` without `limit()`:

```toml
[query]
detect = """
(call
  function: (attribute
    object: (_)
    attribute: (identifier) @method_name
  )
  (#eq? @method_name "collect")
)
"""
exclude = """
(call
  function: (attribute
    object: (call
      function: (attribute
        object: (_)
        attribute: (identifier) @limit_method
      )
      (#eq? @limit_method "limit")
    )
    attribute: (identifier) @collect_method
  )
  (#eq? @collect_method "collect")
)
"""
```

## Future Work

- Full CPL compiler implementation for user-friendly pattern syntax
- Interactive rule testing tool
- Rule validation at build time
