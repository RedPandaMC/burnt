# Task: burnt check — Full Multi-Language Hybrid AST in Rust

---

## Metadata

```yaml
id: s7-03-full-hybrid-ast
status: todo
phase: 7
priority: high
agent: ~
blocked_by: [s7-01-notebook-ingestion, s7-02-semantic-model]
created_by: planner
```

---

## Context

### Goal

Build `HybridAST` and `HybridChecker` entirely in Rust. Every SQL context that can appear in a Databricks codebase — `spark.sql()`, `selectExpr()`, string-form `filter()`, `expr()`, SQL cells, `%sql` magic — is parsed with `sqlparser-rs` and checked against all BQ rules in a single Rust pass. The Python AST (from `ruff_python_parser`) and SQL ASTs (from `sqlparser-rs`) are joined in memory in Rust. Python receives only `Vec<Finding>`.

Supersedes `s3-10-hybrid-ast` (which was a Python-only annotation approach).

### Files to read

```
# Required
src/burnt_rs/src/notebook.rs    # Cell (s7-01)
src/burnt_rs/src/semantic.rs    # SparkSemanticModel (s7-02)
src/burnt_rs/Cargo.toml

# Reference
tasks/s7-01-notebook-ingestion.md
tasks/s7-02-semantic-model.md
tasks/s3-10-hybrid-ast.md       # superseded — read for rule list
```

### Background

**`HybridAST` — in-memory structure (Rust)**

```rust
// src/burnt_rs/src/hybrid.rs

use ruff_python_ast::Mod as PyMod;
use sqlparser::ast::Statement as SqlStatement;

pub struct SqlFragment {
    pub ast: Vec<SqlStatement>,   // sqlparser-rs output
    pub raw: String,              // original SQL text
    pub source_line: u32,         // line in the enclosing file/cell
    pub call_form: SqlCallForm,   // how this SQL was embedded
}

#[derive(Debug, Clone)]
pub enum SqlCallForm {
    SparkSql,          // spark.sql("...")
    SelectExpr,        // df.selectExpr("expr")
    FilterString,      // df.filter("predicate")
    ExprCall,          // expr("sql expr")
    SqlCell,           // standalone SQL notebook cell
    MagicSql,          // %sql magic in Python cell
}

pub struct HybridAST {
    pub cells: Vec<Cell>,
    pub py_asts: Vec<Option<PyMod>>,          // indexed by cell.index; None for SQL/Other cells
    pub sql_fragments: Vec<SqlFragment>,       // ALL SQL from all contexts
    pub semantic: SparkSemanticModel,
    pub is_dlt: bool,
}
```

**Building the `HybridAST`**

```rust
// src/burnt_rs/src/hybrid.rs

pub fn build(cells: Vec<Cell>) -> HybridAST {
    let is_dlt = cells.iter().any(|c| c.is_dlt);
    let n = cells.len();
    let mut py_asts = vec![None; n];
    let mut sql_fragments: Vec<SqlFragment> = Vec::new();
    let mut semantic = SparkSemanticModel::new();

    for cell in &cells {
        match &cell.language {
            CellLanguage::Python => {
                let source = &cell.source;
                match ruff_python_parser::parse_module(source) {
                    Ok(parsed) => {
                        // Extract embedded SQL fragments from this Python AST
                        let frags = extract_sql_from_py_ast(parsed.syntax(), cell);
                        sql_fragments.extend(frags);

                        // Update semantic model
                        let mut builder = SemanticModelBuilder::new(&mut semantic, cell);
                        ruff_python_ast::visitor::walk_module(&mut builder, parsed.syntax());

                        py_asts[cell.index] = Some(parsed.into_syntax());
                    }
                    Err(_) => {} // parse error — skip this cell, no panic
                }
            }
            CellLanguage::Sql => {
                match sqlparser::parser::Parser::parse_sql(
                    &DatabricksDialect::default(),
                    &cell.source,
                ) {
                    Ok(stmts) => sql_fragments.push(SqlFragment {
                        ast: stmts,
                        raw: cell.source.clone(),
                        source_line: cell.line_offset as u32,
                        call_form: SqlCallForm::SqlCell,
                    }),
                    Err(_) => {}
                }
            }
            _ => {}
        }
    }

    semantic.finalize();

    HybridAST { cells, py_asts, sql_fragments, semantic, is_dlt }
}
```

**Extracting SQL from Python AST nodes**

Walk the Python AST looking for call nodes that embed SQL strings. All of these are SQL contexts:

```rust
fn extract_sql_from_py_ast(module: &ruff_python_ast::ModModule, cell: &Cell) -> Vec<SqlFragment> {
    let mut extractor = SqlExtractor { fragments: vec![], cell };
    ruff_python_ast::visitor::walk_module(&mut extractor, module);
    extractor.fragments
}

struct SqlExtractor<'a> {
    fragments: Vec<SqlFragment>,
    cell: &'a Cell,
}

impl<'a> ruff_python_ast::visitor::Visitor<'_> for SqlExtractor<'a> {
    fn visit_expr_call(&mut self, node: &ExprCall) {
        let line = node.range.start().to_u32() / ??? + self.cell.line_offset as u32;

        if let Some((method, sql_str)) = self.try_extract_sql_string(node) {
            let call_form = match method.as_str() {
                "sql"        => SqlCallForm::SparkSql,
                "selectExpr" => SqlCallForm::SelectExpr,
                "filter" | "where" if !sql_str.trim_start().starts_with(|c: char| c.is_alphabetic() && c != 'n') => {
                    SqlCallForm::FilterString
                }
                _ => return,
            };

            // Parse each SQL string argument separately
            for sql in split_selectexpr_args(&sql_str, &method) {
                if let Ok(ast) = sqlparser::parser::Parser::parse_sql(
                    &DatabricksDialect::default(), &sql,
                ) {
                    self.fragments.push(SqlFragment {
                        ast,
                        raw: sql,
                        source_line: line,
                        call_form: call_form.clone(),
                    });
                }
            }
        }
        self.walk_expr_call(node);
    }
}
```

**`DatabricksDialect` for `sqlparser-rs`**

```rust
// src/burnt_rs/src/dialect.rs
use sqlparser::dialect::Dialect;

#[derive(Debug, Default)]
pub struct DatabricksDialect;

impl Dialect for DatabricksDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        ch.is_alphabetic() || ch == '_'
    }
    fn is_identifier_part(&self, ch: char) -> bool {
        ch.is_alphanumeric() || ch == '_'
    }
    fn supports_filter_during_aggregation(&self) -> bool { true }
    fn supports_within_group_in_aggregate(&self) -> bool { true }
    fn supports_qualify(&self) -> bool { true }   // Databricks QUALIFY clause
    // Add Databricks-specific keywords as needed
}
```

**`HybridChecker` — single-pass rule dispatch**

```rust
// src/burnt_rs/src/checker.rs

pub struct HybridChecker<'a> {
    rules: &'a RuleTable,
}

impl<'a> HybridChecker<'a> {
    pub fn run(hybrid: HybridAST) -> Vec<Finding> {
        let mut findings: Vec<Finding> = Vec::new();

        // 1. Python rule findings — from PySparkVisitor (s7-04)
        for (cell, py_ast) in hybrid.cells.iter().zip(hybrid.py_asts.iter()) {
            if let (CellLanguage::Python, Some(ast)) = (&cell.language, py_ast) {
                let mut visitor = PySparkRuleVisitor::new(cell, hybrid.is_dlt);
                ruff_python_ast::visitor::walk_module(&mut visitor, ast);
                findings.extend(visitor.findings);
            }
        }

        // 2. SQL rule findings — all fragments from all call forms
        for frag in &hybrid.sql_fragments {
            findings.extend(check_sql_fragment(frag, hybrid.is_dlt));
        }

        // 3. Semantic model findings (cache, repeated actions, filter_after_spark_sql)
        findings.extend(hybrid.semantic.findings);

        findings
    }
}
```

**SQL rule implementations (Rust)**

Port BQ001–BQ007 from Python to Rust using `sqlparser-rs` AST:

```rust
// src/burnt_rs/src/rules/sql.rs
use sqlparser::ast::*;

pub fn check_sql_fragment(frag: &SqlFragment, is_dlt: bool) -> Vec<Finding> {
    let mut findings = Vec::new();
    for stmt in &frag.ast {
        check_stmt(stmt, frag, is_dlt, &mut findings);
    }
    findings
}

fn check_stmt(stmt: &Statement, frag: &SqlFragment, is_dlt: bool, out: &mut Vec<Finding>) {
    // BQ002: UNION without ALL
    if let Statement::Query(q) = stmt {
        check_query(q, frag, out);
    }
}

fn check_query(q: &Query, frag: &SqlFragment, out: &mut Vec<Finding>) {
    match &*q.body {
        SetExpr::SetOperation { op: SetOperator::Union, set_quantifier, .. }
            if *set_quantifier != SetQuantifier::All =>
        {
            out.push(Finding::warning("union_instead_of_union_all", frag.source_line,
                "UNION without ALL forces full dedup sort",
                "Use UNION ALL if duplicates are acceptable"));
        }
        SetExpr::Select(sel) => check_select(sel, frag, out),
        _ => {}
    }
}

// BQ001: NOT IN with subquery, BQ003: COUNT(DISTINCT), BQ006: LIKE '%', BQ007: division, etc.
```

**DLT severity escalation**

When `hybrid.is_dlt == true`, upgrade certain rule severities:
- `sdp_prohibited_ops` (collect, count in DLT) → ERROR (already ERROR, keep)
- `global_auto_merge_schema` → ERROR (from WARNING)
- `sparksession_in_transform` → ERROR (from WARNING)
- `cache_without_unpersist` → WARNING + hint about Delta caching

---

## Acceptance Criteria

- [ ] `spark.sql("SELECT * FROM t")` in a `.py` file → `select_star` fires from Rust SQL checker
- [ ] `df.selectExpr("x / y as ratio")` → `division_without_zero_guard` fires from Rust SQL checker
- [ ] `df.filter("NOT id IN (SELECT id FROM deleted)")` → `not_in_with_nulls` fires
- [ ] SQL notebook cell with `UNION` → `union_instead_of_union_all` fires
- [ ] `%sql SELECT * FROM t CROSS JOIN s` in notebook → `cross_join` and `select_star` fire
- [ ] DLT file: `global_auto_merge_schema` → ERROR (not WARNING)
- [ ] Malformed SQL string in `spark.sql()` → graceful skip, no panic, Python/other rules still fire
- [ ] f-string `spark.sql(f"SELECT {col}")` → NO SQL parse attempted (non-Constant arg)
- [ ] `cargo test` passes; `maturin develop` builds
- [ ] `uv run pytest -m unit -v` still passes (Python layer unchanged)

---

## Verification

```bash
cd src/burnt_rs && cargo test hybrid
maturin develop --release

python3 -c "
import burnt_rs
# selectExpr SQL rule
findings = burnt_rs.analyze_file('tests/fixtures/pyspark/selectexpr_sample.py', ['ALL'])
names = [f['rule_id'] for f in findings]
print('Findings:', names)
assert 'division_without_zero_guard' in names
"

burnt check tests/fixtures/notebooks/mixed_paradigm.ipynb --output json
```

---

## Handoff

### Result

[Executor fills this in when done.]

```yaml
status: todo
```

### Blocked reason

Requires s7-01 (`Cell` struct, notebook parsing in Rust) and s7-02 (`SparkSemanticModel` in Rust). Supersedes s3-10.
