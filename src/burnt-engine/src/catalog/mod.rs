pub mod databricks;

use std::collections::HashMap;

/// A single column definition returned by a catalog schema fetch.
#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    /// Canonical type name as returned by the catalog, e.g. `"LONG"`, `"STRING"`, `"TIMESTAMP"`.
    pub data_type: String,
    pub nullable: bool,
}

/// Table schema fetched from a catalog (Unity Catalog, Hive metastore).
#[derive(Debug, Clone)]
pub struct TableSchema {
    pub columns: Vec<ColumnDef>,
}

impl TableSchema {
    /// Look up a column by name (case-insensitive).
    pub fn column(&self, name: &str) -> Option<&ColumnDef> {
        let lower = name.to_ascii_lowercase();
        self.columns
            .iter()
            .find(|c| c.name.to_ascii_lowercase() == lower)
    }
}

/// Source of catalog metadata for use during static analysis enrichment.
///
/// Implementations are expected to be cheap to clone / share across threads.
/// The engine calls these methods once per distinct table ref per analysis run
/// (pre-enrichment), so implementations do not need to cache aggressively.
pub trait CatalogClient: Send + Sync {
    /// Fetch the column schema for a fully-qualified table name.
    ///
    /// Returns `None` when the table is not found or the catalog is unreachable.
    fn get_schema(&self, table_fqn: &str) -> Option<TableSchema>;

    /// Fetch the Delta / Hive table properties for a fully-qualified table name.
    ///
    /// Returns `None` when the table is not found or properties are unavailable.
    fn get_table_properties(&self, table_fqn: &str) -> Option<HashMap<String, String>>;
}
