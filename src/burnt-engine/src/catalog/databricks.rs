use std::collections::HashMap;

use serde::Deserialize;

use super::{CatalogClient, ColumnDef, TableSchema};
use crate::session::rest_client::RestClient;

#[derive(Deserialize)]
struct UcColumn {
    name: String,
    #[serde(rename = "type_name")]
    type_name: String,
    #[serde(default = "bool_true")]
    nullable: bool,
}

fn bool_true() -> bool {
    true
}

#[derive(Deserialize)]
struct UcTable {
    #[serde(default)]
    columns: Vec<UcColumn>,
    #[serde(default)]
    properties: HashMap<String, String>,
}

/// Catalog client backed by the Databricks Unity Catalog REST API.
///
/// Fetches table schemas and properties from
/// `GET /api/2.1/unity-catalog/tables/{catalog}.{schema}.{table}`.
/// Responses are cached per instance so repeated calls for the same table
/// within a single analysis run are free.
pub struct DatabricksCatalogClient {
    client: RestClient,
    base_url: String,
    cache: std::sync::Mutex<HashMap<String, Option<UcTableCached>>>,
}

#[derive(Clone)]
struct UcTableCached {
    schema: TableSchema,
    properties: HashMap<String, String>,
}

impl DatabricksCatalogClient {
    pub fn new(base_url: &str, token: Option<&str>) -> Self {
        let auth = token.map(|t| format!("Bearer {t}"));
        Self {
            client: RestClient::new(auth.as_deref()),
            base_url: base_url.trim_end_matches('/').to_string(),
            cache: std::sync::Mutex::new(HashMap::new()),
        }
    }

    fn fetch_cached(&self, table_fqn: &str) -> Option<UcTableCached> {
        {
            let cache = self.cache.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(entry) = cache.get(table_fqn) {
                return entry.clone();
            }
        }
        let url = format!(
            "{}/api/2.1/unity-catalog/tables/{}",
            self.base_url, table_fqn
        );
        let result = self
            .client
            .get_json::<UcTable>(&url)
            .ok()
            .map(|t| UcTableCached {
                schema: TableSchema {
                    columns: t
                        .columns
                        .into_iter()
                        .map(|c| ColumnDef {
                            name: c.name,
                            data_type: c.type_name,
                            nullable: c.nullable,
                        })
                        .collect(),
                },
                properties: t.properties,
            });
        let mut cache = self.cache.lock().unwrap_or_else(|p| p.into_inner());
        cache.insert(table_fqn.to_string(), result.clone());
        result
    }
}

impl CatalogClient for DatabricksCatalogClient {
    fn get_schema(&self, table_fqn: &str) -> Option<TableSchema> {
        self.fetch_cached(table_fqn).map(|c| c.schema)
    }

    fn get_table_properties(&self, table_fqn: &str) -> Option<HashMap<String, String>> {
        self.fetch_cached(table_fqn).map(|c| c.properties)
    }
}
