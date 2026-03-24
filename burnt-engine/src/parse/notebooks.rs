use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotebookCell {
    pub cell_type: String,
    pub source: String,
    pub line_offset: u32,
}

pub fn parse_notebook(path: &str) -> Vec<NotebookCell> {
    let _ = path;
    vec![]
}

pub fn detect_language(cells: &[NotebookCell]) -> String {
    let sql_cells = cells.iter().filter(|c| c.cell_type == "sql").count();
    let python_cells = cells.iter().filter(|c| c.cell_type == "python").count();
    
    if sql_cells > 0 && python_cells == 0 {
        "sql".to_string()
    } else if sql_cells > 0 {
        "mixed".to_string()
    } else {
        "python".to_string()
    }
}