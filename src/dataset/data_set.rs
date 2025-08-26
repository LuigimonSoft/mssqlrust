use std::collections::HashMap;

use super::DataTable;

#[derive(Debug, Clone, PartialEq, Default)]
pub struct DataSet {
    pub tables: HashMap<String, DataTable>,
}

impl DataSet {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }
}
