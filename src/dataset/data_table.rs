use std::ops::Index;

use super::{DataColumn, DataRow};

#[derive(Debug, Clone, PartialEq, Default)]
pub struct DataTable {
    pub name: String,
    pub columns: Vec<DataColumn>,
    pub rows: Vec<DataRow>,
}

impl DataTable {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.into(),
            columns: Vec::new(),
            rows: Vec::new(),
        }
    }
}

impl Index<usize> for DataTable {
    type Output = DataRow;

    fn index(&self, index: usize) -> &Self::Output {
        &self.rows[index]
    }
}
