use std::collections::HashMap;
use std::ops::Index;

use super::{DataCell, DataValue};

#[derive(Debug, Clone, PartialEq, Default)]
pub struct DataRow {
    pub cells: HashMap<String, DataCell>,
}

impl Index<&str> for DataRow {
    type Output = DataValue;

    fn index(&self, column: &str) -> &Self::Output {
        &self.cells.get(column).expect("unknown column").value
    }
}
