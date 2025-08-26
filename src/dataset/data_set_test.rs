use super::*;
use crate::dataset::{DataCell, DataColumn, DataRow, DataValue};

#[test]
fn create_dataset() {
    let mut table = DataTable::new("table1");
    table.columns.push(DataColumn {
        name: "id".into(),
        sql_type: "int".into(),
        size: None,
        nullable: false,
    });
    let mut row = DataRow::default();
    row.cells.insert(
        "id".into(),
        DataCell {
            value: DataValue::Int(1),
        },
    );
    table.rows.push(row);
    let mut ds = DataSet::new();
    ds.tables.insert(table.name.clone(), table);
    assert_eq!(ds.tables["table1"][0]["id"], DataValue::Int(1));
}
