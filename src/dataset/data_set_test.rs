use super::*;
use crate::dataset::{DataCell, DataColumn, DataRow, DataValue};
use rust_decimal::Decimal;
use uuid::Uuid;
use chrono::{DateTime, NaiveDate, NaiveTime};

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
        DataCell::new(1),
    );
    table.rows.push(row);
    let mut ds = DataSet::new();
    ds.tables.insert(table.name.clone(), table);
    assert_eq!(ds.tables["table1"][0]["id"], 1);
}
#[test]
fn float_comparison() {
    let mut table = DataTable::new("table1");
    table.columns.push(DataColumn {
        name: "float_col".into(),
        sql_type: "float".into(),
        size: None,
        nullable: false,
    });
    let mut row = DataRow::default();
    row.cells.insert(
        "float_col".into(),
        DataCell {
            value: DataValue::Float(5.5),
        },
    );
    table.rows.push(row);
    let mut ds = DataSet::new();
    ds.tables.insert(table.name.clone(), table);
    assert_eq!(ds.tables["table1"][0]["float_col"], 5.5);
}

#[test]
fn integer_comparison() {
    let mut table = DataTable::new("table1");
    table.columns.push(DataColumn {
        name: "int_col".into(),
        sql_type: "int".into(),
        size: None,
        nullable: false,
    });
    let mut row = DataRow::default();
    row.cells.insert(
        "int_col".into(),
        DataCell::new(42),
    );
    table.rows.push(row);
    let mut ds = DataSet::new();
    ds.tables.insert(table.name.clone(), table);
    assert_eq!(ds.tables["table1"][0]["int_col"], 42);
}

#[test]
fn decimal_comparison() {
    let mut table = DataTable::new("table1");
    table.columns.push(DataColumn {
        name: "decimal_col".into(),
        sql_type: "decimal".into(),
        size: None,
        nullable: false,
    });
    let mut row = DataRow::default();
    row.cells.insert(
        "decimal_col".into(),
        DataCell {
            value: DataValue::Decimal(Decimal::new(12345, 2)),
        },
    );
    table.rows.push(row);
    let mut ds = DataSet::new();
    ds.tables.insert(table.name.clone(), table);
    assert_eq!(ds.tables["table1"][0]["decimal_col"], Decimal::new(12345, 2));
}

#[test]
fn text_comparison() {
    let mut table = DataTable::new("table1");
    table.columns.push(DataColumn {
        name: "text_col".into(),
        sql_type: "text".into(),
        size: None,
        nullable: false,
    });
    let mut row = DataRow::default();
    row.cells.insert(
        "text_col".into(),
        DataCell::new("Hello, world!"),
    );
    table.rows.push(row);
    let mut ds = DataSet::new();
    ds.tables.insert(table.name.clone(), table);
    assert_eq!(ds.tables["table1"][0]["text_col"], "Hello, world!");
}

#[test]
fn binary_comparison(){
    let mut table = DataTable::new("table1");
    table.columns.push(DataColumn {
        name: "binary_col".into(),
        sql_type: "binary".into(),
        size: None,
        nullable: false,
    });
    let mut row = DataRow::default();
    row.cells.insert(
        "binary_col".into(),
        DataCell {
            value: DataValue::Binary(vec![1, 2, 3]),
        },
    );
    table.rows.push(row);
    let mut ds = DataSet::new();
    ds.tables.insert(table.name.clone(), table);
    assert_eq!(ds.tables["table1"][0]["binary_col"], vec![1, 2, 3]);
}
#[test]
fn guid_comparison() {
    let mut table = DataTable::new("table1");
    table.columns.push(DataColumn {
        name: "guid_col".into(),
        sql_type: "uniqueidentifier".into(),
        size: None,
        nullable: false,
    });
    let mut row = DataRow::default();
    row.cells.insert(
        "guid_col".into(),
        DataCell {
            value: DataValue::Guid(
                Uuid::parse_str("6F9619FF-8B86-D011-B42D-00CF4FC964FF").unwrap(),
            ),
        },
    );
    table.rows.push(row);
    let mut ds = DataSet::new();
    ds.tables.insert(table.name.clone(), table);
    assert_eq!(
        ds.tables["table1"][0]["guid_col"],
        Uuid::parse_str("6F9619FF-8B86-D011-B42D-00CF4FC964FF").unwrap()
    );

}

#[test]
fn date_comparison() {
    let mut table = DataTable::new("table1");
    table.columns.push(DataColumn {
        name: "date_col".into(),
        sql_type: "date".into(),
        size: None,
        nullable: false,
    });
    let mut row = DataRow::default();
    row.cells.insert(
        "date_col".into(),
        DataCell {
            value: DataValue::Date(NaiveDate::from_ymd_opt(2023, 1, 1).unwrap()),
        },
    );
    table.rows.push(row);
    let mut ds = DataSet::new();
    ds.tables.insert(table.name.clone(), table);
    assert_eq!(
        ds.tables["table1"][0]["date_col"],
        NaiveDate::from_ymd_opt(2023, 1, 1).unwrap()
    );
}

#[test]
fn time_comparison() {
    let mut table = DataTable::new("table1");
    table.columns.push(DataColumn {
        name: "time_col".into(),
        sql_type: "time".into(),
        size: None,
        nullable: false,
    });
    let mut row = DataRow::default();
    row.cells.insert(
        "time_col".into(),
        DataCell {
            value: DataValue::Time(NaiveTime::from_hms_opt(12, 34, 56).unwrap()),
        },
    );
    table.rows.push(row);
    let mut ds = DataSet::new();
    ds.tables.insert(table.name.clone(), table);
    assert_eq!(
        ds.tables["table1"][0]["time_col"],
        NaiveTime::from_hms_opt(12, 34, 56).unwrap()
    );
}

#[test]
fn datetime_comparison() {
    let mut table = DataTable::new("table1");
    table.columns.push(DataColumn {
        name: "datetime_col".into(),
        sql_type: "datetime2".into(),
        size: None,
        nullable: false,
    });
    let mut row = DataRow::default();
    row.cells.insert(
        "datetime_col".into(),
        DataCell {
            value: DataValue::DateTime(NaiveDate::from_ymd_opt(2023, 1, 1).unwrap().and_hms_opt(12, 34, 56).unwrap()),
        },
    );
    table.rows.push(row);
    let mut ds = DataSet::new();
    ds.tables.insert(table.name.clone(), table);
    assert_eq!(
        ds.tables["table1"][0]["datetime_col"],
        NaiveDate::from_ymd_opt(2023, 1, 1).unwrap().and_hms_opt(12, 34, 56).unwrap()
    );
}

#[test]
fn dto_comparison() {
    let mut table = DataTable::new("table1");
    table.columns.push(DataColumn {
        name: "dto_col".into(),
        sql_type: "datetimeoffset".into(),
        size: None,
        nullable: false,
    });
    let mut row = DataRow::default();
    row.cells.insert(
        "dto_col".into(),
        DataCell {
            value: DataValue::DateTimeOffset(
                DateTime::parse_from_rfc3339("2023-01-01T01:02:03+02:00").unwrap(),
            ),
        },
    );
    table.rows.push(row);
    let mut ds = DataSet::new();
    ds.tables.insert(table.name.clone(), table);
    assert_eq!(
        ds.tables["table1"][0]["dto_col"],
        DateTime::parse_from_rfc3339("2023-01-01T01:02:03+02:00").unwrap()
    );
}

#[test]
fn null_comparison() {
    let mut table = DataTable::new("table1");
    table.columns.push(DataColumn {
        name: "null_col".into(),
        sql_type: "int".into(),
        size: None,
        nullable: true,
    });
    let mut row = DataRow::default();
    row.cells.insert(
        "null_col".into(),
        DataCell {
            value: DataValue::Null,
        },
    );
    table.rows.push(row);
    let mut ds = DataSet::new();
    ds.tables.insert(table.name.clone(), table);
    assert!(ds.tables["table1"][0]["null_col"].is_null());
}