use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime};
use rust_decimal::Decimal;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq)]
pub enum DataValue {
    TinyInt(u8),
    SmallInt(i16),
    Int(i32),
    BigInt(i64),
    Float(f64),
    Decimal(Decimal),
    Bool(bool),
    Text(String),
    Binary(Vec<u8>),
    Guid(Uuid),
    Date(NaiveDate),
    Time(NaiveTime),
    DateTime(NaiveDateTime),
    DateTimeOffset(DateTime<FixedOffset>),
    Null,
}

impl DataValue {
    pub fn to_tiberius(&self) -> Box<dyn tiberius::ToSql + Send + Sync> {
        match self {
            DataValue::TinyInt(v) => Box::new(*v),
            DataValue::SmallInt(v) => Box::new(*v),
            DataValue::Int(v) => Box::new(*v),
            DataValue::BigInt(v) => Box::new(*v),
            DataValue::Float(v) => Box::new(*v),
            DataValue::Decimal(v) => Box::new(*v),
            DataValue::Bool(v) => Box::new(*v),
            DataValue::Text(v) => Box::new(v.clone()),
            DataValue::Binary(v) => Box::new(v.clone()),
            DataValue::Guid(v) => Box::new(*v),
            DataValue::Date(v) => Box::new(*v),
            DataValue::Time(v) => Box::new(*v),
            DataValue::DateTime(v) => Box::new(*v),
            DataValue::DateTimeOffset(v) => Box::new(*v),
            DataValue::Null => Box::new(Option::<i32>::None),
        }
    }
}

impl PartialEq<i32> for DataValue {
    fn eq(&self, other: &i32) -> bool {
        match self {
            DataValue::Int(v) => v == other,
            DataValue::TinyInt(v) => (*v as i32) == *other,
            DataValue::SmallInt(v) => (*v as i32) == *other,
            DataValue::BigInt(v) => (*v as i64) == (*other as i64),
            _ => false,
        }
    }
}

impl PartialEq<bool> for DataValue {
    fn eq(&self, other: &bool) -> bool {
        match self {
            DataValue::Bool(v) => v == other,
            _ => false,
        }
    }
}

impl Default for DataValue {
    fn default() -> Self {
        DataValue::Null
    }
}
