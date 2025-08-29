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

// From conversions to easily build DataValue from native Rust types
impl From<i32> for DataValue {
    fn from(v: i32) -> Self {
        DataValue::Int(v)
    }
}

impl From<u8> for DataValue {
    fn from(v: u8) -> Self {
        DataValue::TinyInt(v)
    }
}

impl From<i16> for DataValue {
    fn from(v: i16) -> Self {
        DataValue::SmallInt(v)
    }
}

impl From<i64> for DataValue {
    fn from(v: i64) -> Self {
        DataValue::BigInt(v)
    }
}

impl From<f64> for DataValue {
    fn from(v: f64) -> Self {
        DataValue::Float(v)
    }
}

impl From<Decimal> for DataValue {
    fn from(v: Decimal) -> Self {
        DataValue::Decimal(v)
    }
}

impl From<bool> for DataValue {
    fn from(v: bool) -> Self {
        DataValue::Bool(v)
    }
}

impl From<String> for DataValue {
    fn from(v: String) -> Self {
        DataValue::Text(v)
    }
}

impl From<&str> for DataValue {
    fn from(v: &str) -> Self {
        DataValue::Text(v.to_string())
    }
}

impl From<Vec<u8>> for DataValue {
    fn from(v: Vec<u8>) -> Self {
        DataValue::Binary(v)
    }
}

impl From<&[u8]> for DataValue {
    fn from(v: &[u8]) -> Self {
        DataValue::Binary(v.to_vec())
    }
}

impl From<Uuid> for DataValue {
    fn from(v: Uuid) -> Self {
        DataValue::Guid(v)
    }
}

impl From<NaiveDate> for DataValue {
    fn from(v: NaiveDate) -> Self {
        DataValue::Date(v)
    }
}

impl From<NaiveTime> for DataValue {
    fn from(v: NaiveTime) -> Self {
        DataValue::Time(v)
    }
}

impl From<NaiveDateTime> for DataValue {
    fn from(v: NaiveDateTime) -> Self {
        DataValue::DateTime(v)
    }
}

impl From<DateTime<FixedOffset>> for DataValue {
    fn from(v: DateTime<FixedOffset>) -> Self {
        DataValue::DateTimeOffset(v)
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

impl PartialEq<f64> for DataValue {
    fn eq(&self, other: &f64) -> bool {
        match self {
            DataValue::Float(v) => v == other,
            _ => false,
        }
    }
}

impl PartialEq<Decimal> for DataValue {
    fn eq(&self, other: &Decimal) -> bool {
        match self {
            DataValue::Decimal(v) => v == other,
            _ => false,
        }
    }
}

impl PartialEq<&str> for DataValue {
    fn eq(&self, other: &&str) -> bool {
        match self {
            DataValue::Text(v) => v == other,
            _ => false,
        }
    }
}

impl PartialEq<String> for DataValue {
    fn eq(&self, other: &String) -> bool {
        match self {
            DataValue::Text(v) => v == other,
            _ => false,
        }
    }
}

impl PartialEq<Uuid> for DataValue {
    fn eq(&self, other: &Uuid) -> bool {
        match self {
            DataValue::Guid(v) => v == other,
            _ => false,
        }
    }
}

// Chrono types comparisons
impl PartialEq<NaiveDate> for DataValue {
    fn eq(&self, other: &NaiveDate) -> bool {
        match self {
            DataValue::Date(v) => v == other,
            _ => false,
        }
    }
}

impl PartialEq<NaiveTime> for DataValue {
    fn eq(&self, other: &NaiveTime) -> bool {
        match self {
            DataValue::Time(v) => v == other,
            _ => false,
        }
    }
}

impl PartialEq<NaiveDateTime> for DataValue {
    fn eq(&self, other: &NaiveDateTime) -> bool {
        match self {
            DataValue::DateTime(v) => v == other,
            _ => false,
        }
    }
}

impl PartialEq<DateTime<FixedOffset>> for DataValue {
    fn eq(&self, other: &DateTime<FixedOffset>) -> bool {
        match self {
            DataValue::DateTimeOffset(v) => v == other,
            _ => false,
        }
    }
}

// Binary comparisons
impl PartialEq<&[u8]> for DataValue {
    fn eq(&self, other: &&[u8]) -> bool {
        match self {
            DataValue::Binary(v) => v.as_slice() == *other,
            _ => false,
        }
    }
}

impl PartialEq<Vec<u8>> for DataValue {
    fn eq(&self, other: &Vec<u8>) -> bool {
        match self {
            DataValue::Binary(v) => v == other,
            _ => false,
        }
    }
}

impl Default for DataValue {
    fn default() -> Self {
        DataValue::Null
    }
}

impl DataValue {
    pub fn is_null(&self) -> bool {
        matches!(self, DataValue::Null)
    }
}
