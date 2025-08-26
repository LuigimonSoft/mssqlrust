#[derive(Debug, Clone, PartialEq)]
pub enum DataValue {
    Int(i32),
    BigInt(i64),
    Float(f64),
    Bool(bool),
    Text(String),
    Binary(Vec<u8>),
    Null,
}

impl DataValue {
    pub fn to_tiberius(&self) -> Box<dyn tiberius::ToSql + Send + Sync> {
        match self {
            DataValue::Int(v) => Box::new(*v),
            DataValue::BigInt(v) => Box::new(*v),
            DataValue::Float(v) => Box::new(*v),
            DataValue::Bool(v) => Box::new(*v),
            DataValue::Text(v) => Box::new(v.clone()),
            DataValue::Binary(v) => Box::new(v.clone()),
            DataValue::Null => Box::new(Option::<i32>::None),
        }
    }
}

impl Default for DataValue {
    fn default() -> Self {
        DataValue::Null
    }
}
