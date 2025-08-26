use crate::dataset::DataValue;

pub struct Parameter {
    pub name: String,
    pub value: DataValue,
}

impl Parameter {
    pub fn new(name: &str, value: DataValue) -> Self {
        Self { name: name.into(), value }
    }
}
