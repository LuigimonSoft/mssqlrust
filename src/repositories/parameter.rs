use crate::dataset::DataValue;

pub struct Parameter {
    pub name: String,
    pub value: DataValue,
}

impl Parameter {
    pub fn new<T>(name: &str, value: T) -> Self
    where
        T: Into<DataValue>,
    {
        Self {
            name: name.into(),
            value: value.into(),
        }
    }
}
