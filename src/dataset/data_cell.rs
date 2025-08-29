use super::DataValue;

#[derive(Debug, Clone, PartialEq, Default)]
pub struct DataCell {
    pub value: DataValue,
}

impl DataCell {
    pub fn new<T>(value: T) -> Self
    where
        DataValue: From<T>,
    {
        DataCell {
            value: DataValue::from(value),
        }
    }
}

impl<T> From<T> for DataCell
where
    DataValue: From<T>,
{
    fn from(value: T) -> Self {
        DataCell::new(value)
    }
}
