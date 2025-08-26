#[derive(Debug, Clone, PartialEq, Default)]
pub struct DataColumn {
    pub name: String,
    pub sql_type: String,
    pub size: Option<u32>,
    pub nullable: bool,
}
