use anyhow::Result;
use async_trait::async_trait;

use crate::dataset::DataSet;

use crate::infrastructure::mssql::SqlConnection;

#[async_trait]
pub trait QueryExecutor {
    async fn query(&mut self, sql: &str, params: Vec<Box<dyn tiberius::ToSql + Send + Sync>>) -> Result<DataSet>;
}

#[async_trait]
impl QueryExecutor for SqlConnection {
    async fn query(&mut self, sql: &str, params: Vec<Box<dyn tiberius::ToSql + Send + Sync>>) -> Result<DataSet> {
        self.execute(sql, params).await
    }
}
