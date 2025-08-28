use anyhow::Result;
use async_trait::async_trait;

use crate::dataset::DataSet;

use super::{command::Command, query_executor::QueryExecutor};

#[async_trait]
pub trait DatasetRepository {
    async fn execute(&mut self, command: Command) -> Result<DataSet>;
}

pub struct MssqlDatasetRepository<E: QueryExecutor + Send> {
    executor: E,
}

impl<E: QueryExecutor + Send> MssqlDatasetRepository<E> {
    pub fn new(executor: E) -> Self {
        Self { executor }
    }
}

#[async_trait]
impl<E: QueryExecutor + Send> DatasetRepository for MssqlDatasetRepository<E> {
    async fn execute(&mut self, command: Command) -> Result<DataSet> {
        let (sql, params) = command.build();
        self.executor.query(&sql, params).await
    }
}
