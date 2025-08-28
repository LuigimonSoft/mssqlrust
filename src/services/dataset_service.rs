use anyhow::Result;
use async_trait::async_trait;

use crate::{dataset::DataSet, repositories::{Command, DatasetRepository}};

use super::service::Service;

pub struct DatasetService<R: DatasetRepository + Send> {
    repository: R,
}

impl<R: DatasetRepository + Send> DatasetService<R> {
    pub fn new(repository: R) -> Self {
        Self { repository }
    }
}

#[async_trait]
impl<R: DatasetRepository + Send> Service for DatasetService<R> {
    async fn fetch(&mut self, command: Command) -> Result<DataSet> {
        self.repository.execute(command).await
    }
}
