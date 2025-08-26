use anyhow::Result;
use async_trait::async_trait;

use crate::{dataset::DataSet, repositories::Command};

#[async_trait]
pub trait Service {
    async fn fetch(&mut self, command: Command) -> Result<DataSet>;
}
