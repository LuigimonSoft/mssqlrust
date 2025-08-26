pub mod dataset;
pub mod services;
pub mod infrastructure;
mod repositories;

pub use repositories::{Command, CommandType, Parameter};
pub use services::{dataset_service::DatasetService, service::Service};

use anyhow::Result;
use crate::dataset::DataSet;
use crate::infrastructure::mssql::{MssqlConfig, SqlConnection};
use crate::repositories::MssqlDatasetRepository;

/// Execute a [`Command`] against the database using provided [`MssqlConfig`].
/// This function wires up the internal repository and service layers.
pub async fn execute(config: MssqlConfig, command: Command) -> Result<DataSet> {
    let connection = SqlConnection::connect(config).await?;
    let repo = MssqlDatasetRepository::new(connection);
    let mut service = DatasetService::new(repo);
    service.fetch(command).await
}
