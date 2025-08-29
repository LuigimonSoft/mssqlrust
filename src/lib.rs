pub mod dataset;
pub mod services;
pub mod infrastructure;
mod repositories;

pub use repositories::{Command, CommandType, Parameter};
pub use services::{dataset_service::DatasetService, service::Service};

use anyhow::Result;
use crate::dataset::{DataSet, DataValue};
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

/// Execute a non-query [`Command`] (e.g., INSERT/UPDATE/DELETE/DDL) and return the
/// total number of affected rows. If the SQL contains multiple statements, the
/// returned count is the sum of row counts reported by the server.
pub async fn execute_non_query(config: MssqlConfig, command: Command) -> Result<u64> {
    let mut connection = SqlConnection::connect(config).await?;
    let (sql, params) = command.build();
    connection.execute_non_query(&sql, params).await
}

/// Execute a [`Command`] and return the first column of the first row
/// as a `DataValue`. If the command returns no rows, returns `Ok(None)`.
pub async fn execute_scalar(config: MssqlConfig, command: Command) -> Result<Option<DataValue>> {
    let mut connection = SqlConnection::connect(config).await?;
    let (sql, params) = command.build();
    connection.execute_scalar(&sql, params).await
}
