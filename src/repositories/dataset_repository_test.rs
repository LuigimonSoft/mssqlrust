use super::query_executor::QueryExecutor;
use super::*;
use crate::dataset::{DataSet, DataValue};
use crate::repositories::Parameter;
use anyhow::Result;
use async_trait::async_trait;
use std::sync::{Arc, Mutex};

struct MockExecutor {
    pub last_sql: Arc<Mutex<String>>,
    pub last_params: Arc<Mutex<usize>>,
}

#[async_trait]
impl QueryExecutor for MockExecutor {
    async fn query(
        &mut self,
        sql: &str,
        params: Vec<Box<dyn tiberius::ToSql + Send + Sync>>,
    ) -> Result<DataSet> {
        *self.last_sql.lock().unwrap() = sql.to_string();
        *self.last_params.lock().unwrap() = params.len();
        Ok(DataSet::new())
    }
}

#[tokio::test]
async fn test_query_command() {
    let exec = MockExecutor {
        last_sql: Arc::new(Mutex::new(String::new())),
        last_params: Arc::new(Mutex::new(0)),
    };
    let sql_ref = exec.last_sql.clone();
    let params_ref = exec.last_params.clone();
    let mut repo = MssqlDatasetRepository::new(exec);
    let cmd = Command::query("SELECT 1 WHERE id = @P1")
        .with_param(Parameter::new("P1", 1));
    repo.execute(cmd).await.unwrap();
    assert_eq!(*sql_ref.lock().unwrap(), "SELECT 1 WHERE id = @P1");
    assert_eq!(*params_ref.lock().unwrap(), 1);
}

#[tokio::test]
async fn test_sp_command() {
    let exec = MockExecutor {
        last_sql: Arc::new(Mutex::new(String::new())),
        last_params: Arc::new(Mutex::new(0)),
    };
    let sql_ref = exec.last_sql.clone();
    let params_ref = exec.last_params.clone();
    let mut repo = MssqlDatasetRepository::new(exec);
    let cmd = Command::stored_procedure("sp_test").with_param(Parameter::new("id", 1));
    repo.execute(cmd).await.unwrap();
    assert_eq!(*sql_ref.lock().unwrap(), "EXEC sp_test @id = @P1");
    assert_eq!(*params_ref.lock().unwrap(), 1);
}

#[tokio::test]
async fn test_sp_command_with_at_prefix() {
    let exec = MockExecutor {
        last_sql: Arc::new(Mutex::new(String::new())),
        last_params: Arc::new(Mutex::new(0)),
    };
    let sql_ref = exec.last_sql.clone();
    let params_ref = exec.last_params.clone();
    let mut repo = MssqlDatasetRepository::new(exec);
    let cmd = Command::stored_procedure("sp_test").with_param(Parameter::new("@id", 1));
    repo.execute(cmd).await.unwrap();
    assert_eq!(*sql_ref.lock().unwrap(), "EXEC sp_test @id = @P1");
    assert_eq!(*params_ref.lock().unwrap(), 1);
}
