use super::*;
use anyhow::Result;
use async_trait::async_trait;
use crate::{dataset::DataSet, repositories::{Command, DatasetRepository}};
use std::sync::{Arc, Mutex};

struct MockRepo {
    pub called: Arc<Mutex<bool>>,
}

#[async_trait]
impl DatasetRepository for MockRepo {
    async fn execute(&mut self, _command: Command) -> Result<DataSet> {
        *self.called.lock().unwrap() = true;
        Ok(DataSet::new())
    }
}

#[tokio::test]
async fn service_calls_repository() {
    let repo = MockRepo { called: Arc::new(Mutex::new(false)) };
    let called_ref = repo.called.clone();
    let mut service = DatasetService::new(repo);
    service.fetch(Command::query("SELECT 1")).await.unwrap();
    assert_eq!(*called_ref.lock().unwrap(), true);
}
