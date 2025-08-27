use mssqlrust::dataset::DataValue;
use mssqlrust::infrastructure::mssql::MssqlConfig;
use mssqlrust::{execute, Command, Parameter};

use futures::StreamExt;
use tiberius::Client;
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;

async fn run_ddl(config: &MssqlConfig, sql: &str) {
    let cfg = config.to_config();
    let addr = cfg.get_addr();
    let tcp = TcpStream::connect(addr).await.unwrap();
    tcp.set_nodelay(true).unwrap();
    let mut client = Client::connect(cfg, tcp.compat_write()).await.unwrap();
    let mut stream = client.simple_query(sql).await.unwrap();
    while stream.next().await.is_some() {}
}

fn test_config() -> MssqlConfig {
    MssqlConfig::new(
        "localhost",
        1433,
        "sa",
        "YourStrong!Passw0rd",
        "master",
        true,
    )
}

#[tokio::test]
#[ignore]
async fn basic_query() {
    let config = test_config();
    let cmd = Command::query("SELECT 1 as value");
    let ds = execute(config, cmd).await.unwrap();
    assert_eq!(ds.tables["table0"][0]["value"], DataValue::Int(1));
}

#[tokio::test]
#[ignore]
async fn query_with_params() {
    let config = MssqlConfig::new(
        "localhost",
        1433,
        "sa",
        "YourStrong!Passw0rd",
        "master",
        true,
    );
    let cmd =
        Command::query("SELECT @P1 as value").with_param(Parameter::new("P1", DataValue::Int(7)));
    let ds = execute(config, cmd).await.unwrap();
    assert_eq!(ds.tables["table0"][0]["value"], DataValue::Int(7));
}

#[tokio::test]
#[ignore]
async fn stored_procedure() {
    let config = MssqlConfig::new(
        "localhost",
        1433,
        "sa",
        "YourStrong!Passw0rd",
        "master",
        true,
    );
    run_ddl(
        &config,
        "IF OBJECT_ID('sp_no_params', 'P') IS NOT NULL DROP PROCEDURE sp_no_params",
    )
    .await;
    run_ddl(&config, "CREATE PROCEDURE sp_no_params AS BEGIN SELECT 2 AS value; END")
        .await;

    let cmd = Command::stored_procedure("sp_no_params");
    let ds = execute(config, cmd).await.unwrap();
    assert_eq!(ds.tables["table0"][0]["value"], DataValue::Int(2));
}

#[tokio::test]
#[ignore]
async fn stored_procedure_with_params() {
    let config = MssqlConfig::new(
        "localhost",
        1433,
        "sa",
        "YourStrong!Passw0rd",
        "master",
        true,
    );
    run_ddl(
        &config,
        "IF OBJECT_ID('sp_with_param', 'P') IS NOT NULL DROP PROCEDURE sp_with_param",
    )
    .await;
    run_ddl(
        &config,
        "CREATE PROCEDURE sp_with_param @val INT AS BEGIN SELECT @val AS value; END",
    )
    .await;

    let cmd = Command::stored_procedure("sp_with_param")
        .with_param(Parameter::new("val", DataValue::Int(5)));
    let ds = execute(config, cmd).await.unwrap();
    assert_eq!(ds.tables["table0"][0]["value"], DataValue::Int(5));
}
