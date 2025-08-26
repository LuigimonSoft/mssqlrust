use mssqlrust::dataset::DataValue;
use mssqlrust::infrastructure::mssql::MssqlConfig;
use mssqlrust::{execute, Command, Parameter};

#[tokio::test]
#[ignore]
async fn basic_query() {
    let config = MssqlConfig::new(
        "localhost",
        1433,
        "sa",
        "YourStrong!Passw0rd",
        "master",
        true,
    );
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
        Command::query("SELECT @P0 as value").with_param(Parameter::new("p", DataValue::Int(7)));
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
    let create = Command::query(
        r#"
        IF OBJECT_ID('sp_no_params', 'P') IS NOT NULL DROP PROCEDURE sp_no_params;
        CREATE PROCEDURE sp_no_params AS BEGIN SELECT 2 AS value; END
    "#,
    );
    execute(config.clone(), create).await.unwrap();

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
    let create = Command::query(
        r#"
        IF OBJECT_ID('sp_with_param', 'P') IS NOT NULL DROP PROCEDURE sp_with_param;
        CREATE PROCEDURE sp_with_param @val INT AS BEGIN SELECT @val AS value; END
    "#,
    );
    execute(config.clone(), create).await.unwrap();

    let cmd = Command::stored_procedure("sp_with_param")
        .with_param(Parameter::new("@val", DataValue::Int(5)));
    let ds = execute(config, cmd).await.unwrap();
    assert_eq!(ds.tables["table0"][0]["value"], DataValue::Int(5));
}
