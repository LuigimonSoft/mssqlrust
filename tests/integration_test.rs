use mssqlrust::dataset::DataValue::Null;
use mssqlrust::infrastructure::mssql::MssqlConfig;
use mssqlrust::{execute, Command, Parameter};

use chrono::{DateTime, NaiveDate, NaiveTime};
use futures::StreamExt;
use rust_decimal::Decimal;
use tiberius::Client;
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;
use uuid::Uuid;

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
    assert_eq!(ds.tables["table0"][0]["value"], 1);
}

#[tokio::test]
#[ignore]
async fn bit_query() {
    let config = test_config();
    let cmd = Command::query("SELECT CAST(1 AS bit) as value");
    let ds = execute(config, cmd).await.unwrap();
    assert_eq!(ds.tables["table0"][0]["value"], true);
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
        Command::query("SELECT @P1 as value").with_param(Parameter::new("P1", 7));
    let ds = execute(config, cmd).await.unwrap();
    assert_eq!(ds.tables["table0"][0]["value"], 7);
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
    run_ddl(
        &config,
        "CREATE PROCEDURE sp_no_params AS BEGIN SELECT 2 AS value; END",
    )
    .await;

    let cmd = Command::stored_procedure("sp_no_params");
    let ds = execute(config, cmd).await.unwrap();
    assert_eq!(ds.tables["table0"][0]["value"], 2);
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
        .with_param(Parameter::new("val", 5));
    let ds = execute(config, cmd).await.unwrap();
    assert_eq!(ds.tables["table0"][0]["value"], 5);
}

#[tokio::test]
#[ignore]
async fn all_types_query() {
    let config = test_config();
    let cmd = Command::query(
        "SELECT \
            CAST(1 AS tinyint) AS tiny_col, \
            CAST(2 AS smallint) AS small_col, \
            CAST(3 AS int) AS int_col, \
            CAST(4 AS bigint) AS big_col, \
            CAST(5.5 AS float) AS float_col, \
            CAST(123.45 AS numeric(5,2)) AS decimal_col, \
            CAST(1 AS bit) AS bit_col, \
            CAST(N'text' AS nvarchar(10)) AS text_col, \
            CAST(0x010203 AS varbinary(3)) AS binary_col, \
            CAST('6F9619FF-8B86-D011-B42D-00CF4FC964FF' AS uniqueidentifier) AS guid_col, \
            CAST('2023-01-01' AS date) AS date_col, \
            CAST('12:34:56' AS time(0)) AS time_col, \
            CAST('2023-01-01T01:02:03' AS datetime2) AS datetime_col, \
            CAST('2023-01-01T01:02:03+02:00' AS datetimeoffset) AS dto_col, \
            CAST(NULL AS int) AS null_col",
    );
    let ds = execute(config, cmd).await.unwrap();

    let row = &ds.tables["table0"][0];

    assert_eq!(row["tiny_col"], 1);
    assert_eq!(row["small_col"], 2);
    assert_eq!(row["int_col"], 3);
    assert_eq!(row["big_col"], 4);
    assert_eq!(row["float_col"], 5.5);
    assert_eq!(row["decimal_col"], Decimal::new(12345, 2));
    assert_eq!(row["bit_col"], true);
    assert_eq!(row["text_col"], "text");
    assert_eq!(row["binary_col"], vec![1, 2, 3]);
    assert_eq!(
        row["guid_col"],
        Uuid::parse_str("6F9619FF-8B86-D011-B42D-00CF4FC964FF").unwrap()
    );
    assert_eq!(
        row["date_col"],
        NaiveDate::from_ymd_opt(2023, 1, 1).unwrap()
    );
    assert_eq!(
        row["time_col"],
        NaiveTime::from_hms_opt(12, 34, 56).unwrap()
    );
    assert_eq!(
        row["datetime_col"],
        NaiveDate::from_ymd_opt(2023, 1, 1)
            .unwrap()
            .and_hms_opt(1, 2, 3)
            .unwrap()
    );
    assert_eq!(
        row["dto_col"],
        DateTime::parse_from_rfc3339("2023-01-01T01:02:03+02:00")
            .unwrap()
    );
    assert!(matches!(row["null_col"], Null));

    let cols = &ds.tables["table0"].columns;
    assert_eq!(cols[0].sql_type, "Int1");
    assert_eq!(cols[1].sql_type, "Int2");
    assert_eq!(cols[2].sql_type, "Int4");
    assert_eq!(cols[3].sql_type, "Int8");
    assert_eq!(cols[4].sql_type, "Float8");
    assert_eq!(cols[5].sql_type, "Numericn");
    assert_eq!(cols[6].sql_type, "Bitn");
    assert_eq!(cols[7].sql_type, "NVarchar");
    assert_eq!(cols[8].sql_type, "BigVarBin");
    assert_eq!(cols[9].sql_type, "Guid");
    assert_eq!(cols[10].sql_type, "Daten");
    assert_eq!(cols[11].sql_type, "Timen");
    assert_eq!(cols[12].sql_type, "Datetime2");
    assert_eq!(cols[13].sql_type, "DatetimeOffsetn");
    assert_eq!(cols[14].sql_type, "Int4");
}

#[tokio::test]
#[ignore]
async fn non_query_rows_affected() {
    let config = test_config();
    // Prepare a table for testing
    run_ddl(
        &config,
        "IF OBJECT_ID('dbo.mssqlrust_non_query_test', 'U') IS NOT NULL DROP TABLE dbo.mssqlrust_non_query_test",
    )
    .await;
    run_ddl(
        &config,
        "CREATE TABLE dbo.mssqlrust_non_query_test (id INT PRIMARY KEY, val INT NOT NULL)",
    )
    .await;

    // Insert two rows in a single statement -> affected = 2
    let insert_cmd = Command::query(
        "INSERT INTO dbo.mssqlrust_non_query_test (id, val) VALUES (1, 10), (2, 20)",
    );
    let affected = mssqlrust::execute_non_query(config.clone(), insert_cmd)
        .await
        .unwrap();
    assert_eq!(affected, 2);

    // Update one row using parameters -> affected = 1
    let update_cmd = Command::query(
        "UPDATE dbo.mssqlrust_non_query_test SET val = val + 1 WHERE id = @id",
    )
    .with_param(Parameter::new("id", 1));
    let affected = mssqlrust::execute_non_query(config.clone(), update_cmd)
        .await
        .unwrap();
    assert_eq!(affected, 1);

    // Cleanup
    run_ddl(
        &config,
        "DROP TABLE dbo.mssqlrust_non_query_test",
    )
    .await;
}
