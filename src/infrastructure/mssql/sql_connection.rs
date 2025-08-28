use anyhow::Result;
use futures::StreamExt;
use tiberius::{Client, QueryItem};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

use crate::dataset::{DataCell, DataColumn, DataRow, DataSet, DataTable, DataValue};

use super::MssqlConfig;

pub struct SqlConnection {
    client: Client<Compat<TcpStream>>,
}

impl SqlConnection {
    pub async fn connect(config: MssqlConfig) -> Result<Self> {
        let cfg = config.to_config();
        let addr = cfg.get_addr();
        let tcp = TcpStream::connect(addr).await?;
        tcp.set_nodelay(true)?;
        let client = Client::connect(cfg, tcp.compat_write()).await?;
        Ok(Self { client })
    }

    pub async fn execute(
        &mut self,
        sql: &str,
        params: Vec<Box<dyn tiberius::ToSql + Send + Sync>>,
    ) -> Result<DataSet> {
        let param_refs: Vec<&dyn tiberius::ToSql> = params
            .iter()
            .map(|p| p.as_ref() as &dyn tiberius::ToSql)
            .collect();
        let mut stream = self.client.query(sql, &param_refs[..]).await?;
        let mut dataset = DataSet::new();
        let mut current: Option<DataTable> = None;
        while let Some(item) = stream.next().await {
            match item? {
                QueryItem::Metadata(meta) => {
                    if let Some(table) = current.take() {
                        dataset.tables.insert(table.name.clone(), table);
                    }
                    let mut table = DataTable::new(&format!("table{}", meta.result_index()));
                    table.columns = meta
                        .columns()
                        .iter()
                        .map(|c| DataColumn {
                            name: c.name().to_string(),
                            sql_type: format!("{:?}", c.column_type()),
                            size: None,
                            nullable: true,
                        })
                        .collect();
                    current = Some(table);
                }
                QueryItem::Row(row) => {
                    if current.is_none() {
                        current = Some(DataTable::new("table0"));
                    }
                    let table = current.as_mut().unwrap();
                    let mut data_row = DataRow::default();
                    for (cd, col) in row.into_iter().zip(table.columns.iter()) {
                        let v = match cd {
                            tiberius::ColumnData::U8(opt) => {
                                opt.map(DataValue::TinyInt).unwrap_or(DataValue::Null)
                            }
                            tiberius::ColumnData::I16(opt) => {
                                opt.map(DataValue::SmallInt).unwrap_or(DataValue::Null)
                            }
                            tiberius::ColumnData::I32(opt) => {
                                opt.map(DataValue::Int).unwrap_or(DataValue::Null)
                            }
                            tiberius::ColumnData::I64(opt) => {
                                opt.map(DataValue::BigInt).unwrap_or(DataValue::Null)
                            }
                            tiberius::ColumnData::F32(opt) => opt
                                .map(|v| DataValue::Float(v as f64))
                                .unwrap_or(DataValue::Null),
                            tiberius::ColumnData::F64(opt) => {
                                opt.map(DataValue::Float).unwrap_or(DataValue::Null)
                            }
                            tiberius::ColumnData::Bit(opt) => {
                                opt.map(DataValue::Bool).unwrap_or(DataValue::Null)
                            }
                            tiberius::ColumnData::String(opt) => {
                                if let Some(s) = opt.as_ref() {
                                    DataValue::Text(s.to_string())
                                } else {
                                    DataValue::Null
                                }
                            }
                            tiberius::ColumnData::Guid(opt) => {
                                opt.map(DataValue::Guid).unwrap_or(DataValue::Null)
                            }
                            tiberius::ColumnData::Binary(opt) => {
                                if let Some(b) = opt.as_ref() {
                                    DataValue::Binary(b.to_vec())
                                } else {
                                    DataValue::Null
                                }
                            }
                            tiberius::ColumnData::Numeric(opt) => opt
                                .map(|n| {
                                    DataValue::Decimal(rust_decimal::Decimal::from_i128_with_scale(
                                        n.value(),
                                        n.scale() as u32,
                                    ))
                                })
                                .unwrap_or(DataValue::Null),
                            tiberius::ColumnData::DateTime(opt) => {
                                let val: Option<chrono::NaiveDateTime> =
                                    <chrono::NaiveDateTime as tiberius::FromSqlOwned>::from_sql_owned(
                                        tiberius::ColumnData::DateTime(opt),
                                    )
                                    .unwrap();
                                val.map(DataValue::DateTime).unwrap_or(DataValue::Null)
                            }
                            tiberius::ColumnData::SmallDateTime(opt) => {
                                let val: Option<chrono::NaiveDateTime> =
                                    <chrono::NaiveDateTime as tiberius::FromSqlOwned>::from_sql_owned(
                                        tiberius::ColumnData::SmallDateTime(opt),
                                    )
                                    .unwrap();
                                val.map(DataValue::DateTime).unwrap_or(DataValue::Null)
                            }
                            tiberius::ColumnData::DateTime2(opt) => {
                                let val: Option<chrono::NaiveDateTime> =
                                    <chrono::NaiveDateTime as tiberius::FromSqlOwned>::from_sql_owned(
                                        tiberius::ColumnData::DateTime2(opt),
                                    )
                                    .unwrap();
                                val.map(DataValue::DateTime).unwrap_or(DataValue::Null)
                            }
                            tiberius::ColumnData::Time(opt) => {
                                let val: Option<chrono::NaiveTime> =
                                    <chrono::NaiveTime as tiberius::FromSqlOwned>::from_sql_owned(
                                        tiberius::ColumnData::Time(opt),
                                    )
                                    .unwrap();
                                val.map(DataValue::Time).unwrap_or(DataValue::Null)
                            }
                            tiberius::ColumnData::Date(opt) => {
                                let val: Option<chrono::NaiveDate> =
                                    <chrono::NaiveDate as tiberius::FromSqlOwned>::from_sql_owned(
                                        tiberius::ColumnData::Date(opt),
                                    )
                                    .unwrap();
                                val.map(DataValue::Date).unwrap_or(DataValue::Null)
                            }
                            tiberius::ColumnData::DateTimeOffset(opt) => {
                                let val: Option<chrono::DateTime<chrono::FixedOffset>> =
                                    <chrono::DateTime<chrono::FixedOffset> as tiberius::FromSqlOwned>::from_sql_owned(
                                        tiberius::ColumnData::DateTimeOffset(opt),
                                    )
                                    .unwrap();
                                val.map(DataValue::DateTimeOffset)
                                    .unwrap_or(DataValue::Null)
                            }
                            tiberius::ColumnData::Xml(opt) => {
                                if let Some(x) = opt.as_ref() {
                                    DataValue::Text(x.as_ref().to_string())
                                } else {
                                    DataValue::Null
                                }
                            }
                        };
                        data_row
                            .cells
                            .insert(col.name.clone(), DataCell { value: v });
                    }
                    table.rows.push(data_row);
                }
            }
        }
        if let Some(table) = current.take() {
            dataset.tables.insert(table.name.clone(), table);
        }
        Ok(dataset)
    }
}
