use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use tiberius::{FromSql, ToSql};

use crate::{pipeline::value::IntoValue, PiperError, Value, Logged};

#[allow(clippy::from_over_into)]
impl Into<PiperError> for tiberius::error::Error {
    fn into(self) -> PiperError {
        PiperError::ExternalError(self.to_string())
    }
}

impl<'a> FromSql<'a> for Value {
    fn from_sql(value: &'a tiberius::ColumnData<'static>) -> tiberius::Result<Option<Self>> {
        let v = match value {
            tiberius::ColumnData::U8(v) => v.map(i32::from).into_value(),
            tiberius::ColumnData::I16(v) => v.map(i32::from).into_value(),
            tiberius::ColumnData::I32(v) => (*v).into_value(),
            tiberius::ColumnData::I64(v) => (*v).into_value(),
            tiberius::ColumnData::F32(v) => (*v).into_value(),
            tiberius::ColumnData::F64(v) => (*v).into_value(),
            tiberius::ColumnData::Bit(v) => (*v).into_value(),
            tiberius::ColumnData::String(v) => v.clone().into_value(),
            tiberius::ColumnData::DateTime(_) => NaiveDateTime::from_sql(value).into_value(),
            tiberius::ColumnData::SmallDateTime(_) => NaiveDateTime::from_sql(value).into_value(),
            tiberius::ColumnData::Date(_) => NaiveDate::from_sql(value).into_value(),
            tiberius::ColumnData::DateTime2(_) => NaiveDateTime::from_sql(value).into_value(),
            tiberius::ColumnData::DateTimeOffset(_) => {
                DateTime::<Utc>::from_sql(value).into_value()
            }
            _ => Value::Error(PiperError::ExternalError("Unsupported type".to_string())),
        };
        Ok(Some(v))
    }
}

impl ToSql for Value {
    fn to_sql(&self) -> tiberius::ColumnData<'_> {
        match self {
            Value::Bool(v) => tiberius::ColumnData::Bit(Some(*v)),
            Value::Int(v) => tiberius::ColumnData::I32(Some(*v)),
            Value::Long(v) => tiberius::ColumnData::I64(Some(*v)),
            Value::Float(v) => tiberius::ColumnData::F32(Some(*v)),
            Value::Double(v) => tiberius::ColumnData::F64(Some(*v)),
            Value::String(v) => tiberius::ColumnData::String(Some(v.clone())),
            Value::DateTime(v) => v.to_sql(),
            _ => unreachable!("Types should be already checked."),
        }
    }
}

pub fn row_to_values(row: tiberius::Row) -> Vec<Value> {
    (0..row.len())
        .map(|i| {
            let v = row
                .try_get::<'_, Value, _>(i)
                .log()
                .map_err(|e| PiperError::ExternalError(e.to_string()));
            v.into_value()
        })
        .collect()
}
