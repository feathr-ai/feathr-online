use rusqlite::{types::FromSql, ToSql};

use crate::{pipeline::value::IntoValue, PiperError, Value};

#[allow(clippy::from_over_into)]
impl Into<PiperError> for rusqlite::Error {
    fn into(self) -> PiperError {
        PiperError::ExternalError(self.to_string())
    }
}

impl FromSql for Value {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        let v = match value {
            rusqlite::types::ValueRef::Null => Value::Null,
            rusqlite::types::ValueRef::Integer(v) => v.into_value(),
            rusqlite::types::ValueRef::Real(v) => v.into_value(),
            rusqlite::types::ValueRef::Text(v) => String::from_utf8_lossy(v).to_string().into_value(),
            rusqlite::types::ValueRef::Blob(_) => Value::Error(PiperError::ExternalError("Unsupported type".to_string())),
        };
        Ok(v)
    }
}

impl ToSql for Value {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        match self {
            Value::Bool(v) => v.to_sql(),
            Value::Int(v) => v.to_sql(),
            Value::Long(v) => v.to_sql(),
            Value::Float(v) => v.to_sql(),
            Value::Double(v) => v.to_sql(),
            Value::String(v) => v.to_sql(),
            Value::DateTime(v) => v.to_sql(),
            _ => unreachable!("Types should be already checked."),
        }
    }
}
pub fn row_to_values(row: &rusqlite::Row) -> Vec<Value> {
    let mut ret = vec![];
    while let Ok(v) = row.get::<usize, Value>(ret.len()) {
        ret.push(v.into_value());
    }
    ret
}
