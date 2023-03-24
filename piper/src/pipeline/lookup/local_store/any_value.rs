use std::ops::Deref;

use chrono::{TimeZone, Utc};
use chrono_tz::Tz;
use polars::prelude::*;

use crate::{Value, IntoValue};

impl<'a> From<AnyValue<'a>> for Value {
    fn from(val: AnyValue<'a>) -> Self {
        match val {
            AnyValue::Null => Value::Null,
            AnyValue::Boolean(v) => v.into(),
            AnyValue::Utf8(v) => v.to_string().into(),
            AnyValue::UInt8(v) => (v as i32).into(),
            AnyValue::UInt16(v) => (v as i32).into(),
            AnyValue::UInt32(v) => v.into(),
            AnyValue::UInt64(v) => v.into(),
            AnyValue::Int8(v) => (v as i32).into(),
            AnyValue::Int16(v) => (v as i32).into(),
            AnyValue::Int32(v) => v.into(),
            AnyValue::Int64(v) => v.into(),
            AnyValue::Float32(v) => v.into(),
            AnyValue::Float64(v) => v.into(),
            AnyValue::Date(v) => chrono::NaiveDateTime::from_timestamp_opt(v as i64, 0)
                .unwrap()
                .into(),
            AnyValue::Datetime(v, u, tz) => {
                let (sec, nsec) = match u {
                    TimeUnit::Nanoseconds => (v / 1_000_000_000, v % 1_000_000_000),
                    TimeUnit::Microseconds => (v / 1_000_000, v % 1_000_000 * 1000),
                    TimeUnit::Milliseconds => (v / 1_000, v % 1_000 * 1_000_000),
                };
                let ndt = chrono::NaiveDateTime::from_timestamp_opt(sec, nsec as u32).unwrap();
                if let Some(tz) = tz {
                    match tz.parse::<Tz>() {
                        Ok(tz) => tz
                            .from_local_datetime(&ndt)
                            .earliest()
                            .unwrap()
                            .with_timezone(&Utc)
                            .into(),
                        Err(e) => crate::PiperError::InvalidValue(e).into(),
                    }
                } else {
                    ndt.into()
                }
            }
            AnyValue::List(s) => {
                let mut v: Vec<Value> = Vec::with_capacity(s.len());
                for e in s.iter() {
                    v.push(e.into());
                }
                v.into()
            }
            AnyValue::Utf8Owned(s) => s.deref().to_string().into(),
            // AnyValue::Duration(_, _) => todo!(),
            // AnyValue::Time(_) => todo!(),
            // AnyValue::Categorical(_, _, _) => todo!(),
            // AnyValue::Struct(_, _, _) => todo!(),
            // AnyValue::StructOwned(_) => todo!(),
            // AnyValue::Binary(_) => todo!(),
            // AnyValue::BinaryOwned(_) => todo!(),
            _ => crate::PiperError::ExternalError(format!("Unsupported type: {:?}", val)).into(),
        }
    }
}


pub fn to_db_key<T>(v: T) -> String
where
    T: IntoValue,
{
    serde_json::to_string(&serde_json::Value::from(v.into_value())).unwrap()
}
