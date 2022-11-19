use chrono::TimeZone;
use chrono_tz::Tz;
use tracing::instrument;

use crate::pipeline::{PiperError, Value, ValueType};

use super::Function;

#[derive(Debug)]
pub struct TimestampFunction;

const DEFAULT_FORMAT: &'static str = "%Y-%m-%d %H:%M:%S";

impl Function for TimestampFunction {
    fn get_output_type(
        &self,
        argument_types: &[crate::pipeline::ValueType],
    ) -> Result<crate::pipeline::ValueType, crate::pipeline::PiperError> {
        if argument_types.is_empty() || argument_types.len() > 3 {
            return Err(PiperError::ArityError(
                "timestamp".to_string(),
                argument_types.len(),
            ));
        }
        if argument_types[0] != ValueType::String {
            return Err(PiperError::InvalidArgumentType(
                "timestamp".to_string(),
                0,
                argument_types[0],
            ));
        }
        if argument_types.len() > 1 && argument_types[1] != ValueType::String {
            return Err(PiperError::InvalidArgumentType(
                "timestamp".to_string(),
                1,
                argument_types[1],
            ));
        }
        if argument_types.len() > 2 && argument_types[2] != ValueType::String {
            return Err(PiperError::InvalidArgumentType(
                "timestamp".to_string(),
                2,
                argument_types[2],
            ));
        }
        Ok(ValueType::Double)
    }

    #[instrument(level = "trace", skip(self))]
    fn eval(
        &self,
        arguments: Vec<Value>,
    ) -> Result<crate::pipeline::Value, crate::pipeline::PiperError> {
        if arguments.is_empty() || arguments.len() > 3 {
            return Err(PiperError::ArityError(
                "timestamp".to_string(),
                arguments.len(),
            ));
        }

        match arguments.as_slice() {
            [Value::String(s)] => self.timestamp(s, DEFAULT_FORMAT, &Tz::UTC),
            [Value::String(s), Value::String(format)] => self.timestamp(s, &format, &Tz::UTC),
            [Value::String(s), Value::String(format), Value::String(tz)] => {
                if let Ok(tz) = tz.parse::<Tz>() {
                    self.timestamp(s, &format, &tz)
                } else {
                    Ok(Value::Null)
                }
            }

            [a] => Err(PiperError::InvalidArgumentType(
                "timestamp".to_string(),
                1,
                a.value_type(),
            ))?,
            [_, b] => Err(PiperError::InvalidArgumentType(
                "timestamp".to_string(),
                2,
                b.value_type(),
            ))?,
            [_, _, c] => Err(PiperError::InvalidArgumentType(
                "timestamp".to_string(),
                3,
                c.value_type(),
            ))?,
            _ => unreachable!(),
        }
    }
}

impl TimestampFunction {
    fn timestamp(&self, s: &str, format: &str, tz: &Tz) -> Result<Value, PiperError> {
        let timestamp = tz
            .datetime_from_str(s, format)
            .map(|ts| Value::Double(ts.timestamp() as f64));
        Ok(timestamp.unwrap_or_default())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_timestamp() {
        use super::*;
        use crate::pipeline::function::Function;
        use crate::pipeline::Value;

        let f = TimestampFunction;
        // Default format
        assert_eq!(
            f.eval(vec![Value::String("2020-01-01 00:00:00".into())])
                .unwrap(),
            Value::Double(1577836800.0)
        );
        // Customize format
        assert_eq!(
            f.eval(vec![
                Value::String("00:00:00-2020/01/01".into()),
                Value::String("%H:%M:%S-%Y/%m/%d".into())
            ])
            .unwrap(),
            Value::Double(1577836800.0)
        );
        // Customize format and specified time zone
        assert_eq!(
            f.eval(vec![
                Value::String("00:00:00-2020/01/01".into()),
                Value::String("%H:%M:%S-%Y/%m/%d".into()),
                Value::String("Asia/Shanghai".into())
            ])
            .unwrap(),
            // 8 hours earlier than UTC
            Value::Double(1577836800.0 - 8.0 * 3600.0)
        );
    }
}
