use tracing::instrument;

use crate::pipeline::{PiperError, Value, ValueType};

use super::Function;

#[derive(Debug)]
pub struct SubstringFunction;

impl Function for SubstringFunction {
    fn get_output_type(&self, argument_types: &[ValueType]) -> Result<ValueType, PiperError> {
        if argument_types.len() != 2 {
            return Err(PiperError::ArityError(
                "substring".to_string(),
                argument_types.len(),
            ));
        }
        if argument_types[0] != ValueType::String {
            return Err(PiperError::InvalidArgumentType(
                "substring".to_string(),
                0,
                argument_types[0],
            ));
        }
        if argument_types[1] != ValueType::Int {
            return Err(PiperError::InvalidArgumentType(
                "substring".to_string(),
                1,
                argument_types[1],
            ));
        }
        if argument_types[2] != ValueType::Int {
            return Err(PiperError::InvalidArgumentType(
                "substring".to_string(),
                2,
                argument_types[2],
            ));
        }
        Ok(ValueType::String)
    }

    #[instrument(level = "trace", skip(self))]
    fn eval(&self, mut arguments: Vec<Value>) -> Result<Value, PiperError> {
        if arguments.len() != 3 {
            return Err(PiperError::InvalidArgumentCount(3, arguments.len()));
        }
        let length = arguments
            .remove(2)
            .try_convert(super::ValueType::Long)?
            .get_long()?;
        let start = arguments
            .remove(1)
            .try_convert(super::ValueType::Long)?
            .get_long()?;
        let string = arguments.remove(0);
        let start = if start < 0 {
            string.get_string()?.len() as i64 + start
        } else {
            start
        };
        let length = if length < 0 {
            string.get_string()?.len() as i64 + length - start
        } else {
            length
        };
        Ok(Value::String(
            string.get_string()?[start as usize..(start + length) as usize]
                .to_string()
                .into(),
        ))
    }
}
