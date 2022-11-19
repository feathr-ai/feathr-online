use tracing::instrument;

use crate::pipeline::{ValueType, PiperError, Value};

use super::Function;
#[derive(Debug)]
pub struct SplitFunction;

impl Function for SplitFunction {
    fn get_output_type(&self, argument_types: &[ValueType]) -> Result<ValueType, PiperError> {
        if argument_types.len() != 2 {
            return Err(PiperError::ArityError("split".to_string(), argument_types.len()));
        }
        if argument_types[0] != ValueType::String {
            return Err(PiperError::InvalidArgumentType(
                "split".to_string(),
                0,
                argument_types[0],
            ));
        }
        if argument_types[1] != ValueType::String {
            return Err(PiperError::InvalidArgumentType(
                "split".to_string(),
                1,
                argument_types[1],
            ));
        }
        Ok(ValueType::Array)
    }

    #[instrument(level = "trace", skip(self))]
    fn eval(&self, arguments: Vec<Value>) -> Result<Value, PiperError> {
        if arguments.len() != 2 {
            return Err(PiperError::InvalidArgumentCount(2, arguments.len()));
        }
        let string = arguments[0].get_string()?;
        let delimiter = arguments[1].get_string()?;
        let mut result = Vec::new();
        for s in string.split(delimiter.as_ref()) {
            result.push(Value::String(s.to_string().into()));
        }
        Ok(Value::Array(result))
    }
}