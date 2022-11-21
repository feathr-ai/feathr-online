use tracing::instrument;

use crate::pipeline::{PiperError, Value, ValueType};

use super::Function;
#[derive(Debug)]
pub struct SplitFunction;

impl Function for SplitFunction {
    fn get_output_type(&self, argument_types: &[ValueType]) -> Result<ValueType, PiperError> {
        if argument_types.len() != 2 {
            return Err(PiperError::ArityError(
                "split".to_string(),
                argument_types.len(),
            ));
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
    fn eval(&self, arguments: Vec<Value>) -> Value {
        if arguments.len() != 2 {
            return Value::Error(PiperError::InvalidArgumentCount(2, arguments.len()));
        }
        let string = match arguments[0].get_string() {
            Ok(string) => string,
            Err(err) => return Value::Error(err),
        };
        let delimiter = match arguments[1].get_string() {
            Ok(string) => string,
            Err(err) => return Value::Error(err),
        };
        let mut result = Vec::new();
        for s in string.split(delimiter.as_ref()) {
            result.push(Value::String(s.to_string().into()));
        }
        Value::Array(result)
    }
}
