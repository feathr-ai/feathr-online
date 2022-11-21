use crate::pipeline::{PiperError, Value, ValueType};

use super::Function;

#[derive(Clone, Debug)]
pub struct ToJsonStringFunction;

impl Function for ToJsonStringFunction {
    fn get_output_type(&self, argument_types: &[ValueType]) -> Result<ValueType, PiperError> {
        if argument_types.len() != 1 {
            return Err(PiperError::InvalidArgumentCount(1, argument_types.len()));
        }
        return Ok(ValueType::String);
    }

    fn eval(&self, mut arguments: Vec<Value>) -> Result<Value, PiperError> {
        if arguments.len() != 1 {
            return Err(PiperError::InvalidArgumentCount(1, arguments.len()));
        }

        if arguments[0].is_error() {
            return Ok(arguments.pop().unwrap());
        }

        let value: serde_json::Value = arguments.pop().unwrap().into();

        Ok(Value::String(serde_json::to_string(&value).unwrap().into()))
    }
}
