use crate::pipeline::{PiperError, Value, ValueType};

use super::Function;

#[derive(Clone, Debug)]
pub struct Len;

impl Function for Len {
    fn get_output_type(
        &self,
        argument_types: &[ValueType],
    ) -> Result<ValueType, PiperError> {
        match argument_types {
            [ValueType::Array] => Ok(ValueType::Int),
            [ValueType::String] => Ok(ValueType::Int),
            [ValueType::Dynamic] => Ok(ValueType::Int),
            _ => Err(PiperError::InvalidArgumentType(
                "len".to_owned(),
                1,
                argument_types[0],
            )),
        }
    }

    fn eval(&self, arguments: Vec<Value>) -> Value {
        match arguments.as_slice() {
            [Value::Array(array)] => (array.len() as i32).into(),
            [Value::String(string)] => (string.len() as i32).into(),
            [_] => Value::Error(PiperError::InvalidArgumentType(
                "len".to_owned(),
                1,
                arguments[0].value_type(),
            )),
            _ => Value::Error(PiperError::InvalidArgumentCount(1, arguments.len())),
        }
    }
}
