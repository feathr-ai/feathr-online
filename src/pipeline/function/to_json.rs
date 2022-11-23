use crate::pipeline::{PiperError, Value, ValueType};

use super::Function;

#[derive(Clone, Debug)]
pub struct ToJsonStringFunction;

impl Function for ToJsonStringFunction {
    fn get_output_type(&self, argument_types: &[ValueType]) -> Result<ValueType, PiperError> {
        if argument_types.len() != 1 {
            return Err(PiperError::InvalidArgumentCount(1, argument_types.len()));
        }
        Ok(ValueType::String)
    }

    fn eval(&self, mut arguments: Vec<Value>) -> Value {
        match arguments.as_slice() {
            [Value::Error(e)] => e.into(),
            [_] => {
                let j: serde_json::Value = arguments.pop().into();
                Value::String(serde_json::to_string(&j).unwrap().into())
            }
            _ => Value::Error(PiperError::InvalidArgumentCount(1, arguments.len())),
        }
    }
}
