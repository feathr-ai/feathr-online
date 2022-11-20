use crate::pipeline::{ValueType, PiperError};

use super::Function;

#[derive(Clone, Debug)]
pub struct Len;

impl Function for Len {
    fn get_output_type(&self, argument_types: &[crate::pipeline::ValueType]) -> Result<crate::pipeline::ValueType, crate::pipeline::PiperError> {
        match argument_types {
            [ValueType::Array] => Ok(ValueType::Int),
            [ValueType::String] => Ok(ValueType::Int),
            _ => Err(PiperError::InvalidArgumentType("len".to_owned(), 1, argument_types[0])),
        }
    }

    fn eval(&self, arguments: Vec<crate::pipeline::Value>) -> Result<crate::pipeline::Value, crate::pipeline::PiperError> {
        match arguments.as_slice() {
            [crate::pipeline::Value::Array(array)] => Ok((array.len() as i32).into()),
            [crate::pipeline::Value::String(string)] => Ok((string.len() as i32).into()),
            [_] => Err(PiperError::InvalidArgumentType("len".to_owned(), 1, arguments[0].value_type())),
            _ => Err(PiperError::InvalidArgumentCount(1, arguments.len())),
        }
    }
}