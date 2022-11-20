use crate::pipeline::{ValueType, Value};

use super::Function;

#[derive(Clone, Debug)]
pub struct MakeArray;

impl Function for MakeArray {
    fn get_output_type(&self, _argument_types: &[crate::pipeline::ValueType]) -> Result<crate::pipeline::ValueType, crate::pipeline::PiperError> {
        Ok(ValueType::Array)
    }

    fn eval(&self, arguments: Vec<crate::pipeline::Value>) -> Result<crate::pipeline::Value, crate::pipeline::PiperError> {
        Ok(Value::Array(arguments))
    }
}