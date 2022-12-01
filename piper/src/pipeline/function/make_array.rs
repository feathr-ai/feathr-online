use crate::pipeline::{PiperError, Value, ValueType};

use super::Function;

#[derive(Clone, Debug)]
pub struct MakeArray;

impl Function for MakeArray {
    fn get_output_type(
        &self,
        _argument_types: &[ValueType],
    ) -> Result<ValueType, PiperError> {
        Ok(ValueType::Array)
    }

    fn eval(&self, arguments: Vec<Value>) -> Value {
        Value::Array(arguments)
    }
}
