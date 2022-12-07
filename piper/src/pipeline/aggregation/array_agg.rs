use crate::{PiperError, Value, ValueType};

use super::AggregationFunction;

#[derive(Clone, Debug, Default)]
pub struct ArrayAgg {
    result: Vec<Value>,
}

impl AggregationFunction for ArrayAgg {
    fn get_output_type(&self, input_type: &[ValueType]) -> Result<ValueType, PiperError> {
        if input_type.len() != 1 {
            return Err(PiperError::InvalidArgumentCount(1, input_type.len()));
        }
        Ok(ValueType::Array)
    }

    fn feed(&mut self, arguments: &[Value]) -> Result<(), PiperError> {
        if arguments.len() != 1 {
            return Err(PiperError::InvalidArgumentCount(1, arguments.len()));
        }
        self.result.push(arguments[0].clone());
        Ok(())
    }

    fn get_result(&self) -> Result<Value, PiperError> {
        Ok(self.result.clone().into())
    }

    fn dump(&self) -> String {
        "array_agg".to_string()
    }
}
