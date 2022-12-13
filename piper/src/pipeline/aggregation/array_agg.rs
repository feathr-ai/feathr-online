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

#[derive(Clone, Debug, Default)]
pub struct ArrayAggIf {
    result: Vec<Value>,
}

impl AggregationFunction for ArrayAggIf {
    fn get_output_type(&self, input_type: &[ValueType]) -> Result<ValueType, PiperError> {
        if input_type.len() != 2 {
            return Err(PiperError::InvalidArgumentCount(2, input_type.len()));
        }
        Ok(ValueType::Array)
    }

    fn feed(&mut self, arguments: &[Value]) -> Result<(), PiperError> {
        if arguments.len() != 2 {
            return Err(PiperError::InvalidArgumentCount(1, arguments.len()));
        }
        if arguments[1].get_bool().unwrap_or_default() {
            self.result.push(arguments[0].clone());
        }
        Ok(())
    }

    fn get_result(&self) -> Result<Value, PiperError> {
        Ok(self.result.clone().into())
    }

    fn dump(&self) -> String {
        "array_agg_if".to_string()
    }
}
