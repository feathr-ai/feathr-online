use crate::{PiperError, Value, ValueType};

use super::AggregationFunction;

#[derive(Clone, Debug, Default)]
pub struct Count {
    count: usize,
}

impl AggregationFunction for Count {
    fn get_output_type(&self, _input_type: &[ValueType]) -> Result<ValueType, PiperError> {
        Ok(ValueType::Long)
    }

    fn feed(&mut self, _arguments: &[Value]) -> Result<(), PiperError> {
        self.count += 1;
        Ok(())
    }

    fn get_result(&self) -> Result<Value, PiperError> {
        Ok(self.count.into())
    }

    fn dump(&self) -> String {
        "count".to_string()
    }
}
