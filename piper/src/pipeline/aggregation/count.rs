use std::collections::HashSet;

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

#[derive(Clone, Debug, Default)]
pub struct CountIf {
    count: usize,
}

impl AggregationFunction for CountIf {
    fn get_output_type(&self, input_type: &[ValueType]) -> Result<ValueType, PiperError> {
        match input_type {
            [ValueType::Bool] => Ok(ValueType::Long),
            [ValueType::Dynamic] => Ok(ValueType::Long),
            [t] => Err(PiperError::InvalidArgumentType("count_if".to_string(), 1, *t)),
            _ => Err(PiperError::InvalidArgumentCount(1, input_type.len())),
        }
    }

    fn feed(&mut self, arguments: &[Value]) -> Result<(), PiperError> {
        if arguments.len() != 1 {
            return Err(PiperError::InvalidArgumentCount(1, arguments.len()));
        }
        if arguments[0].get_bool().unwrap_or_default() {
            self.count += 1;
        }
        Ok(())
    }

    fn get_result(&self) -> Result<Value, PiperError> {
        Ok(self.count.into())
    }

    fn dump(&self) -> String {
        "count_if".to_string()
    }
}

#[derive(Clone, Debug, Default)]
pub struct DistinctCount {
    buckets: HashSet<Vec<Value>>,
}

impl AggregationFunction for DistinctCount {
    fn get_output_type(&self, input_type: &[ValueType]) -> Result<ValueType, PiperError> {
        if input_type.is_empty() {
            return Err(PiperError::InvalidArgumentCount(1, input_type.len()));
        }
        Ok(ValueType::Long)
    }

    fn feed(&mut self, arguments: &[Value]) -> Result<(), PiperError> {
        self.buckets.insert(arguments.to_vec());
        Ok(())
    }

    fn get_result(&self) -> Result<Value, PiperError> {
        Ok(self.buckets.len().into())
    }

    fn dump(&self) -> String {
        "distinct_count".to_string()
    }
}

