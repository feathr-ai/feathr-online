use crate::{PiperError, Value, ValueType};

use super::AggregationFunction;

#[derive(Clone, Debug, Default)]
pub struct First {
    result: Option<Value>,
}

impl AggregationFunction for First {
    fn get_output_type(&self, input_type: &[ValueType]) -> Result<ValueType, PiperError> {
        match input_type {
            [_] => Ok(input_type[0]),
            [_, ValueType::Bool] => Ok(input_type[0]),
            [_, t] => Err(PiperError::InvalidArgumentType("first".to_string(), 2, *t)),
            _ => Err(PiperError::InvalidArgumentCount(2, input_type.len())),
        }
    }

    fn feed(&mut self, arguments: &[Value]) -> Result<(), PiperError> {
        if arguments.len() > 2 {
            return Err(PiperError::InvalidArgumentCount(2, arguments.len()));
        }
        match arguments {
            [v] => {
                if self.result.is_none() {
                    self.result = Some(v.clone());
                }
                Ok(())
            }
            [v, Value::Bool(false)] => {
                if self.result.is_none() {
                    self.result = Some(v.clone());
                }
                Ok(())
            }
            [v, Value::Bool(true)] => {
                // Ignore null values
                if !v.is_null() && self.result.is_none() {
                    self.result = Some(v.clone());
                }
                Ok(())
            }
            [_, b] => Err(PiperError::InvalidArgumentType(
                "first".to_string(),
                2,
                b.value_type(),
            )),
            _ => Err(PiperError::InvalidArgumentCount(2, arguments.len())),
        }
    }

    fn get_result(&self) -> Result<Value, PiperError> {
        Ok(self.result.clone().unwrap_or_default())
    }

    fn dump(&self) -> String {
        "first".to_string()
    }
}

#[derive(Clone, Debug, Default)]
pub struct Last {
    result: Value,
}

impl AggregationFunction for Last {
    fn get_output_type(&self, input_type: &[ValueType]) -> Result<ValueType, PiperError> {
        match input_type {
            [_] => Ok(input_type[0]),
            [_, ValueType::Bool] => Ok(input_type[0]),
            [_, t] => Err(PiperError::InvalidArgumentType("last".to_string(), 2, *t)),
            _ => Err(PiperError::InvalidArgumentCount(2, input_type.len())),
        }
    }

    fn feed(&mut self, arguments: &[Value]) -> Result<(), PiperError> {
        if arguments.len() > 2 {
            return Err(PiperError::InvalidArgumentCount(2, arguments.len()));
        }
        match arguments {
            [v] => {
                self.result = v.clone();
                Ok(())
            }
            [v, Value::Bool(false)] => {
                self.result = v.clone();
                Ok(())
            }
            [v, Value::Bool(true)] => {
                // Ignore null values
                if !v.is_null() {
                    self.result = v.clone();
                }
                Ok(())
            }
            [_, b] => Err(PiperError::InvalidArgumentType(
                "first".to_string(),
                2,
                b.value_type(),
            )),
            _ => Err(PiperError::InvalidArgumentCount(2, arguments.len())),
        }
    }

    fn get_result(&self) -> Result<Value, PiperError> {
        Ok(self.result.clone())
    }

    fn dump(&self) -> String {
        "last".to_string()
    }
}
