use crate::{
    pipeline::operator::{LessThanOperator, Operator},
    PiperError, Value, ValueType,
};

use super::AggregationFunction;

#[derive(Clone, Debug, Default)]
pub struct Min {
    min: Option<Value>,
    op: LessThanOperator,
}

impl AggregationFunction for Min {
    fn get_output_type(&self, input_type: &[ValueType]) -> Result<ValueType, PiperError> {
        if input_type.len() != 1 {
            return Err(PiperError::InvalidArgumentCount(1, input_type.len()));
        }
        Ok(input_type[0])
    }

    fn feed(&mut self, arguments: &[Value]) -> Result<(), PiperError> {
        if arguments.len() != 1 {
            return Err(PiperError::InvalidArgumentCount(1, arguments.len()));
        }
        if arguments[0].is_null() {
            return Ok(());
        }
        match &self.min {
            None => {
                self.min = Some(arguments[0].clone());
            }
            Some(v) => {
                self.min = if self
                    .op
                    .eval(vec![arguments[0].clone(), v.clone()])
                    .get_bool()?
                {
                    Some(arguments[0].clone())
                } else {
                    Some(v.clone())
                };
            }
        }

        Ok(())
    }

    fn get_result(&self) -> Result<Value, PiperError> {
        Ok(self.min.clone().unwrap_or_default())
    }

    fn dump(&self) -> String {
        "min".to_string()
    }
}

#[derive(Clone, Debug, Default)]
pub struct Max {
    max: Option<Value>,
    op: LessThanOperator,
}

impl AggregationFunction for Max {
    fn get_output_type(&self, input_type: &[ValueType]) -> Result<ValueType, PiperError> {
        if input_type.len() != 1 {
            return Err(PiperError::InvalidArgumentCount(1, input_type.len()));
        }
        Ok(input_type[0])
    }

    fn feed(&mut self, arguments: &[Value]) -> Result<(), PiperError> {
        if arguments.len() != 1 {
            return Err(PiperError::InvalidArgumentCount(1, arguments.len()));
        }
        if arguments[0].is_null() {
            return Ok(());
        }
        match &self.max {
            None => {
                self.max = Some(arguments[0].clone());
            }
            Some(v) => {
                self.max = if self
                    .op
                    .eval(vec![arguments[0].clone(), v.clone()])
                    .get_bool()?
                {
                    Some(v.clone())
                } else {
                    Some(arguments[0].clone())
                };
            }
        }

        Ok(())
    }

    fn get_result(&self) -> Result<Value, PiperError> {
        Ok(self.max.clone().unwrap_or_default())
    }

    fn dump(&self) -> String {
        "max".to_string()
    }
}

#[derive(Clone, Debug, Default)]
pub struct MinBy {
    min: Option<Value>,
    associated: Option<Value>,
    op: LessThanOperator,
}

impl AggregationFunction for MinBy {
    fn get_output_type(&self, input_type: &[ValueType]) -> Result<ValueType, PiperError> {
        if input_type.len() != 2 {
            return Err(PiperError::InvalidArgumentCount(2, input_type.len()));
        }
        Ok(input_type[0])
    }

    fn feed(&mut self, arguments: &[Value]) -> Result<(), PiperError> {
        if arguments.len() != 2 {
            return Err(PiperError::InvalidArgumentCount(2, arguments.len()));
        }
        if arguments[0].is_null() {
            return Ok(());
        }
        match &self.min {
            None => {
                self.min = Some(arguments[0].clone());
                self.associated = Some(arguments[1].clone());
            }
            Some(v) => {
                self.min = if self
                    .op
                    .eval(vec![arguments[0].clone(), v.clone()])
                    .get_bool()?
                {
                    self.associated = Some(arguments[1].clone());
                    Some(arguments[0].clone())
                } else {
                    Some(v.clone())
                };
            }
        }

        Ok(())
    }

    fn get_result(&self) -> Result<Value, PiperError> {
        Ok(self.associated.clone().unwrap_or_default())
    }

    fn dump(&self) -> String {
        "min_by".to_string()
    }
}

#[derive(Clone, Debug, Default)]
pub struct MaxBy {
    max: Option<Value>,
    associated: Option<Value>,
    op: LessThanOperator,
}

impl AggregationFunction for MaxBy {
    fn get_output_type(&self, input_type: &[ValueType]) -> Result<ValueType, PiperError> {
        if input_type.len() != 2 {
            return Err(PiperError::InvalidArgumentCount(2, input_type.len()));
        }
        Ok(input_type[0])
    }

    fn feed(&mut self, arguments: &[Value]) -> Result<(), PiperError> {
        if arguments.len() != 2 {
            return Err(PiperError::InvalidArgumentCount(2, arguments.len()));
        }
        if arguments[0].is_null() {
            return Ok(());
        }
        match &self.max {
            None => {
                self.max = Some(arguments[0].clone());
                self.associated = Some(arguments[1].clone());
            }
            Some(v) => {
                self.max = if self
                    .op
                    .eval(vec![arguments[0].clone(), v.clone()])
                    .get_bool()?
                {
                    Some(v.clone())
                } else {
                    self.associated = Some(arguments[1].clone());
                    Some(arguments[0].clone())
                };
            }
        }

        Ok(())
    }

    fn get_result(&self) -> Result<Value, PiperError> {
        Ok(self.associated.clone().unwrap_or_default())
    }

    fn dump(&self) -> String {
        "max_by".to_string()
    }
}
