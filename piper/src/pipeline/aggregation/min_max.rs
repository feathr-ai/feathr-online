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
        Ok(input_type[1])
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
        Ok(input_type[1])
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

#[cfg(test)]
mod tests {
    #[test]
    fn test_min() {
        use super::*;
        let mut min = Min::default();
        assert_eq!(min.get_output_type(&[ValueType::String]).unwrap(), ValueType::String);
        assert_eq!(min.get_output_type(&[ValueType::Null]).unwrap(), ValueType::Null);
        assert!(min.get_output_type(&[]).is_err());

        min.feed(&[Value::Int(2)]).unwrap();
        min.feed(&[Value::Int(3)]).unwrap();
        min.feed(&[Value::Int(1)]).unwrap();
        min.feed(&[Value::Int(4)]).unwrap();
        assert_eq!(min.get_result().unwrap(), Value::Int(1));
    }

    #[test]
    fn test_max() {
        use super::*;
        let mut max = Max::default();
        assert_eq!(max.get_output_type(&[ValueType::String]).unwrap(), ValueType::String);
        assert_eq!(max.get_output_type(&[ValueType::Null]).unwrap(), ValueType::Null);
        assert!(max.get_output_type(&[]).is_err());

        max.feed(&[Value::Int(2)]).unwrap();
        max.feed(&[Value::Int(3)]).unwrap();
        max.feed(&[Value::Int(4)]).unwrap();
        max.feed(&[Value::Int(1)]).unwrap();
        assert_eq!(max.get_result().unwrap(), Value::Int(4));
    }

    #[test]
    fn test_min_by() {
        use super::*;
        let mut min = MinBy::default();
        assert_eq!(min.get_output_type(&[ValueType::String, ValueType::String]).unwrap(), ValueType::String);
        assert_eq!(min.get_output_type(&[ValueType::Int, ValueType::String]).unwrap(), ValueType::String);
        assert!(min.get_output_type(&[]).is_err());

        min.feed(&[Value::Int(2), "b".into()]).unwrap();
        min.feed(&[Value::Int(3), "c".into()]).unwrap();
        min.feed(&[Value::Int(1), "a".into()]).unwrap();
        min.feed(&[Value::Int(4), "d".into()]).unwrap();
        assert_eq!(min.get_result().unwrap(), Value::String("a".into()));
    }

    #[test]
    fn test_max_by() {
        use super::*;
        let mut max = MaxBy::default();
        assert_eq!(max.get_output_type(&[ValueType::String, ValueType::String]).unwrap(), ValueType::String);
        assert_eq!(max.get_output_type(&[ValueType::Int, ValueType::String]).unwrap(), ValueType::String);
        assert!(max.get_output_type(&[]).is_err());

        max.feed(&[Value::Int(2), "b".into()]).unwrap();
        max.feed(&[Value::Int(3), "c".into()]).unwrap();
        max.feed(&[Value::Int(1), "a".into()]).unwrap();
        max.feed(&[Value::Int(4), "d".into()]).unwrap();
        assert_eq!(max.get_result().unwrap(), Value::String("d".into()));
    }
}