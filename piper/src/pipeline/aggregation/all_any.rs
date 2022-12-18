use crate::{
    pipeline::operator::{AndOperator, Operator, OrOperator},
    PiperError, Value, ValueType,
};

use super::AggregationFunction;

#[derive(Clone, Debug, Default)]
pub struct All {
    all: Option<Value>,
    op: AndOperator,
}

impl AggregationFunction for All {
    fn get_output_type(&self, input_type: &[ValueType]) -> Result<ValueType, PiperError> {
        if input_type.len() != 1 {
            return Err(PiperError::InvalidArgumentCount(1, input_type.len()));
        }
        Ok(ValueType::Bool)
    }

    fn feed(&mut self, arguments: &[Value]) -> Result<(), PiperError> {
        if arguments.len() != 1 {
            return Err(PiperError::InvalidArgumentCount(1, arguments.len()));
        }
        if arguments[0].is_null() {
            // null is treated as false
            self.all = Some(false.into());
            return Ok(());
        }
        match &self.all {
            None => {
                self.all = Some(arguments[0].clone());
            }
            Some(v) => {
                self.all = Some(self.op.eval(vec![v.clone(), arguments[0].clone()]));
            }
        }

        Ok(())
    }

    fn get_result(&self) -> Result<Value, PiperError> {
        Ok(self.all.clone().unwrap_or_default())
    }

    fn dump(&self) -> String {
        "all".to_string()
    }
}

#[derive(Clone, Debug, Default)]
pub struct Any {
    any: Option<Value>,
    op: OrOperator,
}

impl AggregationFunction for Any {
    fn get_output_type(&self, input_type: &[ValueType]) -> Result<ValueType, PiperError> {
        if input_type.len() != 1 {
            return Err(PiperError::InvalidArgumentCount(1, input_type.len()));
        }
        Ok(ValueType::Bool)
    }

    fn feed(&mut self, arguments: &[Value]) -> Result<(), PiperError> {
        if arguments.len() != 1 {
            return Err(PiperError::InvalidArgumentCount(1, arguments.len()));
        }
        if arguments[0].is_null() {
            return Ok(());
        }
        match &self.any {
            None => {
                self.any = Some(arguments[0].clone());
            }
            Some(v) => {
                self.any = Some(self.op.eval(vec![v.clone(), arguments[0].clone()]));
            }
        }

        Ok(())
    }

    fn get_result(&self) -> Result<Value, PiperError> {
        Ok(self.any.clone().unwrap_or_default())
    }

    fn dump(&self) -> String {
        "any".to_string()
    }
}

#[cfg(test)]
mod tests {
    use crate::{pipeline::AggregationFunction, Value};

    #[test]
    fn test_all() {
        let mut all = super::All::default();
        assert_eq!(all.get_result().unwrap(), Value::Null);
        all.feed(&[true.into()]).unwrap();
        assert_eq!(all.get_result().unwrap(), true.into());
        all.feed(&[true.into()]).unwrap();
        assert_eq!(all.get_result().unwrap(), true.into());
        all.feed(&[true.into()]).unwrap();
        assert_eq!(all.get_result().unwrap(), true.into());
        all.feed(&[false.into()]).unwrap();
        assert_eq!(all.get_result().unwrap(), false.into());
        all.feed(&[true.into()]).unwrap();
        assert_eq!(all.get_result().unwrap(), false.into());
    }

    #[test]
    fn test_any() {
        let mut any = super::Any::default();
        assert_eq!(any.get_result().unwrap(), Value::Null);
        any.feed(&[false.into()]).unwrap();
        assert_eq!(any.get_result().unwrap(), false.into());
        any.feed(&[false.into()]).unwrap();
        assert_eq!(any.get_result().unwrap(), false.into());
        any.feed(&[false.into()]).unwrap();
        assert_eq!(any.get_result().unwrap(), false.into());
        any.feed(&[true.into()]).unwrap();
        assert_eq!(any.get_result().unwrap(), true.into());
        any.feed(&[false.into()]).unwrap();
        assert_eq!(any.get_result().unwrap(), true.into());
    }
}