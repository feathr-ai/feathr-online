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

#[cfg(test)]
mod tests {
    use crate::{pipeline::AggregationFunction, Value, ValueType};

    #[test]
    fn test_array_agg() {
        let mut agg = super::ArrayAgg::default();
        assert_eq!(agg.get_output_type(&[ValueType::String]).unwrap(), ValueType::Array);
        assert_eq!(agg.get_result().unwrap(), Value::Array(vec![]));
        agg.feed(&[1.into()]).unwrap();
        assert_eq!(agg.get_result().unwrap(), Value::Array(vec![1.into()]));
        agg.feed(&[2.into()]).unwrap();
        assert_eq!(agg.get_result().unwrap(), Value::Array(vec![1.into(), 2.into()]));
        agg.feed(&[3.into()]).unwrap();
        assert_eq!(agg.get_result().unwrap(), Value::Array(vec![1.into(), 2.into(), 3.into()]));
    }

    #[test]
    fn test_array_agg_if() {
        let mut agg = super::ArrayAggIf::default();
        assert_eq!(agg.get_output_type(&[ValueType::Object, ValueType::Bool]).unwrap(), ValueType::Array);
        assert_eq!(agg.get_result().unwrap(), Value::Array(vec![]));
        agg.feed(&[1.into(), true.into()]).unwrap();
        assert_eq!(agg.get_result().unwrap(), Value::Array(vec![1.into()]));
        agg.feed(&[2.into(), false.into()]).unwrap();
        assert_eq!(agg.get_result().unwrap(), Value::Array(vec![1.into()]));
        agg.feed(&[3.into(), true.into()]).unwrap();
        assert_eq!(agg.get_result().unwrap(), Value::Array(vec![1.into(), 3.into()]));
    }
}