use crate::{
    pipeline::operator::{Operator, PlusOperator},
    PiperError, Value, ValueType,
};

use super::AggregationFunction;

#[derive(Clone, Debug, Default)]
pub struct Sum {
    sum: Option<Value>,
    op: PlusOperator,
}

impl AggregationFunction for Sum {
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
        match &self.sum {
            None => {
                self.sum = Some(arguments[0].clone());
            }
            Some(v) => {
                self.sum = Some(self.op.eval(vec![v.clone(), arguments[0].clone()]));
            }
        }

        Ok(())
    }

    fn get_result(&self) -> Result<Value, PiperError> {
        Ok(self.sum.clone().unwrap_or_default())
    }

    fn dump(&self) -> String {
        "sum".to_string()
    }
}
