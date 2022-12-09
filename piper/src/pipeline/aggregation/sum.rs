use crate::{
    pipeline::operator::{DivideOperator, Operator, PlusOperator},
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
        self.op.get_output_type(&[input_type[0], input_type[0]])
    }

    fn feed(&mut self, arguments: &[Value]) -> Result<(), PiperError> {
        if arguments.len() != 1 {
            return Err(PiperError::InvalidArgumentCount(1, arguments.len()));
        }
        if arguments[0].is_null() {
            return Ok(());
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

#[derive(Clone, Debug, Default)]
pub struct Avg {
    sum: Option<Value>,
    count: usize,
    op: PlusOperator,
    div: DivideOperator,
}

impl AggregationFunction for Avg {
    fn get_output_type(&self, input_type: &[ValueType]) -> Result<ValueType, PiperError> {
        if input_type.len() != 1 {
            return Err(PiperError::InvalidArgumentCount(1, input_type.len()));
        }
        let sum_type = self.op.get_output_type(&[input_type[0], input_type[0]])?;
        self.div.get_output_type(&[sum_type, ValueType::Long])
    }

    fn feed(&mut self, arguments: &[Value]) -> Result<(), PiperError> {
        if arguments.len() != 1 {
            return Err(PiperError::InvalidArgumentCount(1, arguments.len()));
        }
        self.count += 1;
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
        let sum = self.sum.clone().unwrap_or_default();
        Ok(self.div.eval(vec![sum, Value::Long(self.count as i64)]))
    }

    fn dump(&self) -> String {
        "avg".to_string()
    }
}
